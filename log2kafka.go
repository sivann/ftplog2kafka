/*
 * Monitors a proftpd ExtendedLog file and publishes new files to a kafka topic
 * Proftpd: LogFormat flare   "%{%Y-%m-%dT%H:%M:%S}t|%a|%u|%F|%b|%{file-size}|%r|%s"
 */

package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hpcloud/tail"
)

//KMessage format sent to kafka
type KMessage struct {
	Type string
	KPayload
}

//KPayload message payload: ftp file data
type KPayload struct {
	Timestamp       string
	Filename        string
	BytesTransfered string
	FileSize        string
	RemoteIP        string
	Username        string
	FTPCommand      string
	FTPResponse     string
	FileOffset      int64
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func hashString(s string) string {
	hasher := md5.New()
	hasher.Write([]byte(s))
	hash := hex.EncodeToString(hasher.Sum(nil))
	return hash
}

// Saves position to state.dat file so we can continue reading where we left off if restarted
// Called only after data successfully sent to kafka
func savePos(offset int64, line string) {
	f, err := os.Create("state.dat")
	check(err)
	defer f.Close()
	check(err)

	s := fmt.Sprintf("%d %s\n", offset, hashString(line))
	_, err = f.WriteString(s)
	check(err)
	f.Sync()
}

//called on start, seek to previously stored logfile position
func readPos() (bool, int64, string) {
	var offset int64
	var hash string
	var ok bool

	f, err := os.Open("state.dat")
	if err != nil {
		return false, 0, ""
	}
	defer f.Close()
	n, err := fmt.Fscanf(f, "%d %s\n", &offset, &hash)

	ok = false
	if (n == 2) && (err == nil) {
		ok = true
	}

	//fmt.Printf("GOT %d items: %d and %s, err:%v, ok:%v\n", n, offset, hash, err, ok)
	return ok, offset, hash
}

//get an xferlog line and create a kafka json message
func xferlog2KMessage(line string, offset int64) ([]byte, error) {
	//Example line from ftp:
	//2018-06-06T13:24:56|195.46.28.226|ftp-admin|/bbb/LibreOffice_6.0.4_MacOS_x86-64.dmg|91488256||STOR LibreOffice_6.0.4_MacOS_x86-64.dmg|226

	s := strings.Split(line, "|")
	if len(s) < 8 {
		return nil, errors.New("Not enough fields")
	}

	m := &KMessage{
		Type: "NewFile",
		KPayload: KPayload{
			Timestamp:       s[0],
			RemoteIP:        s[1],
			Username:        s[2],
			Filename:        s[3],
			BytesTransfered: s[4],
			FileSize:        s[5],
			FTPCommand:      s[6],
			FTPResponse:     s[7],
			FileOffset:      offset,
		},
	}
	jdata, _ := json.Marshal(m)

	return jdata, nil
}

//Monitor logfile and on each new valid line call kafkaSend
func tailFile(path string, cfg tail.Config,
	td chan *tail.Tail, producer *kafka.Producer, topic string) {
	t, err := tail.TailFile(path, cfg)
	defer t.Cleanup()
	td <- t

	if err != nil {
		log.Fatalln("TailFile failed - ", err)
		return
	}
	i := 0
	for line := range t.Lines {
		i++
		offset, err := t.Tell()                           //current offset
		jdata, err := xferlog2KMessage(line.Text, offset) //pass current offset so producer callback knows where to save
		if err == nil {
			kafkaSend(producer, topic, jdata)
		} else {
			log.Printf("tailFile: Ignoring invalid xferlog line [%s]\n", line.Text)
		}
	}
}

// Ctrl+C signal handler for future cleanups
func sigHdl(sigc <-chan os.Signal) {
	sig := <-sigc
	fmt.Println()
	fmt.Println(sig)
	fmt.Println("interrupted, exiting")
	os.Exit(0)
}

// Callback from kafka producer. If kafka produce was a success, save logfile position of that message. We assume in-order.
func kafkaProdEvtMon(producer *kafka.Producer) {
	log.Printf("kafkaProdEvtMon:Started\n")
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			//fmt.Printf("MSG:%s\n", m.Value)
			if m.TopicPartition.Error != nil {
				log.Printf("kafkaProdEvtMon:Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				km := new(KMessage)
				err := json.Unmarshal(m.Value, &km)
				var fn string
				var offset int64
				if err == nil {
					offset = km.KPayload.FileOffset
					fn = km.KPayload.Filename
					//fmt.Printf("OFFSET:%v %s\n", km.KPayload.FileOffset, km.KPayload.Filename)
					savePos(offset, fn)
				}
				log.Printf("kafkaProdEvtMon:Delivered message to topic %s [%d] at offset %v, filename: %s\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition,
					m.TopicPartition.Offset, fn)
			}

		default:
			log.Printf("Unhandled event %T ignored: %v\n", e, e)
		}
	}
}

// Send data to kafka channel
func kafkaSend(producer *kafka.Producer, topic string, value []byte) {
	log.Printf("kafkaSend:Sending value of length %d\n", len(value))
	kmsg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: []byte(value),
	}

	producer.ProduceChannel() <- &kmsg

	npending := producer.Flush(5000) //5sec timeout
	if npending > 1 {
		log.Printf("kafkaSend:ERROR: %d unflushed messages\n", npending)
	}
	if npending > 10 {
		log.Printf("kafkaSend:ERROR: %d unflushed messages, exiting\n", npending)
		os.Exit(1)
	}
}

// syslog init
func logSetup() {
	logwriter, err := syslog.New(syslog.LOG_NOTICE, "log2kafka")
	if err == nil {
		log.SetOutput(logwriter)
	}

}

func main() {
	var offset int64
	var broker = flag.String("broker", "", "kafka bootstrap servers, e.g.: localhost:9092")
	var topic = flag.String("topic", "", "topic name")
	flag.Parse()

	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s -broker <broker> -topic <topic>\n",
			os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	logSetup() //syslog

	//Initialize producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *broker,
		"acks":              1,
		"retries":           2,
		//"max.block.ms":                          10000, //60000
		"request.timeout.ms":                    5000, //30000
		"max.in.flight.requests.per.connection": 1,
		"produce.offset.report":                 true,
		"auto.offset.reset":                     "latest",
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	_, verstr := kafka.LibraryVersion()
	log.Printf("Created Producer %v (%s)\n", producer, verstr)

	//Start the Kafka event monitor goroutine
	go kafkaProdEvtMon(producer)

	td := make(chan *tail.Tail) //used to communicate the *tail
	sigs := make(chan os.Signal, 5)

	signal.Notify(sigs, syscall.SIGINT)
	go sigHdl(sigs)

	//Seek to previously saved logfile position
	stateOk, offset, fn := readPos()
	log.Printf("Read previous position:%v \toffset:%d, file:%s\n", stateOk, offset, fn)
	var seekInfo *tail.SeekInfo
	if stateOk {
		log.Printf("Seeking to offset:%d\n", offset)
		seekInfo = &tail.SeekInfo{offset, io.SeekStart}
	} else {
		seekInfo = &tail.SeekInfo{0, io.SeekStart}
	}

	//
	cfg := tail.Config{
		Follow:   true,
		ReOpen:   true,
		Location: seekInfo,
	}

	// Start the logfile monitor goroutine
	go tailFile("l1.log", cfg, td, producer, *topic)
	t1 := <-td //wait for it to start and get the *tail so we can access file offset (t1.Tell()) from elsewhere

	_ = err

	for {
		offset, err = t1.Tell()
		log.Printf("ftell offset: %v, Events length:%d, Producer length:%d\n", offset, len(producer.Events()), len(producer.ProduceChannel()))
		time.Sleep(5 * time.Second)
	}

}
