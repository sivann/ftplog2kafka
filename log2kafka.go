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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hpcloud/tail"
)

var kafkaFlushTimeout = 5000 //message produce timeout (ms), override with env KAFKA_FLUSH_TIMEOUT
var kafkaMaxPending = 10     //with that many undelivered messages, exit. Override with env KAFKA_MAX_PENDING

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
	FileInode       uint64
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
func savePos(offset int64, inode uint64, line string, stateFile string) {
	f, err := os.Create(stateFile)
	check(err)
	defer f.Close()
	check(err)

	log.Printf("savePos: saving to %s offset:%d inode:%d line:%s\n", stateFile, offset, inode, line)

	s := fmt.Sprintf("%d %d %s\n", offset, inode, line)
	_, err = f.WriteString(s)
	check(err)
	f.Sync()
}

//called on start, seek to previously stored logfile position
func readPos(stateFile string) (bool, int64, uint64, string) {
	var offset int64
	var inode uint64
	var str string
	var ok bool

	f, err := os.Open(stateFile)
	if err != nil {
		return false, 0, 0, ""
	}
	defer f.Close()
	n, err := fmt.Fscanf(f, "%d %d %s\n", &offset, &inode, &str)

	ok = false
	if (n == 3) && (err == nil) {
		ok = true
	}

	//fmt.Printf("GOT %d items: %d and %s, err:%v, ok:%v\n", n, offset, hash, err, ok)
	return ok, offset, inode, str
}

// Used when starting to determine seek position.
// If ftp logfile is the same (checks inode), and state file is valid: last saved position. Else seek to start.
func determineLogOffset(stateFile string, ftpLogFile string) *tail.SeekInfo {
	stateFileOk, offset, savedInode, fn := readPos(stateFile)
	log.Printf("Read previous position:state:%v offset:%d, inode:%d, file:%s\n", stateFileOk, offset, savedInode, fn)

	currentInode := getInodeOfFile(ftpLogFile)

	var seekInfo *tail.SeekInfo
	if stateFileOk && currentInode == savedInode {
		log.Printf("ftp logfile inodes match (%d) and statefile is valid: seeking to offset:%d\n", currentInode, offset)
		seekInfo = &tail.SeekInfo{Offset: offset, Whence: io.SeekStart}
	} else if !stateFileOk {
		log.Printf("WARNING: statefile seems corrupted, seeking to start of logfile")
		seekInfo = &tail.SeekInfo{Offset: 0, Whence: io.SeekStart}
	} else {
		log.Printf("WARNING: logfile seems changed since we last tailed it (previous inode: %d, current inode:%d), seeking to start of logfile.", savedInode, currentInode)
	}
	return seekInfo
}

//get an xferlog line and create a kafka json message
func xferlog2KMessage(line string, offset int64, inode uint64) ([]byte, error) {
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
			FileInode:       inode,
		},
	}
	jdata, _ := json.Marshal(m)

	return jdata, nil
}

func getInodeOfFile(fileName string) uint64 {
	fileinfo, _ := os.Stat(fileName)
	stat, _ := fileinfo.Sys().(*syscall.Stat_t)
	return stat.Ino
}

//Monitor logfile and on each new valid line call kafkaSend
func tailFile(logFile string, cfg tail.Config,
	td chan *tail.Tail, producer *kafka.Producer, topic string) {
	t, err := tail.TailFile(logFile, cfg)
	defer t.Cleanup()
	td <- t

	if err != nil {
		log.Fatalln("TailFile failed - ", err)
		return
	}
	i := 0
	for line := range t.Lines {
		i++
		offset, err := t.Tell() //current offset
		inode := getInodeOfFile(logFile)
		jdata, err := xferlog2KMessage(line.Text, offset, inode) //pass current offset and inode so producer callback knows where to save
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
func kafkaProdEvtMon(producer *kafka.Producer, stateFile string) {
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
				var ftpFileName string
				var offset int64
				var inode uint64
				if err == nil {
					offset = km.KPayload.FileOffset
					ftpFileName = km.KPayload.Filename
					inode = km.KPayload.FileInode

					savePos(offset, inode, ftpFileName, stateFile)
				}
				log.Printf("kafkaProdEvtMon:Delivered message to topic: %s [%d] kafka_offset: %v, filename: %s\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition,
					m.TopicPartition.Offset, ftpFileName)
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

	npending := producer.Flush(kafkaFlushTimeout) //5sec timeout
	if npending > 1 {
		log.Printf("kafkaSend:ERROR: %d unflushed messages\n", npending)
	}
	if npending > kafkaMaxPending {
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

func init() {
	var x string
	x = os.Getenv("KAFKA_FLUSH_TIMEOUT")
	if len(x) > 0 {
		kafkaFlushTimeout, _ = strconv.Atoi(x)
	}
	x = os.Getenv("KAFKA_MAX_PENDING")
	if len(x) > 0 {
		kafkaMaxPending, _ = strconv.Atoi(x)
	}
	log.Printf("Initialized with kafkaFlushTimeout:%d, kafkaMaxPending:%d\n", kafkaFlushTimeout, kafkaMaxPending)
}

func main() {
	var offset int64
	var broker = flag.String("b", "", "kafka `bootstrap servers`, e.g.: localhost:9092")
	var topic = flag.String("t", "", "`topic` name")
	var ftpLogFile = flag.String("f", "", "ftp `logfile`")
	var stateFile = flag.String("s", "", "`statefile`, a file where to save our state")

	flag.Parse()

	if len(os.Args) != 9 {
		fmt.Fprintf(os.Stderr, "Usage: %s -s <statefile> -f <logfile> -b <broker> -t <topic>\n",
			os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	logSetup() //syslog

	//Initialize producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *broker,
		"acks":              1,
		"retries":           3,
		//"max.block.ms":                        10000, //60000
		"request.timeout.ms":                    15000, //30000
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
	go kafkaProdEvtMon(producer, *stateFile)

	td := make(chan *tail.Tail) //used to communicate the *tail descriptor
	sigs := make(chan os.Signal, 5)

	signal.Notify(sigs, syscall.SIGINT)
	go sigHdl(sigs)

	//Find appropriate file location from previously saved logfile position
	seekInfo := determineLogOffset(*stateFile, *ftpLogFile)

	cfg := tail.Config{
		Follow:   true,
		ReOpen:   true,
		Location: seekInfo,
	}
	// Start the logfile monitor goroutine
	go tailFile(*ftpLogFile, cfg, td, producer, *topic)
	t1 := <-td //wait for it to start and get the *tail so we can access file offset (t1.Tell()) from elsewhere

	_ = err

	for {
		offset, err = t1.Tell()
		log.Printf("log offset: %v, EventsLen:%d, ProducerLen:%d\n", offset, len(producer.Events()), len(producer.ProduceChannel()))
		time.Sleep(5 * time.Second)
	}

}
