/*
 * Monitors a proftpd ExtendedLog file and publishes new files to a kafka topic
 * Proftpd: LogFormat flare   "%{%Y-%m-%dT%H:%M:%S}t|%a|%u|%F|%f|%b|%{file-size}|%r|%s"
 */

/* log2kafka.toml example:
   Brokers="pkgtest.inaccess.com:9092"
   StateFile="/tmp/log2kafka.dat"
   Topic="mytopic"
   FtpHomePrefix='/disk1/ftphome'
*/

package main

import (
	"bufio"
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
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hpcloud/tail"
)

type Config struct {
	KafkaFlushTimeout int //message produce timeout (ms), override with env KAFKA_FLUSH_TIMEOUT
	KafkaMaxPending   int //with that many undelivered messages, exit. Override with env KAFKA_MAX_PENDING
	FtpHomePrefix     string
	FtpLogFile        string
	Topic             string
	Brokers           string
	StateFile         string
}

//C Global config
var C Config

//KMessage format sent to kafka
type KMessage struct {
	Type string
	KPayload
}

//KPayload message payload: ftp file data
type KPayload struct {
	Timestamp       string
	FileName        string
	FileNameFull    string
	BytesTransfered string
	FileSize        string
	RemoteIP        string
	Username        string
	FTPCommand      string
	FTPResponse     string
	Sample          string
	FileOffset      int64
	FileInode       uint64
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func checkNil(s string, name string) int {
	if len(s) == 0 {
		fmt.Printf("ERROR: %s shouldn't be empty\n", name)
		return 1
	}
	return 0

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
		log.Printf("WARNING: statefile seems corrupted, seeking to start of logfile %s",ftpLogFile)
		seekInfo = &tail.SeekInfo{Offset: 0, Whence: io.SeekStart}
	} else {
		log.Printf("WARNING: logfile seems changed since we last tailed it (previous inode: %d, current inode:%d), seeking to start of logfile.", savedInode, currentInode)
	}
	return seekInfo
}

// reads linecount lines from file fn up to hardcoded size limit of 15K
func getLines(fn string, linecount int) string {
	var sample string
	var maxLength = 15000

	file, err := os.Open(fn)
	defer file.Close()
	if err != nil {
		log.Printf("ERROR:getLines: opening file %s: %v\n", fn, err)
		return ""
	}

	reader := bufio.NewReader(file)
	var line string
	var length int
	for i := 0; i < 10; i++ {
		line, err = reader.ReadString('\n')
		length += len(line)

		if err != nil {
			break
		}

		if length > maxLength {
			fmt.Printf("ERROR: reading file %s: size %d exceeds maximum total size of %d  while reading line %d\n", fn, length, maxLength, (i + 1))
		}
		sample += line
	}

	if err != nil && err != io.EOF {
		fmt.Printf("ERROR: getLines: reading file %s: %v\n", fn, err)
	}
	return sample

}

//get an ftp log line and create a kafka json message
func xferlog2KMessage(line string, offset int64, inode uint64) ([]byte, error) {
	//Example line from ftp:
	//2018-06-06T13:24:56|195.46.28.226|ftp-admin|/bbb/LibreOffice_6.0.4_MacOS_x86-64.dmg|91488256||STOR LibreOffice_6.0.4_MacOS_x86-64.dmg|226
	log.Printf("Parsing: %s\n",line);

	s := strings.Split(line, "|")
	if len(s) < 9 {
		return nil, errors.New("Not enough fields in logfile")
	}

	fullpath := C.FtpHomePrefix+"/"+s[4]
	sample := getLines(fullpath, 10)
	//fmt.Printf("sample[%s]:%s\n",s[4],sample);

	m := &KMessage{
		Type: "NewFile",
		KPayload: KPayload{
			Timestamp:       s[0],
			RemoteIP:        s[1],
			Username:        s[2],
			FileName:        s[3],
			FileNameFull:    s[4],
			BytesTransfered: s[5],
			FileSize:        s[6],
			FTPCommand:      s[7],
			FTPResponse:     s[8],
			Sample:          sample,
			FileOffset:      offset,
			FileInode:       inode,
		},
	}
	jdata, _ := json.Marshal(m)

	return jdata, nil
}

func getInodeOfFile(fileName string) uint64 {
	fileinfo, err := os.Stat(fileName)
	if err != nil {
		return 0
	}
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
		fmt.Printf("ERROR:%v\n",err);
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
					ftpFileName = km.KPayload.FileName
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

	npending := producer.Flush(C.KafkaFlushTimeout) //5sec timeout
	if npending > 1 {
		log.Printf("kafkaSend:ERROR: %d unflushed messages\n", npending)
	}
	if npending > C.KafkaMaxPending {
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

	/*
		var x string
		x = os.Getenv("KAFKA_FLUSH_TIMEOUT")
			if len(x) > 0 {
				C.KafkaFlushTimeout, _ = strconv.Atoi(x)
			}
			x = os.Getenv("KAFKA_MAX_PENDING")
			if len(x) > 0 {
				C.KafkaMaxPending, _ = strconv.Atoi(x)
			}
	*/
	//flag.StringVar(&C.ConfFile, "c", "", "`confFile`, configuration file")
}
func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s -c <configuration file>\n", path.Base(os.Args[0]))
	flag.PrintDefaults()
}
func main() {
	var offset int64
	var err error

	var confFile = flag.String("c", "log2kafka.toml", "`configuration file")
	flag.Parse()

	if len(*confFile) < 1 {
		usage()
		os.Exit(1)
	}

	logSetup() //syslog

	/*
		m := multiconfig.NewWithPath("log2kafka.toml") // supports TOML, JSON and YAML
		err = m.Load(&C)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		}
	*/

	//Defaults
	C.FtpLogFile = "/var/log/proftpd/flare-ftp.log"
	C.KafkaFlushTimeout = 15000
	C.KafkaMaxPending = 10
	C.Topic = "mytopic"

	if _, err := toml.DecodeFile(*confFile, &C); err != nil {
		fmt.Printf("ERROR:%v\n", err)
	}

	//fmt.Printf("Initialized with C:%+v\n", &C)
	fmt.Printf("Initialized with C:%+v\n", &C)

	nerr := 0
	nerr += checkNil(C.Brokers, "Brokers")
	nerr += checkNil(C.Topic, "Topic")
	nerr += checkNil(C.StateFile, "StateFile")
	if nerr > 0 {
		os.Exit(3)
	}

	//Initialize producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": C.Brokers,
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
	go kafkaProdEvtMon(producer, C.StateFile)

	td := make(chan *tail.Tail) //used to communicate the *tail descriptor
	sigs := make(chan os.Signal, 5)

	signal.Notify(sigs, syscall.SIGINT)
	go sigHdl(sigs)

	//Find appropriate file location from previously saved logfile position
	seekInfo := determineLogOffset(C.StateFile, C.FtpLogFile)

	cfg := tail.Config{
		Follow:   true,
		ReOpen:   true,
		MustExist:   true,
		Location: seekInfo,
	}
	// Start the logfile monitor goroutine
	go tailFile(C.FtpLogFile, cfg, td, producer, C.Topic)
	t1 := <-td //wait for it to start and get the *tail so we can access file offset (t1.Tell()) from elsewhere

	for {
		offset, err = t1.Tell()
		log.Printf("log offset: %v, EventsLen:%d, ProducerLen:%d\n", offset, len(producer.Events()), len(producer.ProduceChannel()))
		time.Sleep(5 * time.Second)
	}

}
