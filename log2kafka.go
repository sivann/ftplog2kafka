package main

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hpcloud/tail"
)

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

func tailFile(path string, cfg tail.Config, action <-chan string, td chan *tail.Tail) {
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
		hash := hashString(line.Text)
		fmt.Printf("LN %05d:,[%s], %s\n", i, line.Text, hash)
		offset, err := t.Tell()
		check(err)
		savePos(offset, line.Text)
	}
}

func sigHdl(sigc <-chan os.Signal, actionc chan<- string) {
	sig := <-sigc
	fmt.Println()
	fmt.Println(sig)
	fmt.Println("intr ")
	//actionc <- "ftell"
	os.Exit(0)
}

func restorePos() (bool, int64, string) {
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

func kafkaEvtMon(producer *kafka.Producer) {
	fmt.Printf("start goroutine\n")
	for e := range producer.Events() {
		fmt.Printf("forloop\n")
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

		default:
			fmt.Printf("Ignored event: %s\n", ev)
		}
	}
}

func kafkaSend(producer *kafka.Producer, topic string, value []byte) {
	producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: []byte(value),
	}
}

func main() {
	var offset int64

	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
			os.Args[0])
		os.Exit(1)
	}

	var broker = flag.String("broker", "localhost:9092", "kafka bootstrap servers e.g. localhost:9092")
	var topic = flag.String("topic", "mytopic", "topic name")
	flag.Parse()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	//cx := make(chan struct{})
	td := make(chan *tail.Tail)
	action := make(chan string)
	sigs := make(chan os.Signal, 1)

	//signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(sigs, syscall.SIGINT)

	go sigHdl(sigs, action)

	stateOk, offset, hash := restorePos()
	fmt.Printf("main: ok:%v \toffset:%d,hash:%s\n", stateOk, offset, hash)

	var seekInfo *tail.SeekInfo

	if stateOk {
		fmt.Printf("Seeking to offset:%d\n", offset)
		seekInfo = &tail.SeekInfo{offset, io.SeekStart}
	} else {
		seekInfo = &tail.SeekInfo{0, io.SeekStart}
	}

	cfg := tail.Config{
		Follow:   true,
		ReOpen:   true,
		Location: seekInfo,
	}

	go kafkaEvtMon(producer)

	go tailFile("l1.log", cfg, action, td)
	t1 := <-td

	_ = err

	for {
		offset, err = t1.Tell()
		fmt.Printf("ftell: offset: %v\n", offset)
		str := fmt.Sprintf("Current Unix Time: %v\n", time.Now().Unix())
		kafkaSend(producer, *topic, []byte(str))
		time.Sleep(1 * time.Second)
	}

	//<-cx
}
