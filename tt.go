package main

import (
	"github.com/hpcloud/tail"
	"fmt"
	"io"
	"time"
	"os"
	"os/signal"
	"syscall"
	"log"
	"crypto/md5"
	"encoding/hex"
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

func tailFile(path string, cfg tail.Config, action <-chan string, td chan * tail.Tail) {
    t, err := tail.TailFile(path, cfg)
    defer t.Cleanup()
	td <- t

    if err != nil {
        log.Fatalln("TailFile failed - ", err)
		return
    }
	i:=0;
    for line := range t.Lines {
		i+=1
		hash := hashString(line.Text)
		fmt.Printf("LN %05d:,[%s], %s\n",i, line.Text, hash)
		offset, err := t.Tell()
		check(err)
		savePos(offset, line.Text)
    }
}

func sigHdl(sigc <- chan os.Signal, actionc chan<- string) {
        sig := <-sigc
        fmt.Println()
        fmt.Println(sig)
        fmt.Println("intr ")
        //actionc <- "ftell"
}

func restorePos() (bool, int64, string) {
	var offset int64
	var hash string
	var ok bool

    f, err := os.Open("state.dat")
    if err != nil {
        return false,0,""
    }
	defer f.Close()
	n, err := fmt.Fscanf(f, "%d %s\n", &offset, &hash) 

	ok = false
	if (n==2) && (err ==nil) {
		ok = true
	}

	//fmt.Printf("GOT %d items: %d and %s, err:%v, ok:%v\n", n, offset, hash, err, ok)
	return ok, offset, hash
}

func main() {
	var offset int64

	cx := make(chan struct{})
	td := make(chan * tail.Tail)
	action := make(chan string)
	sigs := make(chan os.Signal, 1)

	//signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(sigs, syscall.SIGINT)

	go sigHdl(sigs, action)

	stateOk, offset, hash := restorePos()
	fmt.Printf("main: ok:%v \toffset:%d,hash:%s\n",stateOk, offset, hash)

	var seekInfo * tail.SeekInfo ;

	if stateOk {
		fmt.Printf("Seeking to offset:%d\n",offset)
		seekInfo = &tail.SeekInfo{offset, io.SeekStart}
	} else {
		seekInfo = &tail.SeekInfo{0, io.SeekStart}
	}

	cfg := tail.Config{
		Follow: true, 
		ReOpen: true,
		Location: seekInfo,
	}

	go tailFile("l1.log", cfg, action, td)
	t1 := <- td

	var err error
	_ = err;

	for {
		offset, err = t1.Tell()
		fmt.Printf("ftell: offset: %v\n",offset)
		time.Sleep(1 * time.Second)
	}

	fmt.Printf("Program and goroutines started\n")
	<- cx
}

