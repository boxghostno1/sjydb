package main

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
	"log"
	"net"
	"sync"
	"time"
)

var ch = make(chan int, 3)
var mu sync.Mutex
var ok = "OK"
var no = "FALSE"
var amounterr = "argumentamounterror"

func handleConn(c net.Conn, optype map[string]string, db *gorocksdb.DB, ro *gorocksdb.ReadOptions, wo *gorocksdb.WriteOptions) {
	defer c.Close()
	for {
		// read from the connection
		defer c.Close()
		result := ""
		for {
			// read from the connection
			var buf = make([]byte, 10)
			//log.Println("start to read from conn")
			n, err := c.Read(buf)
			if err != nil {
				log.Println("conn read error:", err)
				return
			}
			decode := string(buf)
			//fmt.Println(decode,len(decode))
			if n < 10 {
				if n != 1 || decode[0] != ' ' {
					result += decode[:n]
				}
				//fmt.Println(result)
				kw1, lencmd, pos := getkw1(result)
				if optype[kw1] == "read" {
					mu.Lock()
					mu.Unlock()
					ch <- 0
					go read(c, result[pos:], kw1, lencmd, db, ro)
					// get a shared-lock goroutine read()
				}
				if optype[kw1] == "write" {
					go write(c, result[pos:], kw1, lencmd, db, ro, wo)
					//get a mutex-lock goroutine write()
				}
				if optype[kw1] == "" {
					//no such command
					backstream := back("no such command", 0)
					c.Write(backstream)
				}
				result = ""
				continue
			}
			result += decode
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "192.168.1.4:8888")
	if err != nil {
		fmt.Println("listen error:", err)
		return
	}
	defer l.Close()
	fmt.Println("listen ok")
	i := 0
	ok := "+OK\r\n"
	ok = ok
	//wrong := "+FALSE\r\n"
	DB_PATH := "/gorocksdb"
	db, err := OpenDB(DB_PATH)
	if err != nil {
		log.Println("fail to open db,", nil, db)
	}
	readOptions := gorocksdb.NewDefaultReadOptions()
	readOptions.SetFillCache(true)
	writeOptions := gorocksdb.NewDefaultWriteOptions()
	writeOptions.SetSync(true)
	//logs := make([]string,10)

	optype := make(map[string]string)
	optype["set"] = "write"
	optype["get"] = "read"
	optype["append"] = "write"
	optype["exists"] = "read"
	optype["strlen"] = "read"
	optype["setrange"] = "write"
	optype["getrange"] = "read"
	optype["lpush"] = "write"
	optype["rpush"] = "write"
	optype["lpop"] = "write"
	optype["rpop"] = "write"
	optype["llen"] = "read"
	optype["lindex"] = "read"
	optype["linsert"] = "write"
	optype["lrem"] = "write"
	optype["lset"] = "write"
	optype["hset"] = "write"
	optype["hget"] = "read"
	optype["hdel"] = "write"
	optype["hlen"] = "read"
	optype["hexists"] = "read"
	optype["hgetall"] = "read"
	optype["sadd"] = "write"
	optype["srem"] = "write"
	optype["scard"] = "read"
	optype["spop"] = "write"
	optype["sismember"] = "read"
	optype["srandmember"] = "read"
	optype["zadd"] = "write"
	optype["zcard"] = "read"
	optype["zscore"] = "read"
	optype["zrem"] = "write"
	optype["zrank"] = "read"
	optype["zcount"] = "read"
	optype["zrange"] = "read"

	//ch := make(chan int,3)
	for {
		time.Sleep(time.Second * 1)
		c, err := l.Accept()
		fmt.Println("a new connection")
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		i += 1
		go handleConn(c, optype, db, readOptions, writeOptions)
		// start a new goroutine to handle
		// the new connection.
		//defer c.Close()
		//result := ""
		//for {
		//	// read from the connection
		//	var buf = make([]byte, 10)
		//	//log.Println("start to read from conn")
		//	n, err := c.Read(buf)
		//	if err != nil {
		//		log.Println("conn read error:", err)
		//		return
		//	}
		//	decode := string(buf)
		//	//fmt.Println(decode,len(decode))
		//	if n<10{
		//		if n!=1 || decode[0]!=' '{
		//			result+=decode[:n]
		//		}
		//		//fmt.Println(result)
		//		kw1,lencmd,pos := getkw1(result)
		//		if optype[kw1]=="read"{
		//			mu.Lock()
		//			mu.Unlock()
		//			ch <- 0
		//			go read(c,result[pos:],kw1,lencmd,db,readOptions)
		//			// get a shared-lock goroutine read()
		//		}
		//		if optype[kw1]=="write"{
		//			logs = append(logs,result)
		//			go write(c,result[pos:],kw1,lencmd,db,readOptions,writeOptions)
		//			//get a mutex-lock goroutine write()
		//		}
		//		if optype[kw1]==""{
		//			//no such command
		//			backstream := back("no such command",0)
		//			c.Write(backstream)
		//		}
		//		result = ""
		//		continue
		//	}
		//	result += decode
		//}
	}
}

//func read(ch chan int){
//	var a int
//	fmt.Println("please")
//	fmt.Scan(&a)
//	<-ch
//	fmt.Println(a)
//}
//func write(ch chan int){
//	var a int
//	fmt.Println("waiting for channels")
//	mu.Lock()
//	for len(ch)>0{
//		time.Sleep(time.Second)
//	}
//	fmt.Println("it's ok now")
//	fmt.Scan(&a)
//	fmt.Println(a)
//	mu.Unlock()
//}
