package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

//ascard := "*2\r\n$5\r\nscard\r\n$4\r\nset1\r\n"
//aspop := "*2\r\n$4\r\nspop\r\n$4\r\nset1\r\n"
//asismember := "*3\r\n$9\r\nsismember\r\n$4\r\nset1\r\n$5\r\nworld"
//asrandmember := "*2\r\n$11\r\nsrandmember\r\n$4\r\nset1\r\n"
func main() {
	//optype := make(map[string]string)
	//optype["set"] = "write"
	//optype["get"] = "read"
	//optype["append"] = "write"
	//optype["exists"] = "read"
	//optype["strlen"] = "read"
	//optype["setrange"] = "write"
	//optype["getrange"] = "read"
	//optype["lpush"] = "write"
	//optype["rpush"] = "write"
	//optype["lpop"] = "write"
	//optype["rpop"] = "write"
	//optype["llen"] = "read"
	//optype["lindex"] = "read"
	//optype["linsert"] = "write"
	//optype["lrem"] = "write"
	//optype["lset"] = "write"
	//optype["hset"] = "write"
	//optype["hget"] = "read"
	//optype["hdel"] = "write"
	//optype["hlen"] = "read"
	//optype["hexists"] = "read"
	//optype["hgetall"] = "read"
	//optype["sadd"] = "write"
	//optype["srem"] = "write"
	//optype["scard"] = "read"
	//optype["spop"] = "write"
	//optype["sismember"] = "read"
	//optype["srandmember"] = "read"
	helptxt := "Hi! Here's the little DataBase from SJY! SJYDB is a lightweight database compatible with reids protocol based on rocksdb storage engine. It now supports most redis commands such as set key value, You can view all the commands on Redis' official website:  https://redis.io/commands"

	log.Println("begin dial...")
	conn, err := net.Dial("tcp", "192.168.1.4:8888")
	if err != nil {
		log.Println("dial error:", err)
		return
	}
	defer conn.Close()
	log.Println("dial ok")
	fmt.Println("SJYDB SUCCESSED OPEN!")
	for {
		fmt.Print("SJYDB-->")
		inputReader := bufio.NewReader(os.Stdin) // 使用了自动类型推导，不需要var关键字声明
		//inputReader = bufio.NewReader(os.Stdin)
		//input, err = inputReader.ReadString('\n')
		input, err := inputReader.ReadString('\n')
		if err != nil {
			break
		}
		input = input[:len(input)-1]
		if input == "exit" {
			conn.Close()
			break
		}
		if input == "help" {
			fmt.Println(helptxt)
			continue
		}
		code := encoder(input)

		conn.Write([]byte(code))
		if len(code)%10 == 0 {
			conn.Write([]byte(" "))
		}
		result := ""
		for {
			// read from the connection
			var buf = make([]byte, 10)
			//log.Println("start to read from conn")
			n, err := conn.Read(buf)
			if err != nil {
				log.Println("conn read error:", err)
				return
			}
			decode := string(buf)
			if n < 10 {
				if n != 1 || decode[0] != ' ' {
					result += decode[:n]
				}
				//fmt.Println(result)
				end, length := bulkstring(0, result)
				if length == -1 {
					fmt.Println("---------------------------error--------------------------")
				} else {
					fmt.Println(result[end-1-length : end-1])
				}
				break
			}
			result += decode
		}

	}
	//time.Sleep(time.Second * 10000)
}

func encoder(input string) string {
	ary := strings.Fields(input)
	result := "*" + strconv.Itoa(len(ary)) + "\r\n"
	i := 0
	for i < len(ary) {
		result += "$" + strconv.Itoa(len(ary[i])) + "\r\n" + ary[i] + "\r\n"
		i += 1
	}

	return result
}
