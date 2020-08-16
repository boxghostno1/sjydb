package main

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
	"log"
	"math/rand"
	"strconv"
	"time"
)

//const (
//	DB_PATH = "/gorocksdb"
//)

func main() {
	DB_PATH := "/gorocksdb"
	db, err := OpenDB(DB_PATH)
	if err != nil {
		log.Println("fail to open db,", nil, db)
	}

	readOptions := gorocksdb.NewDefaultReadOptions()
	readOptions.SetFillCache(true)

	writeOptions := gorocksdb.NewDefaultWriteOptions()
	writeOptions.SetSync(true)
	//for i := 0; i < 10; i++ {
	//	keyStr := "test" + strconv.Itoa(i)
	//	valueStr := "value" + strconv.Itoa(i)
	//	var key []byte = []byte(keyStr)
	//	var value []byte = []byte(valueStr)
	//	db.Put(writeOptions, key, value)
	//	log.Println(i, keyStr)
	//	slice, err2 := db.Get(readOptions, key)
	//	if err2 != nil {
	//		log.Println("get data exception：", key, err2)
	//		continue
	//	}
	//	log.Println("get data：", slice.Size(), string(slice.Data()))
	//}
//	file1, err := os.Open("file.txt")
//	if err != nil {
//		panic(err)
//	}
//	defer file1.Close()
//	// 使用ioutil读取文件所有内容
//	b, err := ioutil.ReadAll(file1)
//	if err != nil {
//		panic(err)
//	}
////	a := string(b)
	a := "*3\r\n$3\r\nset\r\n$4\r\nkey1\r\n$11\r\nHello world\r\n"
	//aget := "*2\r\n$3\r\nget\r\n$4\r\nkey1\r\n"
	//aappend := "*3\r\n$6\r\nappend\r\n$4\r\nkey1\r\n$7\r\ncaonima\r\n"
	//astrlen := "*2\r\n$6\r\nstrlen\r\n$4\r\nkey1\r\n"
	//asetrange := "*4\r\n$8\r\nsetrange\r\n$4\r\nkey1\r\n:6\r\n$7\r\ncaonima\r\n"
	//agetrange := "*4\r\n$8\r\ngetrange\r\n$4\r\nkey1\r\n:0\r\n:6\r\n"
	//alpush := "*4\r\n$5\r\nrpush\r\n$5\r\nlist1\r\n$11\r\nHello right\r\n$12\r\nsecond right\r\n"
	//arpush := "*3\r\n$5\r\nrpush\r\n$5\r\nlist1\r\n$11\r\nHello right\r\n"
	//alpop := "*2\r\n$4\r\nlpop\r\n$5\r\nlist1\r\n"
	//arpop := "*2\r\n$4\r\nrpop\r\n$5\r\nlist1\r\n"
	//allen := "*2\r\n$4\r\nllen\r\n$5\r\nlist1\r\n"
	//alindex := "*3\r\n$6\r\nlindex\r\n$5\r\nlist1\r\n:2d\r\n"
	//alset := "*4\r\n$4\r\nlset\r\n$5\r\nlist1\r\n:3\r\n$7\r\nfuckyou\r\n"
	//alinsert := "*5\r\n$7\r\nlinsert\r\n$5\r\nlist1\r\n$5\r\nafter\r\n$12\r\nsecond right\r\n$3\r\ngan\r\n"
	//alrem := "*4\r\n$4\r\nlrem\r\n$5\r\nlist1\r\n:2\r\n$11\r\nHello right\r\n"
	//ahset := "*6\r\n$4\r\nhset\r\n$5\r\nhash1\r\n$6\r\nfield3\r\n$6\r\nvalue1\r\n$6\r\nfield3\r\n$6\r\nvalue2\r\n"
	//ahget := "*3\r\n$4\r\nhget\r\n$5\r\nhash1\r\n$5\r\nhello\r\n"
	//ahdel := "*3\r\n$4\r\nhdel\r\n$5\r\nhash1\r\n$4\r\nshit\r\n"
	//ahexists := "*3\r\n$7\r\nhexists\r\n$5\r\nhash1\r\n$4\r\nsafd\r\n"
	//ahlen := "*2\r\n$4\r\nhlen\r\n$5\r\nhash1\r\n"
	//ahgetall := "*2\r\n$7\r\nhgetall\r\n$5\r\nhash1\r\n"
	//asadd := "*4\r\n$4\r\nsadd\r\n$4\r\nset1\r\n$5\r\nworld\r\n$4\r\nfuck\r\n"
	//asrem := "*3\r\n$4\r\nsrem\r\n$4\r\nset1\r\n$5\r\nhello"
	//ascard := "*2\r\n$5\r\nscard\r\n$4\r\nset1\r\n"
	//aspop := "*2\r\n$4\r\nspop\r\n$4\r\nset1\r\n"
	//asismember := "*3\r\n$9\r\nsismember\r\n$4\r\nset1\r\n$5\r\nworld"
	//asrandmember := "*2\r\n$11\r\nsrandmember\r\n$4\r\nset1\r\n"
	azadd := "*6\r\n$4\r\nzadd\r\n$5\r\nzset1\r\n$6\r\n123765\r\n$5\r\nsbsjy\r\n$6\r\n090900\r\n$5\r\nntsjy\r\n"
	a = azadd
	rand.Seed(time.Now().UnixNano())
	ok := "+OK\r\n"
	wrong := "+FALSE\r\n"
	pos := 0
	next := 0
	it := db.NewIterator(readOptions)
	defer it.Close()
	it.SeekToFirst()
	fmt.Println("Key List:")
	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		fmt.Println(string(key.Data()),"|", string(value.Data()))
		key.Free()
		value.Free()
	}

	//if exist(db,readOptions,"999"){
	//	fmt.Println("1")
	//}
	//db.Put(writeOptions,[]byte("999"),[]byte("456"))
	//if exist(db,readOptions,"999"){
	//	fmt.Println("1")
	//}
//	key := []byte("lll")
//	it.Seek(key)
////	it.Next()
//	key1 := it.Key()
////	value1 := it.Value()
//	fmt.Println(string(key1.Data()))
////	fmt.Println(string(value1.Data()))
//	key1.Free()
//	value1.Free()
//	db.Delete(writeOptions,[]byte("Zzset1"))
//	db.Delete(writeOptions,[]byte("zzset100090900jkjkj"))
//	db.Delete(writeOptions,[]byte("zzset100123765hello"))
	for  pos < len(a) {
		for a[pos]!='*'{
			pos+=1
			if pos>=len(a){
				break
			}
		}
		if pos>=len(a){break}
		next = newcommand(pos,a)
		lencmd,_ := strconv.Atoi(a[pos+1:next-1])//mingling zifuchuan de shumu
		pos = next+1
		end,length := bulkstring(pos,a)
		kw1 := a[end-1-length:end-1]
		pos = end+1
		switch kw1 {
		case "exists":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			if exist(db, readOptions, keystr) {
				fmt.Println(1)
			} else {
				fmt.Println(0)
			}
		//stringcommands
		case "set":
			if lencmd != 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			key := []byte(keystr)
			end, length = bulkstring(pos, a)
			valuestr := a[end-1-length : end-1]
			value := []byte("$" + valuestr)
			pos = end + 1
			db.Put(writeOptions, key, value)
			fmt.Println(ok)
		case "get":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			key := []byte(keystr)
			slice, _ := db.Get(readOptions, key)
			if len(slice.Data()) == 0 {
				fmt.Println("No such key:", keystr[1:])
			} else {
				fmt.Println("get data:", string(slice.Data())[1:])
			}
			//	case "exit":
			//		fmt.Println("byebye")
			//		break
			//	case "keys":
			//		if a[pos]=='*' {
			//			it := db.NewIterator(readOptions)
			//			defer it.Close()
			//			it.SeekToFirst()
			//			fmt.Println("Key List:")
			//			for it = it; it.Valid(); it.Next() {
			//				key := it.Key()
			//				value := it.Value()
			//				fmt.Println(string(key.Data()))
			//				key.Free()
			//				value.Free()
			//			}
			//		}
			//	case "type":
			//		next = catch(pos,a)
			//		key := []byte(a[pos:next])
			//		pos = next+1
			//		slice,err2 := db.Get(readOptions, key)
			//		if err2 != nil {
			//			log.Println("get data exception：", key, err2)
			//			continue
			//		}
			//		if len(slice.Data())==0 {
			//			fmt.Println("No such key")
			//		}
			//		vstr := string(slice.Data())
			//		switch vstr[1]{
			//		case '+':
			//			fmt.Println("string")
			//		case ':':
			//			fmt.Println("int")
			//		case '$':
			//			fmt.Println("bulk string")
			//		case '*':
			//			fmt.Println("array")
			//		}
			//		fmt.Println("get data:",string(slice.Data()))
			//		fmt.Println(ok)
			//	case "del":
			//		next = catch(pos,a)
			//		key := []byte(a[pos:next])
			//		pos = next+1
			//		db.Delete(writeOptions, key)
			//		fmt.Println(ok)
		case "setnx":
			if lencmd != 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			key := []byte(keystr)
			end, length = bulkstring(pos, a)
			slice, _ := db.Get(readOptions, key)
			pos = end + 1
			if len(slice.Data()) == 0 {
				value := []byte("$" + a[end-1-length:end-1])
				db.Put(writeOptions, key, value)
				fmt.Println(ok)
			} else {
				fmt.Println("The key is existed")
			}
		case "append":
			if lencmd != 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			key := []byte(keystr)
			end, length = bulkstring(pos, a)
			slice, err2 := db.Get(readOptions, key)
			pos = end + 1
			if err2 != nil {
				fmt.Println("get data exception：", key, err2)
				continue
			}
			if len(slice.Data()) == 0 {
				fmt.Println("No such key:", keystr[1:])
			} else {
				valuestr := string(slice.Data()) + a[end-1-length:end-1]
				value := []byte(valuestr)
				fmt.Println(ok)
				db.Put(writeOptions, key, value)
			}
		case "strlen":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			key := []byte(keystr)
			slice, err2 := db.Get(readOptions, key)
			if err2 != nil {
				fmt.Println("get data exception：", key, err2)
				continue
			}
			if len(slice.Data()) == 0 {
				fmt.Println("No such key:", keystr[1:])
			} else {
				fmt.Println("stinglength:", len(string(slice.Data()))-1)
			}
		case "setrange":
			if lencmd != 4 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			key := []byte(keystr)
			end, index := inter(pos, a)
			pos = end + 1
			end, length = bulkstring(pos, a)
			appstr := a[end-1-length : end-1]
			slice, err2 := db.Get(readOptions, key)
			if err2 != nil {
				fmt.Println("get data exception：", key, err2)
				continue
			}
			pos = end + 1
			valuestr := string(slice.Data())
			if len(slice.Data()) == 0 {
				fmt.Println("No such key:", keystr[1:])
			} else {
				if len(valuestr) < index+1 {
					i := index + 1 - len(valuestr)
					for i > 0 {
						i -= 1
						valuestr += "\u0000"
					}
				}
				valuestr = valuestr[:index+1] + appstr
				value := []byte(valuestr)
				fmt.Println(ok)
				db.Put(writeOptions, key, value)
			}
		case "getrange":
			if lencmd != 4 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "$" + a[end-1-length:end-1]
			pos = end + 1
			end, start := inter(pos, a)
			pos = end + 1
			end, aend := inter(pos, a)
			pos = end + 1

			key := []byte(keystr)
			slice, err2 := db.Get(readOptions, key)
			if err2 != nil {
				fmt.Println("get data exception：", key, err2)
				continue
			}
			if len(slice.Data()) == 0 {
				fmt.Println("No such key:", keystr[1:])
			} else {
				valuestr := string(slice.Data())
				var res string
				if aend > len(valuestr)-2 {
					res = valuestr[start+1:]
				} else {
					res = valuestr[start+1 : aend+2]
				}
				fmt.Println("get data:", res)
			}
		//listcommands
		case "lpush":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			var finallen int
			if !exist(db, readOptions, keystr) {
				end, length = bulkstring(pos, a)
				item1 := a[end-1-length : end-1]
				createnewlist(db, writeOptions, keystr, item1)
				lencmd -= 1
				pos = end + 1
				finallen = 1
			}
			for lencmd > 2 {
				lencmd -= 1
				end, length = bulkstring(pos, a)
				pos = end + 1
				itemvaluestr := a[end-1-length : end-1]
				listlen, leftseq, rightseq, curseq := message(db, readOptions, keystr)
				itemkeystr := "l" + keystr[1:] + B8(curseq)
				itemvaluestr = B8(0) + B8(leftseq) + itemvaluestr
				db.Put(writeOptions, []byte(itemkeystr), []byte(itemvaluestr)) //append new leftseq
				olditemkeystr := "l" + keystr[1:] + B8(leftseq)
				slice, _ := db.Get(readOptions, []byte(olditemkeystr))
				olditemvaluestr := string(slice.Data())
				olditemvaluestr = B8(curseq) + olditemvaluestr[8:]
				db.Put(writeOptions, []byte(olditemkeystr), []byte(olditemvaluestr)) //change old leftseq
				newlistvaluestr := B8(listlen+1) + B8(curseq) + B8(rightseq) + B8(curseq+1)
				db.Put(writeOptions, []byte(keystr), []byte(newlistvaluestr)) //change list message
				finallen = listlen + 1
			}
			fmt.Println(finallen)
		case "rpush":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			var finallen int
			if !exist(db, readOptions, keystr) {
				end, length = bulkstring(pos, a)
				item1 := a[end-1-length : end-1]
				createnewlist(db, writeOptions, keystr, item1)
				lencmd -= 1
				pos = end + 1
				finallen = 1
			}
			for lencmd > 2 {
				lencmd -= 1
				end, length = bulkstring(pos, a)
				pos = end + 1
				itemvaluestr := a[end-1-length : end-1]
				listlen, leftseq, rightseq, curseq := message(db, readOptions, keystr)
				itemkeystr := "l" + keystr[1:] + B8(curseq)
				itemvaluestr = B8(rightseq) + B8(0) + itemvaluestr
				db.Put(writeOptions, []byte(itemkeystr), []byte(itemvaluestr)) //append new leftseq
				olditemkeystr := "l" + keystr[1:] + B8(rightseq)
				slice, _ := db.Get(readOptions, []byte(olditemkeystr))
				olditemvaluestr := string(slice.Data())
				olditemvaluestr = olditemvaluestr[:8] + B8(curseq) + olditemvaluestr[16:]
				db.Put(writeOptions, []byte(olditemkeystr), []byte(olditemvaluestr)) //change old leftseq
				newlistvaluestr := B8(listlen+1) + B8(leftseq) + B8(curseq) + B8(curseq+1)
				db.Put(writeOptions, []byte(keystr), []byte(newlistvaluestr)) //change list message
				finallen = listlen + 1
			}
			fmt.Println(finallen)
		case "lpop":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("No such list")
				continue
			}
			listlen, leftseq, rightseq, curseq := message(db, readOptions, keystr)
			if listlen == 1 {
				db.Delete(writeOptions, []byte(keystr))
				db.Delete(writeOptions, []byte("l"+keystr[1:]+B8(leftseq)))
				continue
			}
			popitemkeystr := "l" + keystr[1:] + B8(leftseq)
			it.Seek([]byte(popitemkeystr))
			key := it.Key()
			value := it.Value()
			fmt.Println(string(key.Data()), "|", string(value.Data()))
			key.Free()
			value.Free()
			newleftseq, _ := strconv.Atoi(string(value.Data())[8:16])
			newleftkeystr := "l" + keystr[1:] + B8(newleftseq)
			it.Seek([]byte(newleftkeystr))
			newleftvalue := it.Value()
			newleftvaluestr := string(newleftvalue.Data())
			newleftvalue.Free()
			newleftvaluestr = B8(0) + newleftvaluestr[8:]
			db.Delete(writeOptions, []byte(popitemkeystr))                       //lpop
			db.Put(writeOptions, []byte(newleftkeystr), []byte(newleftvaluestr)) //change newleftitem
			newlistvaluestr := B8(listlen-1) + B8(newleftseq) + B8(rightseq) + B8(curseq)
			db.Put(writeOptions, []byte(keystr), []byte(newlistvaluestr)) //change list message
		case "rpop":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("No such list")
				continue
			}
			listlen, leftseq, rightseq, curseq := message(db, readOptions, keystr)
			if listlen == 1 {
				db.Delete(writeOptions, []byte(keystr))
				db.Delete(writeOptions, []byte("l"+keystr[1:]+B8(leftseq)))
				continue
			}
			popitemkeystr := "l" + keystr[1:] + B8(rightseq)
			it.Seek([]byte(popitemkeystr))
			key := it.Key()
			value := it.Value()
			fmt.Println(string(key.Data()), "|", string(value.Data()))
			key.Free()
			value.Free()
			newrightseq, _ := strconv.Atoi(string(value.Data())[:8])
			newrightkeystr := "l" + keystr[1:] + B8(newrightseq)
			it.Seek([]byte(newrightkeystr))
			newrightvalue := it.Value()
			newrightvaluestr := string(newrightvalue.Data())
			newrightvalue.Free()
			newrightvaluestr = newrightvaluestr[:8] + B8(0) + newrightvaluestr[16:]
			db.Delete(writeOptions, []byte(popitemkeystr))                         //rpop
			db.Put(writeOptions, []byte(newrightkeystr), []byte(newrightvaluestr)) //change newrightitem
			newlistvaluestr := B8(listlen-1) + B8(leftseq) + B8(newrightseq) + B8(curseq)
			db.Put(writeOptions, []byte(keystr), []byte(newlistvaluestr)) //change list message
		case "llen":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("No such list")
				continue
			}
			listlen, _, _, _ := message(db, readOptions, keystr)
			fmt.Println(listlen)
		case "lindex":
			if lencmd != 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("No such list")
				continue
			}
			end, index := inter(pos, a)
			pos = end + 1
			listlen, leftseq, _, _ := message(db, readOptions, keystr)
			if index < 0 {
				index += listlen
			}
			if index >= listlen {
				fmt.Println("list out of range")
			} else {
				itemkeystr := "l" + keystr[1:] + B8(leftseq)
				it.Seek([]byte(itemkeystr))
				itemvalue := it.Value()
				nextseq := string(itemvalue.Data())[8:16]
				for index > 0 {
					itemkeystr = "l" + keystr[1:] + nextseq
					it.Seek([]byte(itemkeystr))
					itemvalue = it.Value()
					nextseq = string(itemvalue.Data())[8:16]
					index -= 1
				}
				fmt.Println(string(itemvalue.Data())[16:])
				itemvalue.Free()
			}
		case "lset":
			if lencmd != 4 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("No such list")
				continue
			}
			end, index := inter(pos, a)
			pos = end + 1
			end, length = bulkstring(pos, a)
			pos = end + 1
			valuestr := a[end-1-length : end-1]
			listlen, leftseq, _, _ := message(db, readOptions, keystr)
			if index < 0 {
				index += listlen
			}
			if index >= listlen {
				fmt.Println("list out of range")
			} else {
				itemkeystr := "l" + keystr[1:] + B8(leftseq)
				it.Seek([]byte(itemkeystr))
				itemvalue := it.Value()
				nextseq := string(itemvalue.Data())[8:16]
				for index > 0 {
					itemkeystr = "l" + keystr[1:] + nextseq
					it.Seek([]byte(itemkeystr))
					itemvalue = it.Value()
					nextseq = string(itemvalue.Data())[8:16]
					index -= 1
				}
				itemkey := it.Key()
				valuestr = string(itemvalue.Data())[:8] + nextseq + valuestr
				db.Put(writeOptions, itemkey.Data(), []byte(valuestr))
				fmt.Println(ok)
				itemkey.Free()
				itemvalue.Free()
			}
		case "linsert":
			if lencmd != 5 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("No such list")
				continue
			}
			end, length := bulkstring(pos, a)
			kw2 := a[end-1-length : end-1] //before or after
			pos = end + 1
			end, length = bulkstring(pos, a)
			pos = end + 1
			pivot := a[end-1-length : end-1]
			end, length = bulkstring(pos, a)
			element := a[end-1-length : end-1]
			pos = end + 1
			listlen, leftseq, rightseq, curseq := message(db, readOptions, keystr)
			i := listlen
			firstitemkeystr := "l" + keystr[1:] + B8(leftseq)
			it.Seek([]byte(firstitemkeystr))
			insertkeystr := "l" + keystr[1:] + B8(curseq)
			for i > 0 {
				itemvalue := it.Value()
				itemvaluestr := string(itemvalue.Data())
				nextkey := "l" + keystr[1:] + string(itemvalue.Data())[8:16]
				if itemvaluestr[16:] == pivot {
					itemkey := it.Key()
					itemkeystr := string(itemkey.Data())
					preseq := string(itemvalue.Data())[:8]
					nextseq := string(itemvalue.Data())[8:16]
					thisseq := itemkeystr[len(itemkeystr)-8:]
					switch kw2 {
					case "before":
						insertvaluestr := preseq + thisseq + element
						itemvaluestr = B8(curseq) + itemvaluestr[8:]
						db.Put(writeOptions, []byte(insertkeystr), []byte(insertvaluestr))
						db.Put(writeOptions, []byte(itemkeystr), []byte(itemvaluestr))
						if preseq == "00000000" {
							leftseq = curseq
						} else {
							prekeystr := "l" + keystr[1:] + preseq
							it.Seek([]byte(prekeystr))
							prevalue := it.Value()
							prevaluestr := string(prevalue.Data())
							prevaluestr = prevaluestr[:8] + B8(curseq) + prevaluestr[16:]
							db.Put(writeOptions, []byte(prekeystr), []byte(prevaluestr))
						}
					case "after":
						insertvaluestr := thisseq + nextseq + element
						itemvaluestr = itemvaluestr[:8] + B8(curseq) + itemvaluestr[16:]
						db.Put(writeOptions, []byte(insertkeystr), []byte(insertvaluestr))
						db.Put(writeOptions, []byte(itemkeystr), []byte(itemvaluestr))
						if nextseq == "00000000" {
							rightseq = curseq
						} else {
							nextkeystr := "l" + keystr[1:] + nextseq
							it.Seek([]byte(nextkeystr))
							nextvalue := it.Value()
							nextvaluestr := string(nextvalue.Data())
							nextvaluestr = B8(curseq) + nextvaluestr[8:]
							db.Put(writeOptions, []byte(nextkeystr), []byte(nextvaluestr))
						}
					}
					messstr := B8(listlen+1) + B8(leftseq) + B8(rightseq) + B8(curseq+1)
					db.Put(writeOptions, []byte(keystr), []byte(messstr))
					break
				}
				it.Seek([]byte(nextkey))
				i -= 1
			}
			if i == 0 {
				fmt.Println("no such pivot")
			} else {
				fmt.Println(listlen + 1)
			}
		case "lrem":
			if lencmd != 4 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "L" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("No such list")
				continue
			}
			end, count := inter(pos, a)
			pos = end + 1
			end, length = bulkstring(pos, a)
			pos = end + 1
			element := a[end-1-length : end-1]
			listlen, leftseq, rightseq, curseq := message(db, readOptions, keystr)
			i := listlen
			firstitemkeystr := "l" + keystr[1:] + B8(leftseq)
			it.Seek([]byte(firstitemkeystr))
			if count == 0 {
				count = listlen
			}
			thecount := 0
			deleteitems := make(map[string]string)
			for i > 0 {
				itemvalue := it.Value()
				itemvaluestr := string(itemvalue.Data())
				nextkey := "l" + keystr[1:] + string(itemvalue.Data())[8:16]
				if itemvaluestr[16:] == element {
					itemkey := it.Key()
					itemkeystr := string(itemkey.Data())
					preseq := string(itemvalue.Data())[:8]
					nextseq := string(itemvalue.Data())[8:16]
					thisseq := itemkeystr[len(itemkeystr)-8:]
					deleteitems[thisseq] = preseq
					for {
						deleteitem, ok := deleteitems[preseq]
						if ok {
							preseq = deleteitem
						} else {
							break
						}
					}
					if preseq != "00000000" {
						prekeystr := "l" + keystr[1:] + preseq
						slice, _ := db.Get(readOptions, []byte(prekeystr))
						prevaluestr := string(slice.Data())
						prevaluestr = prevaluestr[:8] + nextseq + prevaluestr[16:]
						db.Put(writeOptions, []byte(prekeystr), []byte(prevaluestr))

					} else {
						leftseq, _ = strconv.Atoi(nextseq)
					}
					if nextseq != "00000000" {
						nextkeystr := "l" + keystr[1:] + nextseq
						slice, _ := db.Get(readOptions, []byte(nextkeystr))
						nextvaluestr := string(slice.Data())
						nextvaluestr = preseq + nextvaluestr[8:]
						db.Put(writeOptions, []byte(nextkeystr), []byte(nextvaluestr))
					} else {
						rightseq, _ = strconv.Atoi(preseq)
					}
					db.Delete(writeOptions, []byte(itemkeystr))
					thecount += 1
					//nextkeystr := "l"+keystr[1:]+nextseq

					messstr := B8(listlen-thecount) + B8(leftseq) + B8(rightseq) + B8(curseq)
					db.Put(writeOptions, []byte(keystr), []byte(messstr))

					if thecount >= listlen {
						db.Delete(writeOptions, []byte(keystr))
						break
					}
					if thecount >= count {
						break
					}
				}
				it.Seek([]byte(nextkey))
				i -= 1
			}
			if thecount == 0 {
				fmt.Println("no such element")
			} else {
				fmt.Println(thecount)
			}
		//hashcommands
		case "hset":
			if lencmd < 4 || lencmd%2 != 0 {
				fmt.Println(wrong)
				continue
			}
			setnum := lencmd/2 - 1
			createnum := 0
			end, length = bulkstring(pos, a)
			keystr := "H" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				createnewhash(db, writeOptions, keystr)
			}
			for lencmd > 2 {
				lencmd -= 2
				end, length = bulkstring(pos, a)
				pos = end + 1
				hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
				if !exist(db, readOptions, hashkeystr) {
					createnum += 1
				}
				end, length = bulkstring(pos, a)
				pos = end + 1
				hashvaluestr := "h" + a[end-1-length:end-1]
				db.Put(writeOptions, []byte(hashkeystr), []byte(hashvaluestr))
			}
			slice, _ := db.Get(readOptions, []byte(keystr))
			hashlen, _ := strconv.Atoi(string(slice.Data()))
			hashlen += createnum
			db.Put(writeOptions, []byte(keystr), []byte(strconv.Itoa(hashlen)))
			fmt.Println(setnum)
		case "hdel":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "H" + a[end-1-length:end-1]
			pos = end + 1
			if exist(db, readOptions, keystr) {
				removenum := 0
				for lencmd > 2 {
					lencmd -= 1
					end, length = bulkstring(pos, a)
					hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
					pos = end + 1
					if exist(db, readOptions, hashkeystr) {
						db.Delete(writeOptions, []byte(hashkeystr))
						removenum += 1
					}
				}
				slice, _ := db.Get(readOptions, []byte(keystr))
				hashlen, _ := strconv.Atoi(string(slice.Data()))
				hashlen -= removenum
				if hashlen == 0 {
					db.Delete(writeOptions, []byte(keystr))
				} else {
					db.Put(writeOptions, []byte(keystr), []byte(strconv.Itoa(hashlen)))
				}
				fmt.Println(removenum)
			} else {
				fmt.Println(0)
			}
		case "hget":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "H" + a[end-1-length:end-1]
			pos = end + 1
			end, length = bulkstring(pos, a)
			hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
			pos = end + 1
			slice, _ := db.Get(readOptions, []byte(hashkeystr))
			if len(slice.Data()) == 0 {
				fmt.Println("nil")
			} else {
				fmt.Println("get data:", string(slice.Data())[1:])
			}
		case "hexists":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "H" + a[end-1-length:end-1]
			pos = end + 1
			end, length = bulkstring(pos, a)
			hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
			pos = end + 1
			if exist(db, readOptions, hashkeystr) {
				fmt.Println(1)
			} else {
				fmt.Println(0)
			}
		case "hlen":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "H" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println(0)
			} else {
				slice, _ := db.Get(readOptions, []byte(keystr))
				hashlen, _ := strconv.Atoi(string(slice.Data()))
				fmt.Println(hashlen)
			}
		case "hgetall":
			var result []string
			result = append(result, " ")
			fmt.Println(result[0])
			end, length = bulkstring(pos, a)
			keystr := "H" + a[end-1-length:end-1]
			pos = end + 1
			if exist(db, readOptions, keystr) {
				slice, _ := db.Get(readOptions, []byte(keystr))
				hashlen, _ := strconv.Atoi(string(slice.Data()))
				it.Seek([]byte("h" + keystr[1:]))
				for hashlen > 0 {
					hashlen -= 1
					key := it.Key()
					value := it.Value()
					result = append(result, string(key.Data())[len(keystr):], string(value.Data())[1:])
					key.Free()
					value.Free()
					it.Next()
				}
			}
			i := 0
			for i < len(result) {
				fmt.Println(result[i])
				i += 1
			}

			//case "keys"
		//setcommands
		case "sadd":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			addnum := 0
			end, length = bulkstring(pos, a)
			keystr := "S" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				createnewhash(db, writeOptions, keystr)
			}
			for lencmd > 2 {
				lencmd -= 1
				end, length = bulkstring(pos, a)
				pos = end + 1
				setmemberstr := "s" + keystr[1:] + a[end-1-length:end-1]
				if !exist(db, readOptions, setmemberstr) {
					addnum += 1
				}
				db.Put(writeOptions, []byte(setmemberstr), []byte("s"))
			}
			slice, _ := db.Get(readOptions, []byte(keystr))
			scard, _ := strconv.Atoi(string(slice.Data()))
			scard += addnum
			db.Put(writeOptions, []byte(keystr), []byte(strconv.Itoa(scard)))
			fmt.Println(addnum)
		case "srem":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "S" + a[end-1-length:end-1]
			pos = end + 1
			if exist(db, readOptions, keystr) {
				removenum := 0
				for lencmd > 2 {
					lencmd -= 1
					end, length = bulkstring(pos, a)
					memberstr := "s" + keystr[1:] + a[end-1-length:end-1]
					pos = end + 1
					if exist(db, readOptions, memberstr) {
						db.Delete(writeOptions, []byte(memberstr))
						removenum += 1
					}
				}
				slice, _ := db.Get(readOptions, []byte(keystr))
				scard, _ := strconv.Atoi(string(slice.Data()))
				scard -= removenum
				if scard == 0 {
					db.Delete(writeOptions, []byte(keystr))
				} else {
					db.Put(writeOptions, []byte(keystr), []byte(strconv.Itoa(scard)))
				}
				fmt.Println(removenum)
			} else {
				fmt.Println(0)
			}
		case "scard":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "S" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println(0)
			} else {
				slice, _ := db.Get(readOptions, []byte(keystr))
				setcard, _ := strconv.Atoi(string(slice.Data()))
				fmt.Println(setcard)
			}
		case "spop":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "S" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("nil")
			} else {
				slice, _ := db.Get(readOptions, []byte(keystr))
				scard, _ := strconv.Atoi(string(slice.Data()))
				index := rand.Intn(scard)
				it.Seek([]byte("s" + keystr[1:]))
				for index > 0 {
					it.Next()
					index -= 1
				}
				key := it.Key()
				memberkeystr := key.Data()
				db.Delete(writeOptions, memberkeystr)
				if scard == 1 {
					db.Delete(writeOptions, []byte(keystr))
				} else {
					db.Put(writeOptions, []byte(keystr), []byte(strconv.Itoa(scard-1)))
				}
				fmt.Println(string(memberkeystr)[len(keystr):])
			}
		case "sismember":
			if lencmd != 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "S" + a[end-1-length:end-1]
			pos = end + 1
			end, length = bulkstring(pos, a)
			memberstr := "s" + keystr[1:] + a[end-1-length:end-1]
			if exist(db, readOptions, memberstr) {
				fmt.Println(1)
			} else {
				fmt.Println(0)
			}
		case "smembers":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "S" + a[end-1-length:end-1]
			pos = end + 1
			slice, _ := db.Get(readOptions, []byte(keystr))
			var scard int
			if len(slice.Data()) == 0 {
				scard = 0
			} else {
				scard, _ = strconv.Atoi(string(slice.Data()))
			}
			it.Seek([]byte("s" + keystr[1:]))
			for scard > 0 {
				scard -= 1
				key := it.Key()
				memberstr := string(key.Data())[len(keystr):]
				fmt.Println(memberstr)
				key.Free()
				it.Next()
			}
		case "srandmember":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "S" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("nil")
			} else {
				slice, _ := db.Get(readOptions, []byte(keystr))
				scard, _ := strconv.Atoi(string(slice.Data()))
				index := rand.Intn(scard)
				it.Seek([]byte("s" + keystr[1:]))
				for index > 0 {
					it.Next()
					index -= 1
				}
				key := it.Key()
				memberkeystr := key.Data()
				fmt.Println(string(memberkeystr)[len(keystr):])
			}
		//zsetcommands
		case "zadd":
			if lencmd < 4 {
				fmt.Println(wrong)
				continue
			}
			addnum := 0
			end, length = bulkstring(pos, a)
			keystr := "Z" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				createnewhash(db, writeOptions, keystr)
			}
			for lencmd > 2 {
				lencmd -= 2
				end, length = bulkstring(pos, a)
				pos = end + 1
				score := a[end-1-length : end-1]
				intscore, _ := strconv.Atoi(score)
				score = B8(intscore)
				end, length = bulkstring(pos, a)
				pos = end + 1
				member := a[end-1-length : end-1]
				mkeystr := "z" + keystr[1:] + score + member
				oldstr, tmp := zexists(db, readOptions, keystr, member)
				if tmp < 0 {
					addnum += 1
					slice, _ := db.Get(readOptions, []byte(keystr))
					zcard, _ := strconv.Atoi(string(slice.Data()))
					zcard += 1
					db.Put(writeOptions, []byte(keystr), []byte(strconv.Itoa(zcard)))
				} else {
					db.Delete(writeOptions, []byte(oldstr))
				}
				db.Put(writeOptions, []byte(mkeystr), []byte("z"))
			}
			fmt.Println(addnum)
			//slice,_ := db.Get(readOptions,[]byte(keystr))
			//zcard,_ := strconv.Atoi(string(slice.Data()))
			//fmt.Println(zcard)
			//zcard += addnum
			//db.Put(writeOptions,[]byte(keystr),[]byte(strconv.Itoa(zcard)))
			//fmt.Println(zcard)
			//fmt.Println(addnum)
		case "zcard":
			if lencmd != 2 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "Z" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println(0)
			} else {
				slice, _ := db.Get(readOptions, []byte(keystr))
				zcard, _ := strconv.Atoi(string(slice.Data()))
				fmt.Println(zcard)
			}
		case "zscore":
			if lencmd != 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "Z" + a[end-1-length:end-1]
			pos = end + 1
			end, length = bulkstring(pos, a)
			member := a[end-1-length:end-1]
			pos = end + 1
			zkeystr,tmp := zexists(db,readOptions,keystr,member)
			if tmp<0{
				fmt.Println("nil")
			}else{
				score,_ := strconv.Atoi(zkeystr[len(keystr):len(keystr)+8])
				fmt.Println(score)
			}
		case "zrem":
			if lencmd < 3 {
				fmt.Println(wrong)
				continue
			}
			removenum := 0
			end, length = bulkstring(pos, a)
			keystr := "Z" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println(removenum)
			}else{
				for lencmd > 2 {
					lencmd -= 1
					end, length = bulkstring(pos, a)
					pos = end + 1
					member := a[end-1-length : end-1]
					oldstr, tmp := zexists(db, readOptions, keystr, member)
					if tmp >= 0 {
						removenum += 1
						slice, _ := db.Get(readOptions, []byte(keystr))
						zcard, _ := strconv.Atoi(string(slice.Data()))
						zcard -= 1
						db.Put(writeOptions, []byte(keystr), []byte(strconv.Itoa(zcard)))
						db.Delete(writeOptions, []byte(oldstr))
					}
				}
				fmt.Println(removenum)
			}
		case "zrank":
			if lencmd != 3 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "Z" + a[end-1-length:end-1]
			pos = end + 1
			end, length = bulkstring(pos, a)
			member := a[end-1-length:end-1]
			pos = end + 1
			_,tmp := zexists(db,readOptions,keystr,member)
			if tmp<0{
				fmt.Println("nil")
			}else{
				fmt.Println(tmp)
			}
		case "zcount":
			if lencmd != 4 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "Z" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("nil")
				continue
			}
			end, length = bulkstring(pos, a)
			min,_ := strconv.Atoi(a[end-1-length:end-1])
			pos = end + 1
			end, length = bulkstring(pos, a)
			max,_ := strconv.Atoi(a[end-1-length:end-1])
			pos = end + 1

			slice,_ := db.Get(readOptions,[]byte(keystr))
			zlen,_ := strconv.Atoi(string(slice.Data()))
			i := 0
			count := 0
			it.Seek([]byte("z"+keystr[1:]))
			for i<zlen{
				zkey := it.Key()
				score,_ := strconv.Atoi(string(zkey.Data()))
				if score>=min{
					break
				}
				i+=1
				it.Next()
			}
			for i<zlen{
				zkey := it.Key()
				score,_ := strconv.Atoi(string(zkey.Data()))
				if score>max{
					break
				}
				count+=1
				i+=1
				it.Next()
			}
			fmt.Println(count)
		case "zrange":
			if lencmd != 4 {
				fmt.Println(wrong)
				continue
			}
			end, length = bulkstring(pos, a)
			keystr := "Z" + a[end-1-length:end-1]
			pos = end + 1
			if !exist(db, readOptions, keystr) {
				fmt.Println("nil")
				continue
			}
			end, length = bulkstring(pos, a)
			start,_ := strconv.Atoi(a[end-1-length:end-1])
			pos = end + 1
			end, length = bulkstring(pos, a)
			stop,_ := strconv.Atoi(a[end-1-length:end-1])
			pos = end + 1
			slice,_ := db.Get(readOptions,[]byte(keystr))
			zlen,_ := strconv.Atoi(string(slice.Data()))
			if start<0{
				start+=zlen
			}
			if stop<0{
				stop+=zlen
			}
			if start>0 &&start<zlen{
				if stop>=zlen{
					stop = zlen-1
				}
				i := 0
				it.Seek([]byte("z"+keystr[1:]))
				for i<start{
					it.Next()
					i+=1
				}
				for i<stop{
					zkey := it.Key()
					zkeystr := string(zkey.Data())
					fmt.Println(zkeystr[len(keystr)+8:])
					it.Next()
					i+=1
				}
			}
		}
		//case "keys"
		//case "keys"
		//case "keys"
		//case "keys"
		//case "key
		//case "keys"
		//case "keys"
		//case "keys"
		//case "keys"
		//case "keys"
		//case "keys"
		//case "keys"
		//
		//
		//
		//
		//


//		if kw1=="set" {
//			next = catch(pos,a)
//			key := []byte(a[pos:next])
//			pos = next+1
////			db.Put(writeOptions, key, value)
//			next = resp(pos,a)
//			value := []byte(a[pos:next+1])
//			pos = next+1
//			db.Put(writeOptions, key, value)
//			fmt.Println(kw1)
//			fmt.Println(string(key))
//			fmt.Println(string(value))
//			fmt.Println(ok)
//		}
//		if kw1=="get" {
//			next = catch(pos,a)
//			key := []byte(a[pos:next])
//			pos = next+1
//			slice,err2 := db.Get(readOptions, key)
//			if err2 != nil {
//				log.Println("get data exception：", key, err2)
//				continue
//				}
//			fmt.Println(kw1)
//			fmt.Println(string(key))
//			fmt.Println("get data:",string(slice.Data()))
//			fmt.Println(ok)
//		}
//		if kw1=="exit" {
//			break
//		}
//		if kw1=="keys" {
//			if a[pos]=='*' {
//				it := db.NewIterator(readOptions)
//				defer it.Close()
//				it.SeekToFirst()
//				fmt.Println("Key List:")
//				for it = it; it.Valid(); it.Next() {
//					key := it.Key()
//					value := it.Value()
//					fmt.Println(string(key.Data()))
//					key.Free()
//					value.Free()
//				}
//			}
//			pos+=2
//		}//
//		if kw1=="exists" {
//			continue
//		}
//		if kw1=="type" {
//			continue
//		}
//		if kw1=="del" {
//			continue
//		}
//		if kw1=="setnx" {
//			continue
//		}
//		if kw1=="append" {
//			continue
//		}
//		if kw1=="strlen" {
//			continue
//		}
//		if kw1=="incr" {
//			continue
//		}
//		if kw1=="decr" {
//			continue
//		}
//		if kw1=="incrby" {
//			continue
//		}
//		if kw1=="decrby" {
//			continue
//		}
//		if kw1=="getrange" {
//			continue
//		}
//		if kw1=="setrange" {
//			continue
//		}
//		if kw1=="del" {
//			continue
//		}
//		if kw1=="del" {
//			continue
//		}
	}
	//for {
	//	var input string
	//	fmt.Println("请输入:")
	//	fmt.Scan(&input)
	//	if input == ""{
	//		break
	//	}
	//	if input == "set"{
	//		var k1,vt,v1 string
	//		fmt.Scan(&k1)
	//		fmt.Scan(&vt)
	//		fmt.Scan(&v1)
	//		var key []byte = []byte(k1)
	//		var value []byte = []byte(v1)
	//		db.Put(writeOptions, key, value)
	//	}
	//	if input == "get" {
	//		var k string
	//		fmt.Scan(&k)
	//		var key []byte = []byte(k)
	//		slice, err2 := db.Get(readOptions, key)
	//		if err2 != nil {
	//			log.Println("get data exception：", key, err2)
	//			continue
	//		}
	//		fmt.Println("get data：", slice.Size(), string(slice.Data()))
	//	}
	//
	//}

}

// opendb
//func OpenDB(DB_PATH string) (*gorocksdb.DB, error) {
//	options := gorocksdb.NewDefaultOptions()
//	options.SetCreateIfMissing(true)
//
//	bloomFilter := gorocksdb.NewBloomFilter(10)
//
//	readOptions := gorocksdb.NewDefaultReadOptions()
//	readOptions.SetFillCache(false)
//
//	rateLimiter := gorocksdb.NewRateLimiter(10000000, 10000, 10)
//	options.SetRateLimiter(rateLimiter)
//	options.SetCreateIfMissing(true)
//	options.EnableStatistics()
//	options.SetWriteBufferSize(8 * 1024)
//	options.SetMaxWriteBufferNumber(3)
//	options.SetMaxBackgroundCompactions(10)
//	// options.SetCompression(gorocksdb.SnappyCompression)
//	// options.SetCompactionStyle(gorocksdb.UniversalCompactionStyle)
//
//	options.SetHashSkipListRep(2000000, 4, 4)
//
//	blockBasedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
//	blockBasedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(64 * 1024))
//	blockBasedTableOptions.SetFilterPolicy(bloomFilter)
//	blockBasedTableOptions.SetBlockSizeDeviation(5)
//	blockBasedTableOptions.SetBlockRestartInterval(10)
//	blockBasedTableOptions.SetBlockCacheCompressed(gorocksdb.NewLRUCache(64 * 1024))
//	blockBasedTableOptions.SetCacheIndexAndFilterBlocks(true)
//	blockBasedTableOptions.SetIndexType(gorocksdb.KHashSearchIndexType)
//
//	options.SetBlockBasedTableFactory(blockBasedTableOptions)
//	//log.Println(bloomFilter, readOptions)
//	options.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))
//
//	options.SetAllowConcurrentMemtableWrites(false)
//
//	db, err := gorocksdb.OpenDb(options, DB_PATH)
//
//	if err != nil {
//		log.Fatalln("OPEN DB error", db, err)
//		db.Close()
//		return nil, errors.New("fail to open db")
//	} else {
//		log.Println("OPEN DB success", db)
//	}
//	return db, nil
//}

//func catch(oldpos int, s string) int {
//	pos := oldpos
//	for s[pos]!=' ' && s[pos]!= '\n'{ //
//		pos+=1
//	}
//	return pos
//}
//func resp(pos int, s string) int {
//	if s[pos]!='"'{
//		fmt.Print("error")
//	}
//	pos+=1
//	var end int
//	if s[pos]=='+'{
//		end = sstr(pos,s)
//	}
//	return end
//
//}
//func sstr(pos int, s string) int {
//	end := pos
//	for s[end]!='"'{
//		end+=1
//	}
//	return end
//}
