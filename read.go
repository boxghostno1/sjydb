package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/tecbot/gorocksdb"
	"net"
	"strconv"
)

func read(conn net.Conn, a string, kw1 string, lencmd int, db *gorocksdb.DB, ro *gorocksdb.ReadOptions){
	pos := 0
	result := ""
	tmp := 0
	it := db.NewIterator(ro)
	defer it.Close()
	rand.Seed(time.Now().UnixNano())
	switch kw1{
	case "exists":
		num := 0
		if lencmd < 2 {
			result = amounterr
			break
		}
		for lencmd > 1 {
			lencmd -= 1
			end, length := bulkstring(pos, a)
			pos = end + 1
			keystr := "$" + a[end-1-length:end-1]
			if exist(db, ro, keystr) {
				num+=1
			}
		}
		result = strconv.Itoa(num)
	case "get":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "$" + a[end-1-length:end-1]
		pos = end + 1
		key := []byte(keystr)
		slice, _ := db.Get(ro, key)
		if len(slice.Data()) != 0 {
			result =  string(slice.Data())[1:]
		}else{
			tmp = -1
		}
	case "strlen":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "$" + a[end-1-length:end-1]
		pos = end + 1
		key := []byte(keystr)
		slice,_ := db.Get(ro, key)
		if len(slice.Data()) == 0 {
			result = strconv.Itoa(0)
		} else {
			result = strconv.Itoa(len(string(slice.Data()))-1)
		}
	case "getrange":
		if lencmd != 4 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "$" + a[end-1-length:end-1]
		pos = end + 1
		end, length = bulkstring(pos, a)
		pos = end + 1
		astart,_ := strconv.Atoi(a[end-1-length:end-1])
		end, length = bulkstring(pos, a)
		aend,_ := strconv.Atoi(a[end-1-length:end-1])
		pos = end + 1
		key := []byte(keystr)
		slice,_ := db.Get(ro, key)
		if len(slice.Data()) == 0 {
			result = ""
		} else {
			valuestr := string(slice.Data())[1:]
			if astart<0{astart+=len(valuestr)}
			if aend<0{aend+=len(valuestr)}
			aend+=1
			if astart>=0 && astart<len(valuestr) && aend>0{
				var res string
				if aend > len(valuestr) {
					res = valuestr[astart:]
				} else {
					res = valuestr[astart:aend]
				}
				result = res
			}
		}
	case "llen":
		listlen := 0
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "L" + a[end-1-length:end-1]
		pos = end + 1
		if exist(db, ro, keystr) {
			listlen, _, _, _ = message(db, ro, keystr)
		}
		result = strconv.Itoa(listlen)
	case "lindex":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "L" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			tmp = -1
			break
		}
		end, length = bulkstring(pos, a)
		index,err := strconv.Atoi(a[end-1-length:end-1])
		if err!=nil{
			tmp = -1
			break
		}
		pos = end + 1
		listlen, leftseq, _, _ := message(db, ro, keystr)
		if index<0{index+=listlen}
		if index>=listlen{
			tmp = -1
		}else{
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
			result = string(itemvalue.Data())[16:]
			itemvalue.Free()
		}
	case "hget":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "H" + a[end-1-length:end-1]
		pos = end + 1
		end, length = bulkstring(pos, a)
		hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
		pos = end + 1
		slice, _ := db.Get(ro, []byte(hashkeystr))
		if len(slice.Data()) == 0 {
			tmp = -1
		} else {
			result = string(slice.Data())[1:]
		}
	case "hexists":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "H" + a[end-1-length:end-1]
		pos = end + 1
		end, length = bulkstring(pos, a)
		hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
		pos = end + 1
		if exist(db, ro, hashkeystr) {
			result = "1"
		} else {
			result = "0"
		}
	case "hlen":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "H" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			result = "0"
		} else {
			slice, _ := db.Get(ro, []byte(keystr))
			result = string(slice.Data())
		}
	case "hgetall":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "H" + a[end-1-length:end-1]
		pos = end + 1
		if exist(db, ro, keystr) {
			slice, _ := db.Get(ro, []byte(keystr))
			hashlen, _ := strconv.Atoi(string(slice.Data()))
			it.Seek([]byte("h" + keystr[1:]))
			for hashlen > 0 {
				hashlen -= 1
				key := it.Key()
				value := it.Value()
				result += string(key.Data())[len(keystr):]+"\r\n"+string(value.Data())[1:]+"\r\n"
				key.Free()
				value.Free()
				it.Next()
			}
		}

		//case "keys"
	case "scard":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "S" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			result = "0"
		} else {
			slice, _ := db.Get(ro, []byte(keystr))
			result = string(slice.Data())
		}
	case "sismember":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "S" + a[end-1-length:end-1]
		pos = end + 1
		end, length = bulkstring(pos, a)
		memberstr := "s" + keystr[1:] + a[end-1-length:end-1]
		if exist(db, ro, memberstr) {
			result = "1"
		} else {
			result = "0"
		}
	case "smembers":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "S" + a[end-1-length:end-1]
		pos = end + 1
		slice, _ := db.Get(ro, []byte(keystr))
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
			result += string(key.Data())[len(keystr):]+"\r\n"
			key.Free()
			it.Next()
		}
	case "srandmember":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "S" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			fmt.Println("nil")
		} else {
			slice, _ := db.Get(ro, []byte(keystr))
			scard, _ := strconv.Atoi(string(slice.Data()))
			index := rand.Intn(scard)
			it.Seek([]byte("s" + keystr[1:]))
			for index > 0 {
				it.Next()
				index -= 1
			}
			key := it.Key()
			result = string(key.Data())[len(keystr):]
		}
	case "zcard":
		if lencmd != 2 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "Z" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			result = "0"
		} else {
			slice, _ := db.Get(ro, []byte(keystr))
			result = string(slice.Data())
		}
	case "zscore":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "Z" + a[end-1-length:end-1]
		pos = end + 1
		end, length = bulkstring(pos, a)
		member := a[end-1-length:end-1]
		pos = end + 1
		zkeystr,index := zexists(db,ro,keystr,member)
		if index<0{
			tmp = -1
		}else{
			score,_ := strconv.Atoi(zkeystr[len(keystr):len(keystr)+8])
			result = strconv.Itoa(score)
		}
	case "zrank":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "Z" + a[end-1-length:end-1]
		pos = end + 1
		end, length = bulkstring(pos, a)
		member := a[end-1-length:end-1]
		pos = end + 1
		_,index := zexists(db,ro,keystr,member)
		if index<0{
			tmp = -1
		}else{
			result = strconv.Itoa(index)
		}
	case "zcount":
		if lencmd != 4 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "Z" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			result = "0"
			break
		}
		end, length = bulkstring(pos, a)
		min,_ := strconv.Atoi(a[end-1-length:end-1])
		pos = end + 1
		end, length = bulkstring(pos, a)
		max,_ := strconv.Atoi(a[end-1-length:end-1])
		pos = end + 1
		slice,_ := db.Get(ro,[]byte(keystr))
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
		result = strconv.Itoa(count)
	case "zrange":
		if lencmd != 4 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "Z" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			break
		}
		end, length = bulkstring(pos, a)
		start,_ := strconv.Atoi(a[end-1-length:end-1])
		pos = end + 1
		end, length = bulkstring(pos, a)
		stop,_ := strconv.Atoi(a[end-1-length:end-1])
		pos = end + 1
		slice,_ := db.Get(ro,[]byte(keystr))
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
				result += zkeystr[len(keystr)+8:]+"\r\n"
				it.Next()
				i+=1
			}
		}
	}
	backstream := back(result,tmp)
	conn.Write(backstream)
	<-ch
}
