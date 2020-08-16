package main

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
	"math/rand"
	"net"
	"strconv"
	"time"
)

func write(conn net.Conn, a string, kw1 string, lencmd int, db *gorocksdb.DB, ro *gorocksdb.ReadOptions, wo *gorocksdb.WriteOptions){

	mu.Lock()

	for len(ch)>0{
		time.Sleep(time.Second)
	}
	pos := 0
	result := ""
	tmp := 0
	it := db.NewIterator(ro)
	defer it.Close()
	rand.Seed(time.Now().UnixNano())
	switch kw1{
	case "set":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "$" + a[end-1-length:end-1]
		pos = end + 1
		key := []byte(keystr)
		end, length = bulkstring(pos, a)
		valuestr := a[end-1-length : end-1]
		value := []byte("$" + valuestr)
		pos = end + 1
		db.Put(wo, key, value)
		result = ok
	case "setnx":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "$" + a[end-1-length:end-1]
		pos = end + 1
		end, length = bulkstring(pos, a)
		pos = end + 1
		if !exist(db,ro,keystr) {
			value := []byte("$" + a[end-1-length:end-1])
			db.Put(wo, []byte(keystr), value)
			result = "1"
		} else {
			result = "0"
		}
	case "append":
		if lencmd != 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "$" + a[end-1-length:end-1]
		pos = end + 1
		key := []byte(keystr)
		end, length = bulkstring(pos, a)
		slice, _ := db.Get(ro, key)
		pos = end + 1
		valuestr := string(slice.Data()) + a[end-1-length:end-1]
		if len(slice.Data()) == 0 {
			valuestr = "$"+valuestr
		}
		value := []byte(valuestr)
		db.Put(wo,key,value)
		result = strconv.Itoa(len(valuestr)-1)
	case "setrange":
		if lencmd != 4 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "$" + a[end-1-length:end-1]
		pos = end + 1
		key := []byte(keystr)
		end, index := inter(pos, a)
		pos = end + 1
		end, length = bulkstring(pos, a)
		appstr := a[end-1-length : end-1]
		slice, _ := db.Get(ro, key)
		pos = end + 1
		valuestr := string(slice.Data())
		if len(valuestr)==0{
			valuestr+="$"
		}
		if len(valuestr) < index+1 {
			i := index + 1 - len(valuestr)
			for i > 0 {
				i -= 1
				valuestr += "\u0000"
			}
		}
		valuestr = valuestr[:index+1] + appstr
		value := []byte(valuestr)
		db.Put(wo, key, value)
		result = strconv.Itoa(len(valuestr)-1)
	case "lpush":
		if lencmd < 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "L" + a[end-1-length:end-1]
		pos = end + 1
		var finallen int
		if !exist(db, ro, keystr) {
			end, length = bulkstring(pos, a)
			item1 := a[end-1-length : end-1]
			createnewlist(db, wo, keystr, item1)
			lencmd -= 1
			pos = end + 1
			finallen = 1
		}
		for lencmd > 2 {
			lencmd -= 1
			end, length = bulkstring(pos, a)
			pos = end + 1
			itemvaluestr := a[end-1-length : end-1]
			listlen, leftseq, rightseq, curseq := message(db, ro, keystr)
			itemkeystr := "l" + keystr[1:] + B8(curseq)
			itemvaluestr = B8(0) + B8(leftseq) + itemvaluestr
			db.Put(wo, []byte(itemkeystr), []byte(itemvaluestr)) //append new leftseq
			olditemkeystr := "l" + keystr[1:] + B8(leftseq)
			slice, _ := db.Get(ro, []byte(olditemkeystr))
			olditemvaluestr := string(slice.Data())
			olditemvaluestr = B8(curseq) + olditemvaluestr[8:]
			db.Put(wo, []byte(olditemkeystr), []byte(olditemvaluestr)) //change old leftseq
			newlistvaluestr := B8(listlen+1) + B8(curseq) + B8(rightseq) + B8(curseq+1)
			db.Put(wo, []byte(keystr), []byte(newlistvaluestr)) //change list message
			finallen = listlen + 1
		}
		result = strconv.Itoa(finallen)
	case "rpush":
		if lencmd < 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "L" + a[end-1-length:end-1]
		pos = end + 1
		var finallen int
		if !exist(db, ro, keystr) {
			end, length = bulkstring(pos, a)
			item1 := a[end-1-length : end-1]
			createnewlist(db, wo, keystr, item1)
			lencmd -= 1
			pos = end + 1
			finallen = 1
		}
		for lencmd > 2 {
			lencmd -= 1
			end, length = bulkstring(pos, a)
			pos = end + 1
			itemvaluestr := a[end-1-length : end-1]
			listlen, leftseq, rightseq, curseq := message(db, ro, keystr)
			itemkeystr := "l" + keystr[1:] + B8(curseq)
			itemvaluestr = B8(rightseq) + B8(0) + itemvaluestr
			db.Put(wo, []byte(itemkeystr), []byte(itemvaluestr)) //append new leftseq
			olditemkeystr := "l" + keystr[1:] + B8(rightseq)
			slice, _ := db.Get(ro, []byte(olditemkeystr))
			olditemvaluestr := string(slice.Data())
			olditemvaluestr = olditemvaluestr[:8] + B8(curseq) + olditemvaluestr[16:]
			db.Put(wo, []byte(olditemkeystr), []byte(olditemvaluestr)) //change old leftseq
			newlistvaluestr := B8(listlen+1) + B8(leftseq) + B8(curseq) + B8(curseq+1)
			db.Put(wo, []byte(keystr), []byte(newlistvaluestr)) //change list message
			finallen = listlen + 1
		}
		result = strconv.Itoa(finallen)
	case "lpop":
		if lencmd != 2 {
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
		listlen, leftseq, rightseq, curseq := message(db, ro, keystr)
		if listlen == 1 {
			slice,_ := db.Get(ro,[]byte("l"+keystr[1:]+B8(leftseq)))
			db.Delete(wo, []byte(keystr))
			db.Delete(wo, []byte("l"+keystr[1:]+B8(leftseq)))
			result = string(slice.Data())[16:]
			break
		}
		popitemkeystr := "l" + keystr[1:] + B8(leftseq)
		it.Seek([]byte(popitemkeystr))
		value := it.Value()
		result = string(value.Data())[16:]
		newleftseq, _ := strconv.Atoi(string(value.Data())[8:16])
		newleftkeystr := "l" + keystr[1:] + B8(newleftseq)
		it.Seek([]byte(newleftkeystr))
		newleftvalue := it.Value()
		newleftvaluestr := string(newleftvalue.Data())
		newleftvalue.Free()
		newleftvaluestr = B8(0) + newleftvaluestr[8:]
		db.Delete(wo, []byte(popitemkeystr))                       //lpop
		db.Put(wo, []byte(newleftkeystr), []byte(newleftvaluestr)) //change newleftitem
		newlistvaluestr := B8(listlen-1) + B8(newleftseq) + B8(rightseq) + B8(curseq)
		db.Put(wo, []byte(keystr), []byte(newlistvaluestr)) //change list message
	case "rpop":
		if lencmd != 2 {
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
		listlen, leftseq, rightseq, curseq := message(db, ro, keystr)
		if listlen == 1 {
			slice,_ := db.Get(ro,[]byte("l"+keystr[1:]+B8(leftseq)))
			db.Delete(wo, []byte(keystr))
			db.Delete(wo, []byte("l"+keystr[1:]+B8(leftseq)))
			result = string(slice.Data())[16:]
			break
		}
		popitemkeystr := "l" + keystr[1:] + B8(rightseq)
		it.Seek([]byte(popitemkeystr))
		value := it.Value()
		result = string(value.Data())[16:]
		value.Free()
		newrightseq, _ := strconv.Atoi(string(value.Data())[:8])
		newrightkeystr := "l" + keystr[1:] + B8(newrightseq)
		it.Seek([]byte(newrightkeystr))
		newrightvalue := it.Value()
		newrightvaluestr := string(newrightvalue.Data())
		newrightvalue.Free()
		newrightvaluestr = newrightvaluestr[:8] + B8(0) + newrightvaluestr[16:]
		db.Delete(wo, []byte(popitemkeystr))                         //rpop
		db.Put(wo, []byte(newrightkeystr), []byte(newrightvaluestr)) //change newrightitem
		newlistvaluestr := B8(listlen-1) + B8(leftseq) + B8(newrightseq) + B8(curseq)
		db.Put(wo, []byte(keystr), []byte(newlistvaluestr)) //change list message
	case "lset":
		if lencmd != 4 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "L" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			result = "list not exist"
			break
		}
		end, length = bulkstring(pos, a)
		pos = end + 1
		index,err := strconv.Atoi(a[end-1-length:end-1])
		if err!=nil{
			result = "index need to be integer"
			break
		}
		end, length = bulkstring(pos, a)
		pos = end + 1
		valuestr := a[end-1-length : end-1]
		listlen, leftseq, _, _ := message(db, ro, keystr)
		if index < 0 {
			index += listlen
		}
		if index >= listlen {
			result = "list out of range"
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
			db.Put(wo, itemkey.Data(), []byte(valuestr))
			result = ok
			itemkey.Free()
			itemvalue.Free()
		}
	case "linsert":
		if lencmd != 5 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "L" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			result = "-1"
			break
		}
		end, length = bulkstring(pos, a)
		kw2 := a[end-1-length : end-1] //before or after
		pos = end + 1
		end, length = bulkstring(pos, a)
		pos = end + 1
		pivot := a[end-1-length : end-1]
		end, length = bulkstring(pos, a)
		element := a[end-1-length : end-1]
		pos = end + 1
		listlen, leftseq, rightseq, curseq := message(db, ro, keystr)
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
					db.Put(wo, []byte(insertkeystr), []byte(insertvaluestr))
					db.Put(wo, []byte(itemkeystr), []byte(itemvaluestr))
					if preseq == "00000000" {
						leftseq = curseq
					} else {
						prekeystr := "l" + keystr[1:] + preseq
						it.Seek([]byte(prekeystr))
						prevalue := it.Value()
						prevaluestr := string(prevalue.Data())
						prevaluestr = prevaluestr[:8] + B8(curseq) + prevaluestr[16:]
						db.Put(wo, []byte(prekeystr), []byte(prevaluestr))
					}
				case "after":
					insertvaluestr := thisseq + nextseq + element
					itemvaluestr = itemvaluestr[:8] + B8(curseq) + itemvaluestr[16:]
					db.Put(wo, []byte(insertkeystr), []byte(insertvaluestr))
					db.Put(wo, []byte(itemkeystr), []byte(itemvaluestr))
					if nextseq == "00000000" {
						rightseq = curseq
					} else {
						nextkeystr := "l" + keystr[1:] + nextseq
						it.Seek([]byte(nextkeystr))
						nextvalue := it.Value()
						nextvaluestr := string(nextvalue.Data())
						nextvaluestr = B8(curseq) + nextvaluestr[8:]
						db.Put(wo, []byte(nextkeystr), []byte(nextvaluestr))
					}
				}
				messstr := B8(listlen+1) + B8(leftseq) + B8(rightseq) + B8(curseq+1)
				db.Put(wo, []byte(keystr), []byte(messstr))
				break
			}
			it.Seek([]byte(nextkey))
			i -= 1
		}
		if i == 0 {
			result = "-1"
		} else {
			result = strconv.Itoa(listlen+1)
		}
	case "lrem":
		if lencmd != 4 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "L" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			result = "0"
			break
		}
		end, count := inter(pos, a)
		pos = end + 1
		end, length = bulkstring(pos, a)
		pos = end + 1
		element := a[end-1-length : end-1]
		listlen, leftseq, rightseq, curseq := message(db, ro, keystr)
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
					slice, _ := db.Get(ro, []byte(prekeystr))
					prevaluestr := string(slice.Data())
					prevaluestr = prevaluestr[:8] + nextseq + prevaluestr[16:]
					db.Put(wo, []byte(prekeystr), []byte(prevaluestr))

				} else {
					leftseq, _ = strconv.Atoi(nextseq)
				}
				if nextseq != "00000000" {
					nextkeystr := "l" + keystr[1:] + nextseq
					slice, _ := db.Get(ro, []byte(nextkeystr))
					nextvaluestr := string(slice.Data())
					nextvaluestr = preseq + nextvaluestr[8:]
					db.Put(wo, []byte(nextkeystr), []byte(nextvaluestr))
				} else {
					rightseq, _ = strconv.Atoi(preseq)
				}
				db.Delete(wo, []byte(itemkeystr))
				thecount += 1
				//nextkeystr := "l"+keystr[1:]+nextseq
				messstr := B8(listlen-thecount) + B8(leftseq) + B8(rightseq) + B8(curseq)
				db.Put(wo, []byte(keystr), []byte(messstr))
				if thecount >= listlen {
					db.Delete(wo, []byte(keystr))
					break
				}
				if thecount >= count {
					break
				}
			}
			it.Seek([]byte(nextkey))
			i -= 1
		}
		result = strconv.Itoa(thecount)
	case "hset":
		if lencmd < 4 || lencmd%2 != 0 {
			result = amounterr
			break
		}
		createnum := 0
		end, length := bulkstring(pos, a)
		keystr := "H" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			createnewhash(db, wo, keystr)
		}
		for lencmd > 2 {
			lencmd -= 2
			end, length = bulkstring(pos, a)
			pos = end + 1
			hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
			if !exist(db, ro, hashkeystr) {
				createnum += 1
			}
			end, length = bulkstring(pos, a)
			pos = end + 1
			hashvaluestr := "h" + a[end-1-length:end-1]
			db.Put(wo, []byte(hashkeystr), []byte(hashvaluestr))
		}
		slice, _ := db.Get(ro, []byte(keystr))
		hashlen, _ := strconv.Atoi(string(slice.Data()))
		hashlen += createnum
		db.Put(wo, []byte(keystr), []byte(strconv.Itoa(hashlen)))
		result = strconv.Itoa(createnum)
	case "hdel":
		if lencmd < 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "H" + a[end-1-length:end-1]
		pos = end + 1
		if exist(db, ro, keystr) {
			removenum := 0
			for lencmd > 2 {
				lencmd -= 1
				end, length = bulkstring(pos, a)
				hashkeystr := "h" + keystr[1:] + a[end-1-length:end-1]
				pos = end + 1
				if exist(db, ro, hashkeystr) {
					db.Delete(wo, []byte(hashkeystr))
					removenum += 1
				}
			}
			slice, _ := db.Get(ro, []byte(keystr))
			hashlen, _ := strconv.Atoi(string(slice.Data()))
			hashlen -= removenum
			if hashlen == 0 {
				db.Delete(wo, []byte(keystr))
			} else {
				db.Put(wo, []byte(keystr), []byte(strconv.Itoa(hashlen)))
			}
			result = strconv.Itoa(removenum)
		} else {
			result = "0"
		}
	case "sadd":
		if lencmd < 3 {
			result = amounterr
			break
		}
		addnum := 0
		end, length := bulkstring(pos, a)
		keystr := "S" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			createnewhash(db, wo, keystr)
		}
		for lencmd > 2 {
			lencmd -= 1
			end, length = bulkstring(pos, a)
			pos = end + 1
			setmemberstr := "s" + keystr[1:] + a[end-1-length:end-1]
			if !exist(db, ro, setmemberstr) {
				addnum += 1
			}
			db.Put(wo, []byte(setmemberstr), []byte("s"))
		}
		slice, _ := db.Get(ro, []byte(keystr))
		scard, _ := strconv.Atoi(string(slice.Data()))
		scard += addnum
		db.Put(wo, []byte(keystr), []byte(strconv.Itoa(scard)))
		result = strconv.Itoa(addnum)
	case "srem":
		if lencmd < 3 {
			result = amounterr
			break
		}
		end, length := bulkstring(pos, a)
		keystr := "S" + a[end-1-length:end-1]
		pos = end + 1
		if exist(db, ro, keystr) {
			removenum := 0
			for lencmd > 2 {
				lencmd -= 1
				end, length = bulkstring(pos, a)
				memberstr := "s" + keystr[1:] + a[end-1-length:end-1]
				pos = end + 1
				if exist(db, ro, memberstr) {
					db.Delete(wo, []byte(memberstr))
					removenum += 1
				}
			}
			slice, _ := db.Get(ro, []byte(keystr))
			scard, _ := strconv.Atoi(string(slice.Data()))
			scard -= removenum
			if scard == 0 {
				db.Delete(wo, []byte(keystr))
			} else {
				db.Put(wo, []byte(keystr), []byte(strconv.Itoa(scard)))
			}
			result = strconv.Itoa(removenum)
		} else {
			result = "0"
		}
	case "spop":
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
			memberkeystr := key.Data()
			db.Delete(wo, memberkeystr)
			if scard == 1 {
				db.Delete(wo, []byte(keystr))
			} else {
				db.Put(wo, []byte(keystr), []byte(strconv.Itoa(scard-1)))
			}
			result = string(memberkeystr)[len(keystr):]
		}
	case "zadd":
		if lencmd < 4 {
			result = amounterr
			break
		}
		addnum := 0
		end, length := bulkstring(pos, a)
		keystr := "Z" + a[end-1-length:end-1]
		pos = end + 1
		if !exist(db, ro, keystr) {
			createnewhash(db, wo, keystr)
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
			oldstr, para := zexists(db, ro, keystr, member)
			if para < 0 {
				addnum += 1
				slice, _ := db.Get(ro, []byte(keystr))
				zcard, _ := strconv.Atoi(string(slice.Data()))
				zcard += 1
				db.Put(wo, []byte(keystr), []byte(strconv.Itoa(zcard)))
			} else {
				db.Delete(wo, []byte(oldstr))
			}
			db.Put(wo, []byte(mkeystr), []byte("z"))
		}
		result = strconv.Itoa(addnum)
		//slice,_ := db.Get(readOptions,[]byte(keystr))
		//zcard,_ := strconv.Atoi(string(slice.Data()))
		//fmt.Println(zcard)
		//zcard += addnum
		//db.Put(writeOptions,[]byte(keystr),[]byte(strconv.Itoa(zcard)))
		//fmt.Println(zcard)
		//fmt.Println(addnum)
	case "zrem":
		if lencmd < 3 {
			result = amounterr
			break
		}
		removenum := 0
		end, length := bulkstring(pos, a)
		keystr := "Z" + a[end-1-length:end-1]
		pos = end + 1
		if exist(db, ro, keystr) {
			for lencmd > 2 {
				lencmd -= 1
				end, length = bulkstring(pos, a)
				pos = end + 1
				member := a[end-1-length : end-1]
				oldstr, para := zexists(db, ro, keystr, member)
				if para >= 0 {
					removenum += 1
					slice, _ := db.Get(ro, []byte(keystr))
					zcard, _ := strconv.Atoi(string(slice.Data()))
					zcard -= 1
					db.Put(wo, []byte(keystr), []byte(strconv.Itoa(zcard)))
					db.Delete(wo, []byte(oldstr))
				}
			}
		}
		result = strconv.Itoa(removenum)
	}
	backstream := back(result,tmp)
	conn.Write(backstream)
	mu.Unlock()
}
