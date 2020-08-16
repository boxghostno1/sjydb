package main

import (
	"github.com/tecbot/gorocksdb"
	"strconv"
)

func exist(db *gorocksdb.DB, ro *gorocksdb.ReadOptions, keystr string) bool {
	key := []byte(keystr)
	slice, _ := db.Get(ro, key)
	if len(slice.Data()) == 0 {
		return false
	} else {
		return true
	}
}

func createnewlist(db *gorocksdb.DB, wo *gorocksdb.WriteOptions, keystr string, item1 string) {
	key := []byte(keystr)
	valuestr := B8(1) + B8(1) + B8(1) + B8(2)
	value := []byte(valuestr)
	db.Put(wo, key, value)
	item1key := []byte("l" + keystr[1:] + B8(1))
	item1valuestr := B8(0) + B8(0) + item1
	item1value := []byte(item1valuestr)
	db.Put(wo, item1key, item1value)
}
func B8(x int) string {
	strx := strconv.Itoa(x)
	for len(strx) < 8 {
		strx = "0" + strx
	}
	return strx
}

//func newcommand(pos int, s string) int {
//	var end int
//	if s[pos]!='*' {
//		end = -1
//	}else{
//		pos+=1
//		for s[pos]!='\r'{
//			pos+=1
//		}
//		pos+=1
//		if s[pos]=='\n'{
//			end = pos
//		} else{
//			end = -1
//		}
//	}
//	return end
//}
//func bulkstring(pos int, s string) (int,int) {
//	end := pos+1
//	for s[end]!='\r'{
//		end+=1
//	}
//	length,_ := strconv.Atoi(s[pos+1:end])
//	if length==-1{
//		end +=1
//	}else{
//		end += 3+length
//	}
//	return end,length
//}
func ignore(pos int, s string, lencmd int) int {
	return 0
}
func inter(pos int, s string) (int, int) {
	num := 0
	end := pos + 1
	for s[end] != '\r' {
		num = (num * 10) + (int(s[end]) - 48)
		end += 1
	}
	end += 1
	return end, num
}
func message(db *gorocksdb.DB, ro *gorocksdb.ReadOptions, keystr string) (int, int, int, int) {
	key := []byte(keystr)
	slice, _ := db.Get(ro, key)
	valuestr := string(slice.Data())
	length, _ := strconv.Atoi(valuestr[:8])
	leftseq, _ := strconv.Atoi(valuestr[8:16])
	rightseq, _ := strconv.Atoi(valuestr[16:24])
	curseq, _ := strconv.Atoi(valuestr[24:32])
	return length, leftseq, rightseq, curseq
}
func createnewhash(db *gorocksdb.DB, wo *gorocksdb.WriteOptions, keystr string) {
	key := []byte(keystr)
	valuestr := "0"
	value := []byte(valuestr)
	db.Put(wo, key, value)
}
func happend(db *gorocksdb.DB, wo *gorocksdb.WriteOptions, keystr string, vaulestr string) {

}
func zexists(db *gorocksdb.DB, ro *gorocksdb.ReadOptions, keystr string, member string) (string, int) {
	if !exist(db, ro, keystr) {
		return "", -1 //key not exist
	} else {
		it := db.NewIterator(ro)
		it.Seek([]byte(keystr))
		value := it.Value()
		zlen, _ := strconv.Atoi(string(value.Data()))
		value.Free()
		it.Seek([]byte("z" + keystr[1:]))
		rank := 0
		for rank < zlen {
			zkey := it.Key()
			zkeystr := string(zkey.Data())
			item := zkeystr[len(keystr)+8:]
			if item == member {
				return zkeystr, rank
			}
			it.Next()
			rank += 1
		}
		return "", -2 //key exist , member not exist
	}

}

//func getkw1(str string) (string,int,int){
//	pos := 0
//	next := newcommand(pos,str)
//	lencmd,_ := strconv.Atoi(str[pos+1:next-1])
//	pos = next+1
//	end,length := bulkstring(pos,str)
//	kw1 := str[end-1-length:end-1]
//	pos = end+1
//	return kw1,lencmd,pos
//}
//func back(str string, tmp int) []byte{
//	var result string
//	if tmp<0{
//		result = "$-1\r\n"
//	}else{
//		result = "$"+strconv.Itoa(len(str))+"\r\n"+str+"\r\n"
//		if len(result)%10==0{
//			result+=" "
//		}
//	}
//
//	return []byte(result)
//}
//func readback(str string) string{
//	end := 1
//	for str[end]!='\r'{
//		end+=1
//	}
//	length,_ := strconv.Atoi(str[1:end])
//	if length==-1{
//		return "nil"
//	}else{
//
//	}
//
//}
