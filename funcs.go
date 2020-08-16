package main

import "strconv"

func getkw1(str string) (string, int, int) {
	pos := 0
	next := newcommand(pos, str)
	lencmd, _ := strconv.Atoi(str[pos+1 : next-1])
	pos = next + 1
	end, length := bulkstring(pos, str)
	kw1 := str[end-1-length : end-1]
	pos = end + 1
	return kw1, lencmd, pos
}
func back(str string, tmp int) []byte {
	var result string
	if tmp < 0 {
		result = "$-1\r\n"
	} else {
		result = "$" + strconv.Itoa(len(str)) + "\r\n" + str + "\r\n"
		if len(result)%10 == 0 {
			result += " "
		}
	}

	return []byte(result)
}
func newcommand(pos int, s string) int {
	var end int
	if s[pos] != '*' {
		end = -1
	} else {
		pos += 1
		for s[pos] != '\r' {
			pos += 1
		}
		pos += 1
		if s[pos] == '\n' {
			end = pos
		} else {
			end = -1
		}
	}
	return end
}
func bulkstring(pos int, s string) (int, int) {
	end := pos + 1
	for s[end] != '\r' {
		end += 1
	}
	length, _ := strconv.Atoi(s[pos+1 : end])
	if length == -1 {
		end += 1
	} else {
		end += 3 + length
	}
	return end, length
}
