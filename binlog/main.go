package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"strconv"
)

func main() {

	binlog, _ := os.Open("/Users/leo/go/src/github.com/totoleo/beanstool/binlog/binlog.46767")

	reader := &BinlogReader{bufio.NewReaderSize(binlog, 11<<20)}

	version := make([]byte, 4)
	err := reader.readFull(version, "version")
	if err != nil {
		panic(err)
	}
	v := binary.LittleEndian.Uint32(version)
	switch v {
	case 7:
		var err error
		var c bool = true
		for err == nil && c {
			c, err = reader.readrec()
			if err != nil {
				fmt.Println(err)
			}
		}
		break
	case 5:
		break

	}

}

type BinlogReader struct {
	r *bufio.Reader
}
type Jobrec struct {
	Id          uint64 // id >= 1
	Pri         uint32
	_           uint32 // 注意这里有一个内存对齐导致的 padding
	Delay       int64  // 精确到纳秒
	Ttr         int64  // 精确到纳秒
	Body_size   int32
	_           int32  // 这里是另外一个内存对齐导致的 padding
	Created_at  int64  // 创建时间， epoch 纪年，精确到纳秒
	Deadline_at int64  // 下一个会因超时而产生状态变迁的时间
	Reserve_ct  uint32 // reserve 状态切换计数，_ct 结尾的都是状态计数
	Timeout_ct  uint32
	Release_ct  uint32
	Bury_ct     uint32
	Kick_ct     uint32
	State       byte
	_           [3]byte // 又一个 padding
}

func (j Jobrec) String() string {
	bytes, _ := json.Marshal(j)
	return string(bytes)
}

func (r *BinlogReader) readrec() (bool, error) {
	var nameLen int32
	err := binary.Read(r.r, binary.LittleEndian, &nameLen)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, errors.New("error reading name len")
	}

	name := make([]byte, nameLen)
	err = r.readFull(name, "tube name")
	if err != nil {
		return false, err
	}
	var jobRec Jobrec
	err = binary.Read(r.r, binary.LittleEndian, &jobRec)
	if err != nil {
		return false, errors.New("error reading name len")
	}
	if nameLen == 0 {
		return true, nil
	}
	if jobRec.Id == 0 {
		return false, nil
	}
	body := make([]byte, jobRec.Body_size)
	_, err = r.r.Read(body)
	if err != nil {
		return true, err
	}
	if string(name) == "SettlementAction" {
		//fmt.Println("tube", string(name), "| jobRec", jobRec)
		fmt.Print(string(body))
	}

	return true, nil
}

func (r *BinlogReader) readFull(out []byte, desc string) error {
	n, err := r.r.Read(out)
	if n < len(out) || err != nil {
		fmt.Println(string(out))
		return errors.New("error reading " + desc + strconv.FormatInt(int64(len(out)), 10))
	}
	return nil
}
