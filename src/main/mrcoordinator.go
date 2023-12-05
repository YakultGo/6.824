package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"path/filepath"
	"src/mr"
	"strings"
)
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	removeTmpFile()
	time.Sleep(time.Second)
}

// 删除map中间文件
func removeTmpFile() {
	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		filename := filepath.Base(path)
		if strings.HasPrefix(filename, "mr-") && !strings.HasSuffix(filename, "mr-out") {
			_ = os.Remove(path)
		}
		return nil
	})
}
