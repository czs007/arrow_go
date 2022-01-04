package main

import (
	"context"
	"example.com/arrow_go/storage"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	_ "net/http/pprof"
)

func ServePProf() {
	go func() {
		if err := http.ListenAndServe("0.0.0.0:6060", nil); err != nil {
			panic("Start web service for pprof failed")
		}
	}()
}

func execute(i int, enableGC bool) {
	fmt.Println("round ", i)
	b, err := ioutil.ReadFile("parquet-arrow-example2.parquet") // just pass the file name
	if err != nil {
		fmt.Print(err)
	}
	fmt.Println(len(b))

	r, err2 := storage.NewPayloadReader(storage.DataType_Int64, b)
	if err2 != nil {
		panic(err2.Error())
	}
	fmt.Println(r)
	for j := 0; j < 10; j++ {
		int64s, _ := r.GetInt64FromPayload()
		fmt.Println(len(int64s))
	}
	err = r.ReleasePayloadReader()
	if err != nil {
		fmt.Println(err.Error())
	}
	if enableGC {
		runtime.GC()
	}
}

func main() {
	if len(os.Args) < 1 {
		_, _ = fmt.Fprint(os.Stderr, "usage: my_example --enableGC\n")
		return
	}

	ServePProf()
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var enableGC bool
	flags.BoolVar(&enableGC, "enableGC", false, "enable manually gc")
	if err := flags.Parse(os.Args[1:]); err != nil {
		os.Exit(-1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context, group *sync.WaitGroup) {
		defer group.Done()
		for {
			select {
			case <-ctx.Done():
				return
				break
			}
		}
	}(ctx, &wg)
	for i := 0; i < 1000; i++ {
		execute(i, enableGC)
	}
	cancel()
	time.Sleep(10 * time.Second)
	wg.Wait()
}
