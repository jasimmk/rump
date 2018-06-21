package main

import (
	"os"
	"fmt"
	"flag"
	"github.com/gomodule/redigo/redis"
)

// Report all errors to stdout.
func handle(err error) {
	if err != nil && err != redis.ErrNil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type DataDump struct {
	Ttl   int64
	Value string
}

// Scan and queue source keys.
func get(conn redis.Conn, queue chan<- map[string]*DataDump) {
	var (
		cursor int64
		keys   []string
	)

	for {
		// Scan a batch of keys.
		values, err := redis.Values(conn.Do("SCAN", cursor))
		handle(err)
		values, err = redis.Scan(values, &cursor, &keys)
		handle(err)

		// Get pipelined dumps.
		for _, key := range keys {
			conn.Send("DUMP", key)
		}
		dumps, err := redis.Strings(conn.Do(""))
		handle(err)

		// Get pipelined TTLs.
		for _, key := range keys {
			conn.Send("PTTL", key)
		}
		ttls, err := redis.Int64s(conn.Do(""))
		handle(err)

		// Build batch map.
		batch := make(map[string]*DataDump)
		for i, _ := range keys {
			batch[keys[i]] = &DataDump{
				Value: dumps[i],
				Ttl:   ttls[i],
			}
		}

		// Last iteration of scan.
		if cursor == 0 {
			// queue last batch.
			select {
			case queue <- batch:
			}
			close(queue)
			break
		}

		fmt.Printf(">")
		// queue current batch.
		queue <- batch
	}
}

// Restore a batch of keys on destination.
func put(conn redis.Conn, queue <-chan map[string]*DataDump) {
	for batch := range queue {
		for key, dump := range batch {
			ttl := dump.Ttl
			if ttl <= 0 {
				ttl = 0
			}
			conn.Send("RESTORE", key, ttl, dump.Value)
		}
		_, err := conn.Do("")
		handle(err)

		fmt.Printf(".")
	}
}

func Sync(from string, to string) {
	source, err := redis.DialURL(from)
	handle(err)
	destination, err := redis.DialURL(to)
	handle(err)
	defer source.Close()
	defer destination.Close()

	// Channel where batches of keys will pass.
	queue := make(chan map[string]*DataDump, 100)

	// Scan and send to queue.
	go get(source, queue)

	// Restore keys as they come into queue.
	put(destination, queue)

	fmt.Println("Sync done.")
}

func main() {
	from := flag.String("from", "", "example: redis://127.0.0.1:6379/0")
	to := flag.String("to", "", "example: redis://127.0.0.1:6379/1")
	flag.Parse()
	Sync(*from, *to)

}
