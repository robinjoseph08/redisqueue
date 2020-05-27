# redisqueue

![Version](https://img.shields.io/badge/version-v1.1.0-green.svg)
[![GoDoc](https://godoc.org/github.com/robinjoseph08/redisqueue?status.svg)](https://godoc.org/github.com/robinjoseph08/redisqueue)
[![Build Status](https://travis-ci.org/robinjoseph08/redisqueue.svg?branch=master)](https://travis-ci.org/robinjoseph08/redisqueue)
[![Coverage Status](https://coveralls.io/repos/github/robinjoseph08/redisqueue/badge.svg?branch=master)](https://coveralls.io/github/robinjoseph08/redisqueue?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/robinjoseph08/redisqueue)](https://goreportcard.com/report/github.com/robinjoseph08/redisqueue)
![License](https://img.shields.io/github/license/robinjoseph08/redisqueue.svg)

`redisqueue` provides a producer and consumer of a queue that uses [Redis
streams](https://redis.io/topics/streams-intro).

## Features

- A `Producer` struct to make enqueuing messages easy.
- A `Consumer` struct to make processing messages concurrenly.
- Claiming and acknowledging messages if there's no error, so that if a consumer
  dies while processing, the message it was working on isn't lost. This
  guarantees at least once delivery.
- A "visibility timeout" so that if a message isn't processed in a designated
  time frame, it will be be processed by another consumer.
- A max length on the stream so that it doesn't store the messages indefinitely
  and run out of memory.
- Graceful handling of Unix signals (`SIGINT` and `SIGTERM`) to let in-flight
  messages complete.
- A channel that will surface any errors so you can handle them centrally.
- Graceful handling of panics to avoid crashing the whole process.
- A concurrency setting to control how many goroutines are spawned to process
  messages.
- A batch size setting to limit the total messages in flight.
- Support for multiple streams.

## Example

Here's an example of a producer that inserts 1000 messages into a queue:

```go
package main

import (
	"fmt"

	"github.com/robinjoseph08/redisqueue"
)

func main() {
	p, err := redisqueue.NewProducerWithOptions(&redisqueue.ProducerOptions{
		StreamMaxLength:      10000,
		ApproximateMaxLength: true,
	})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		err := p.Enqueue(&redisqueue.Message{
			Stream: "redisqueue:test",
			Values: map[string]interface{}{
				"index": i,
			},
		})
		if err != nil {
			panic(err)
		}

		if i%100 == 0 {
			fmt.Printf("enqueued %d\n", i)
		}
	}
}
```

And here's an example of a consumer that reads the messages off of that queue:

```go
package main

import (
	"fmt"
	"time"

	"github.com/robinjoseph08/redisqueue"
)

func main() {
	c, err := redisqueue.NewConsumerWithOptions(&redisqueue.ConsumerOptions{
		VisibilityTimeout: 60 * time.Second,
		BlockingTimeout:   5 * time.Second,
		ReclaimInterval:   1 * time.Second,
		BufferSize:        100,
		Concurrency:       10,
	})
	if err != nil {
		panic(err)
	}

	c.Register("redisqueue:test", process)

	go func() {
		for err := range c.Errors {
			// handle errors accordingly
			fmt.Printf("err: %+v\n", err)
		}
	}()

	fmt.Println("starting")
	c.Run()
	fmt.Println("stopped")
}

func process(msg *redisqueue.Message) error {
	fmt.Printf("processing message: %v\n", msg.Values["index"])
	return nil
}
```
