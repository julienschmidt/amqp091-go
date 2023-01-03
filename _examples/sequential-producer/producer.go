// This example declares a durable exchange, and publishes one messages to that
// exchange. This example waits for a publish-confirmation before sending another
// message.
//
package main

import (
	"context"
	"flag"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	routingKey   = flag.String("key", "test-key", "AMQP routing key")
	body         = flag.String("body", "foobar", "Body of message")
	continuous   = flag.Bool("continuous", false, "Keep publishing messages at a 1msg/sec rate")
	ErrLog       = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
	Log          = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
)

func init() {
	flag.Parse()
}

func main() {
	done := make(chan struct{})

	SetupCloseHandler(done)

	publish(context.Background(), done)
}

func publish(ctx context.Context, done chan struct{}) {
	config := amqp.Config{
		Vhost:      "/",
		Properties: amqp.NewConnectionProperties(),
	}
	config.Properties.SetClientConnectionName("sequential-producer")

	Log.Printf("dialing %s", uri)
	conn, err := amqp.Dial(*uri)
	if err != nil {
		ErrLog.Fatalf("error in dial: %s", err)
	}
	defer conn.Close()

	Log.Println("got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		ErrLog.Fatalf("error getting a channel: %s", err)
	}

	Log.Printf("declaring exchange")
	if err := channel.ExchangeDeclare(
		*exchange,     // name
		*exchangeType, // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // noWait
		nil,           // arguments
	); err != nil {
		ErrLog.Fatalf("Exchange Declare: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	Log.Printf("enabling publisher confirms.")
	if err := channel.Confirm(false); err != nil {
		ErrLog.Fatalf("Channel could not be put into confirm mode: %s", err)
	}

	for {
		Log.Printf("publishing %dB body (%q)", len(*body), *body)
		dConfirmation, err := channel.PublishWithDeferredConfirmWithContext(
			ctx,
			*exchange,
			*routingKey,
			false,
			false,
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "text/plain",
				ContentEncoding: "",
				DeliveryMode:    amqp.Persistent,
				Priority:        0,
				AppId:           "sequential-producer",
				Body:            []byte(*body),
			},
		)
		if err != nil {
			ErrLog.Fatalf("error in publish: %s", err)
		}

		dConfirmation.Wait()
		Log.Printf("confirmed delivery with tag: %d", dConfirmation.DeliveryTag)

		if *continuous {
			select {
			case <-done:
				Log.Println("producer is stopping")
				return
			case <-time.After(time.Second):
				continue
			}
		} else {
			break
		}
	}
}

func SetupCloseHandler(done chan struct{}) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		close(done)
		Log.Printf("Ctrl+C pressed in Terminal")
	}()
}
