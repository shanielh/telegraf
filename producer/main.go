package main

import (
	"bufio"
	"log"
	"os"

	"github.com/streadway/amqp"
)

func main() {
	// Open file with zabbix data
	f, err := os.Open("zabbix.txt")
	defer f.Close()
	logErr(err, "error opening file")

	// Make a new scanner to feed points into Queue
	scan := bufio.NewScanner(f)

	// Create producer to publish to Queue
	p := newProducer()
	defer p.conn.Close()
	defer p.ch.Close()

	for scan.Scan() {
		body := scan.Text()
		err = p.publish(body)
		logErr(err, "Failed to publish a message")
	}

}

type producer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
}

func (p producer) publish(body string) error {
	err := p.ch.Publish("", p.q.Name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(body),
	})
	return err
}

func newProducer() producer {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	logErr(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	logErr(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	logErr(err, "Failed to declare a queue")
	return producer{
		conn: conn,
		ch:   ch,
		q:    q,
	}
}

func logErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
