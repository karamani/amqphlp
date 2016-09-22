package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

type amqpProcessor struct {
	CnnString     string
	Exchange      string
	Queue         string
	PrefetchCount int
}

func newAmqpProcessor(cnn, exchange, queue string, prefetchCount int) *amqpProcessor {
	return &amqpProcessor{CnnString: cnn, Exchange: exchange, Queue: queue, PrefetchCount: prefetchCount}
}

func (pc *amqpProcessor) Process(h func([]byte) error) error {

	conn, err := amqp.Dial(pc.CnnString)
	fatalOnError(err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	fatalOnError(err, "failed to open a channel")
	defer ch.Close()

	durable, noAutodelete, noExclusive, noWait := true, false, false, false
	err = ch.ExchangeDeclare(
		pc.Exchange,
		"fanout",
		durable,
		noAutodelete,
		noExclusive,
		noWait,
		nil,
	)
	fatalOnError(err, "failed to declare an exchange")

	err = ch.Qos(pc.PrefetchCount, 0, false)
	fatalOnError(err, "qos error")

	deleteWhenUnused := false
	q, err := ch.QueueDeclare(
		pc.Queue,
		true,
		deleteWhenUnused,
		false,
		false,
		nil,
	)
	fatalOnError(err, "failed to declare a queue")

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeArg,
		false,
		nil,
	)
	fatalOnError(err, "failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	fatalOnError(err, "failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			go func(d amqp.Delivery) {
				defer func() {
					// подтверждаем сам факт обработки без привязки к результату
					d.Ack(false)
				}()

				// в нашем случае функция логирует и гасит ошибку
				// нет смысла в её дополнительной обработке
				h(d.Body)
			}(d)
		}

		forever <- true
	}()

	select {
	case err = <-ch.NotifyClose(make(chan *amqp.Error)):
		log.Fatalln("NotifyClose: " + err.Error())
	case <-forever:
		log.Infoln("forever = true")
		err = nil
	}

	return err
}
