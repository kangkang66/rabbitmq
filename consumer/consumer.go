package consumer

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

type Consumer struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	connNotify    chan *amqp.Error
	channelNotify chan *amqp.Error
	quit          chan struct{}
	addr          string
	exchange      string
	exchangeType  string
	queue         string
	routingKey    string
	consumerTag   string
	autoDelete    bool
	handler       func([]byte) error
}


func NewConsumer(addr, exchange, exchangeType, queue string, autoDelete bool, handler func([]byte) error) *Consumer {
	c := &Consumer{
		addr:        addr,
		exchange:    exchange,
		exchangeType: exchangeType,
		queue:       queue,
		routingKey:  "",
		consumerTag: "consumer",
		autoDelete:  autoDelete,
		handler:     handler,
		quit:        make(chan struct{}),
	}

	return c
}

func (c *Consumer) Start() error {
	if err := c.run(); err != nil {
		return err
	}
	go c.reConnect()

	return nil
}

func (c *Consumer) Stop() {
	close(c.quit)

	if !c.conn.IsClosed() {
		// 关闭 SubMsg message delivery
		if err := c.channel.Cancel(c.consumerTag, true); err != nil {
			log.Println("rabbitmq consumer - channel cancel failed: ", err)
		}

		if err := c.conn.Close(); err != nil {
			log.Println("rabbitmq consumer - connection close failed: ", err)
		}
	}
}


func (c *Consumer) reConnect() {
	for {
		select {
		case err := <-c.connNotify:
			if err != nil {
				log.Println("rabbitmq consumer - connection NotifyClose: ", err)
			}
		case err := <-c.channelNotify:
			if err != nil {
				log.Println("rabbitmq consumer - channel NotifyClose: ", err)
			}
		case <-c.quit:
			return
		}

		// backstop
		if !c.conn.IsClosed() {
			// close message delivery
			if err := c.channel.Cancel(c.consumerTag, true); err != nil {
				log.Println("rabbitmq consumer - channel cancel failed: ", err)
			}

			if err := c.conn.Close(); err != nil {
				log.Println("rabbitmq consumer - channel cancel failed: ", err)
			}
		}

		// IMPORTANT: 必须清空 Notify，否则死连接不会释放
		for err := range c.channelNotify {
			println(err)
		}
		for err := range c.connNotify {
			println(err)
		}

	quit:
		for {
			select {
			case <-c.quit:
				return
			default:
				log.Println("rabbitmq consumer - reconnect")

				if err := c.run(); err != nil {
					log.Println("rabbitmq consumer - failCheck: ", err)

					// sleep 5s reconnect
					time.Sleep(time.Second * 5)
					continue
				}

				break quit
			}
		}
	}
}

func (c *Consumer) run() error {
	var err error
	if c.conn, err = amqp.Dial(c.addr); err != nil {
		return err
	}

	if c.channel, err = c.conn.Channel(); err != nil {
		c.conn.Close()
		return err
	}

	if _, err = c.channel.QueueDeclare(
		c.queue,      // name
		false,        // durable
		c.autoDelete, // delete when usused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	); err != nil {
		c.channel.Close()
		c.conn.Close()
		return err
	}

	if c.exchange != "" {
		err = c.channel.ExchangeDeclare(
			c.exchange,
			//交换机类型
			c.exchangeType,
			true,
			false,
			//YES表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
			false,
			false,
			nil,
		)

		if err = c.channel.QueueBind(
			c.queue,
			c.routingKey,
			c.exchange,
			false,
			nil,
		); err != nil {
			c.channel.Close()
			c.conn.Close()
			return err
		}
	}


	var mgs <-chan amqp.Delivery
	if mgs, err = c.channel.Consume(
		c.queue,       // queue
		c.consumerTag, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	); err != nil {
		c.channel.Close()
		c.conn.Close()
		return err
	}

	go c.handle(mgs)

	c.connNotify = c.conn.NotifyClose(make(chan *amqp.Error))
	c.channelNotify = c.channel.NotifyClose(make(chan *amqp.Error))

	return err
}

func (c *Consumer) handle(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		go func(msg amqp.Delivery) {
			if err := c.handler(msg.Body); err == nil {
				msg.Ack(false)
			} else {
				// 重新入队，否则未确认的消息会持续占用内存
				msg.Reject(true)
			}
		}(msg)
	}
}
