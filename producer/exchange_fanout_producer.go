package producer

import "github.com/streadway/amqp"

type exchangeFanoutProducer struct {
	*baseProducer
}

func NewExchangeFanoutProducerProducer(url  string) (efp *exchangeFanoutProducer,err error) {
	var prod *baseProducer
	prod,err = newBaseProducer(url)
	if err != nil {
		return nil, err
	}
	efp = &exchangeFanoutProducer{prod}
	return
}

//订阅模式生产
func (e *exchangeFanoutProducer) PublishPub(message, exchange string) (err error) {
	//1.尝试创建交换机
	err = e.channel.ExchangeDeclare(
		exchange,
		"fanout",
		//是否持久化
		true,
		//是否自动删除
		false,
		//true表示这个exchange不可以被client用来推送消息，仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	//2.发送消息
	err = e.channel.Publish(
		exchange,
		"",
		//如果为true，根据自身exchange类型和routekey规则无法找到符合条件的队列会把消息返还给发送者
		false,
		//如果为true，当exchange发送消息到队列后发现队列上没有消费者，则会把消息返还给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	return
}
