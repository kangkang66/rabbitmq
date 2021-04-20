package producer

import "github.com/streadway/amqp"

type queueProducer struct {
	*baseProducer
}

func NewQueueProducer(url string) (queue *queueProducer,err error) {
	//创建RabbitMQ实例
	var pder *baseProducer
	pder,err = newBaseProducer(url)
	if err != nil {
		return nil, err
	}
	queue = &queueProducer{pder}
	return
}

//直接模式队列生产
func (q *queueProducer) Publish(message, queueName string) (err error) {
	//1.申请队列，如果队列不存在会自动创建，存在则跳过创建
	_, err = q.channel.QueueDeclare(
		queueName,
		//是否持久化
		false,
		//是否自动删除
		false,
		//是否具有排他性
		false,
		//是否阻塞处理
		false,
		//额外的属性
		nil,
	)
	if err != nil {
		return
	}

	//调用channel 发送消息到队列中
	err = q.channel.Publish(
		"",
		queueName,
		//如果为true，根据自身exchange类型和routekey规则无法找到符合条件的队列会把消息返还给发送者
		false,
		//如果为true，当exchange发送消息到队列后发现队列上没有消费者，则会把消息返还给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
	return
}

