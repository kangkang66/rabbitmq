package producer

import (
	"github.com/streadway/amqp"
)

type Producer interface {

}

type baseProducer struct {
	conn    *amqp.Connection
	//channel的套接字连接
	channel *amqp.Channel
	//连接信息
	url string
}

//断开channel 和 connection
func (b *baseProducer) Destory() {
	b.channel.Close()
	b.conn.Close()
}

func newBaseProducer(url string) (pder *baseProducer,err error) {
	pder = &baseProducer{
		conn:      nil,
		channel:   nil,
		url:     url,
	}
	//获取connection
	pder.conn, err = amqp.Dial(pder.url)
	if err != nil {
		return nil, err
	}
	//获取channel
	pder.channel, err = pder.conn.Channel()
	if err != nil {
		return nil, err
	}
	return
}
