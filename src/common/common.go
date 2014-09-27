package common

type Message struct {
	To   string
	From string
	What string
}

type MessageBag struct {
	pack []Message
}

type Nothing struct{}
