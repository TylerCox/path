package main

import (
	"common"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Members struct {
	m map[string][]common.Message
}

type Server struct {
	mem chan *Members
	pow chan bool
}

//type Commands chan *Members

func (s Server) Register(name string, reply *string) error { //don't need to do a pointer to a string
	m := <-s.mem
	if _, ok := m.m[name]; ok {
		s.mem <- m
		return errors.New("The name " + name + " already exists.")
	}
	*reply = "Welcome " + name + "\n\n"
	*reply += "Current members are:\n"
	fmt.Println(name + " Logged On")
	serverMessageAll(m, name+" Logged On")
	for key := range m.m {
		*reply += key + "\n"
	}
	m.m[name] = nil // make([]common.Message,0,0)
	s.mem <- m
	return nil
}

func serverMessageAll(mems *Members, message string) {
	mes := &common.Message{
		To:   "all",
		From: "Server",
		What: message,
	}
	for key, _ := range mems.m {
		insertMessage(mems, mes, key)
	}
}

func insertMessage(mems *Members, mes *common.Message, name string) {
	target := mems.m[name]
	mems.m[name] = append(target, *mes)
	///fmt.Println(len(target.m[name]), cap(Members.m[name]), Members.m[name])
}

func (s Server) Tell(mes *common.Message, reply *string) error {
	m := <-s.mem
	//fmt.Println(m)
	//fmt.Println(m.m[mes.To])
	if _, ok := m.m[mes.To]; ok {
		insertMessage(m, mes, mes.To)
		//fmt.Println(m)
	} else {
		*reply = "Sorry, user " + mes.To + " does not exist."
	}
	s.mem <- m
	return nil
}

func (s Server) Say(mes *common.Message, reply *string) error {
	m := <-s.mem
	fmt.Println(mes.From, ": ", mes.What)
	for key, _ := range m.m {
		if key != mes.From {
			insertMessage(m, mes, key)
		}
	}
	s.mem <- m
	return nil
}

func (s Server) List(none *common.Nothing, reply *string) error {
	m := <-s.mem
	*reply = "Current members are:\n"

	for key := range m.m {
		*reply += key + "\n"
	}
	s.mem <- m
	return nil
}

func (s Server) Quit(name string, reply *string) error {
	m := <-s.mem
	delete(m.m, name)
	fmt.Printf(name + " Logged Off")
	serverMessageAll(m, name+" Logged Off")
	s.mem <- m
	*reply = "\nYou are logged off, TTYL."
	return nil
}

func (s Server) Help(none *common.Nothing, reply *string) error {
	*reply = "Commands for chat:\n"
	*reply += "say <message> (tell everyone)\n"
	*reply += "tell <active user> <message> (whisper to specific user)\n"
	*reply += "list (get list of active users)\n"
	*reply += "quit (log off)\n"
	*reply += "shutdown (well that's a turn-off)\n"
	return nil
}

func (s Server) Shutdown(none common.Nothing, reply *string) error {
	fmt.Printf("Shutting Down")
	a := "\n[~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~]\n"
	a += "Well, it's been fun but I'm\n"
	a += "shutting down within the next\n"
	a += "10 or so seconds. TTFN\n"
	a += "[~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~]\n"

	m := <-s.mem
	serverMessageAll(m, a)
	s.mem <- m

	time.Sleep(10 * time.Second)
	s.pow <- true
	*reply = "Ahh, time to cool down."
	return nil
}

func (s Server) CheckMessages(name *string, reply *[]common.Message) error {
	m := <-s.mem
	//fmt.Printf("hi")
	if _, ok := m.m[*name]; ok {
		*reply = m.m[*name] //just copy directly
		m.m[*name] = nil
	}
	//fmt.Println(reply)

	s.mem <- m
	return nil
}

func Listen(port string) {
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	var port string
	if len(os.Args) == 2 {
		port = os.Args[1]
	} else {
		port = "3411"
	}
	mems := &Members{ //...changed it from &Messages
		m: make(map[string][]common.Message),
	}
	serv := &Server{
		mem: make(chan *Members, 1),
		pow: make(chan bool, 1),
	}
	//data := make(Commands, 1)
	serv.mem <- mems
	rpc.Register(serv)
	rpc.HandleHTTP()

	go Listen(port)

	fmt.Println("Wating for shutdown command")
	off := <-serv.pow
	if off {
		fmt.Println("Goodbye")
		os.Exit(0)
	}
	fmt.Println("Well that is weird")
	os.Exit(0)

}
