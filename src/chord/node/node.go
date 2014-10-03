package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
)

type Data struct {
	vals map[string]string
}

type StringPair struct {
	Key   string
	Value string
}

type Node struct {
	port           string
	data           chan *Data
	successor      *rpc.Client
	successor_addr string
	listening      bool
}

//Helper functions///////////////////////////////////////////
func get_second_string(command string, skip string) (string, string) {
	parts := strings.Split(command, " ")
	ret := ""
	remain := command //Whatever the sentence is after first skip word is removed
	for key, what := range parts {
		if what != "" && what != skip {
			ret = what
			remain = strings.Join(parts[key:], " ")
			break
		}
	}
	return ret, remain
}

//////////////////////////////////////////////////////////////
//Server only commands////////////////////////////////////////

//////////////////////////////////////////////////////////////
//Server-Keyboard commands////////////////////////////////////
func (n Node) Put_reciever(pair StringPair, existed *bool) error {
	m := <-n.data
	key := pair.Key
	if _, ok := m.vals[key]; ok {
		*existed = true
		m.vals[pair.Key] = pair.Value
	} else {
		*existed = false
		m.vals[pair.Key] = pair.Value
	}
	n.data <- m
	return nil
}

func put(command string) { //Assuming that the string is correct length currently
	address, remain := get_second_string(command, "put")
	skey, remain2 := get_second_string(remain, address)
	svalue, _ := get_second_string(remain2, skey)

	pair := &StringPair{
		Key:   skey,
		Value: svalue,
	}

	if address != "" {
		client, err := rpc.DialHTTP("tcp", address)
		if err != nil {
			log.Println("Put connect:", err)
		} else {
			reply := false
			err := client.Call("Node.Put_reciever", pair, &reply)
			if err != nil {
				log.Println("Remote Put Error:", err)
			} else {
				log.Println("Success. Key Existed:", reply)
			}
			errc := client.Close()
			if errc != nil {
				log.Println("Closing rpc error:", errc)
			}
		}
	} else {
		log.Println("Put format: put <#.#.#.#:port#> <key> <value>")
	}
}

func (n Node) Get_respond(key string, reply *string) error {
	m := <-n.data
	if _, ok := m.vals[key]; ok {
		*reply = m.vals[key]
		n.data <- m
		return nil
	}
	n.data <- m
	return errors.New("Key [" + key + "] does not exist")

}

func get(command string) {
	address, remain := get_second_string(command, "get")
	skey, _ := get_second_string(remain, address)

	if address != "" {
		client, err := rpc.DialHTTP("tcp", address)
		if err != nil {
			log.Println("Get connect:", err)
		} else {
			var reply string
			err := client.Call("Node.Get_respond", skey, &reply)
			if err != nil {
				log.Println("Remote Get Error:", err)
			} else {
				//////////////
				//Key is found
				log.Println(skey, "=>", reply)
			}
			errc := client.Close()
			if errc != nil {
				log.Println("Closing rpc error:", errc)
			}
		}
	} else {
		log.Println("Get format: get <#.#.#.#:port#> <key>")
	}
}

func (n Node) Delete_request(key string, reply *bool) error {
	m := <-n.data
	if _, ok := m.vals[key]; ok {
		//log.Println("!!!!",m.vals[key])
		delete(m.vals,key)
		n.data <- m
		return nil
	}
	n.data <- m
	return errors.New("Key [" + key + "] does not exist")
}

func delete(command string) {
	address, remain := get_second_string(command, "delete")
	skey, _ := get_second_string(remain, address)

	if address != "" {
		client, err := rpc.DialHTTP("tcp", address)
		if err != nil {
			log.Println("Delete connect:", err)
		} else {
			var reply bool
			err := client.Call("Node.Delete_request", skey, &reply)
			if err != nil {
				log.Println("Remote Delete Error:", err)
			} else {
				//////////////
				//Key is found
				log.Println("Successfully Removed")
			}
			errc := client.Close()
			if errc != nil {
				log.Println("Closing rpc error:", errc)
			}
		}
	} else {
		log.Println("Delete format: delete <#.#.#.#:port#> <key>")
	}
}

func (n Node) Ping_respond(empty bool, reply *bool) error {
	*reply = true
	return nil
}

func ping(command string) {
	address, _ := get_second_string(command, "ping")
	if address != "" {
		client, err := rpc.DialHTTP("tcp", address)
		if err != nil {
			log.Println("Ping:", err)
		} else {
			reply := false
			err := client.Call("Node.Ping_respond", true, &reply)
			if err != nil {
				log.Println("Remote Ping Error:", err)
			} else {
				log.Println("Ping Responce:", reply)
			}
			errc := client.Close()
			if errc != nil {
				log.Println("Closeing rpc error:", errc)
			}
		}
	} else {
		log.Println("Please enter an address <#.#.#.#:port#>")
	}
}

///////////////////////////////////////////////////////////////
//Keyboard Commands only///////////////////////////////////////
func set_port(node *Node, command string) {
	parts := strings.Split(command, " ")
	for _, what := range parts {
		if what != "" {
			if _, err := strconv.Atoi(what); err == nil {
				node.port = what
				log.Println("Port set to:", node.port)
				return
			}
		}
	}
	log.Println("Port remains:", node.port, "(No new port number read)")
}

func connect_successor(node *Node, command string) {
	address, _ := get_second_string(command, "join")
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println("Ping:", err)
	} else {
		log.Println("Joining Ring at:", address)
		go listen(node)
		node.successor = client
		node.successor_addr = address
	}
}

func listen(node *Node) {
	node.listening = true
	log.Println("Listening on port:", node.port)
	rpc.Register(node)
	rpc.HandleHTTP()
	err := http.ListenAndServe(":"+node.port, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func dump(node *Node) {
	fmt.Println()
	fmt.Println()
	log.Println("Listening port:", node.port, "Successor_addr:", node.successor_addr, "Listening:", node.listening)
	log.Println("-Data---------------------------------------")
	m := <-node.data
	log.Println(m.vals)
	node.data <- m
}

func main() {
	var line string
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)
	data := &Data{
		vals: make(map[string]string),
	}
	node := &Node{
		port:           "3410",
		data:           make(chan *Data, 1),
		successor:      nil,
		successor_addr: "",
		listening:      false,
	}
	node.data <- data
	for scanner.Scan() {
		line = scanner.Text()
		switch {
		case strings.HasPrefix(line, "help"): //Help
			fmt.Println("help menu")
		case strings.HasPrefix(line, "quit"): //Quit
			log.Println("quitting")
			return
		case strings.HasPrefix(line, "dump"): //Dump
			dump(node)
		case strings.HasPrefix(line, "port "): //Port
			if node.listening == false {
				set_port(node, line)
			} else {
				log.Println("Cannot change port after listening has begun.")
			}
		case strings.HasPrefix(line, "create"): //Create
			if node.listening == false {
				log.Println("Creating New Ring")
				go listen(node)
			} else {
				log.Println("Already listening on port:", node.port)
			}
		case strings.HasPrefix(line, "join "): //Join
			if node.listening == false {
				connect_successor(node, line)
			} else {
				log.Println("Already listening on port:", node.port)
			}
		case strings.HasPrefix(line, "ping "): //Ping
			ping(line)
		case strings.HasPrefix(line, "put "): //put
			put(line)
		case strings.HasPrefix(line, "get "): //get
			get(line)
		case strings.HasPrefix(line,"delete "): //delete
			delete(line)

		default:
			fmt.Println("Not a recognized command, might be missing argument, type 'help' for assistance.")

		}

	}
}

/*

fixfingers(){
	elt.FingerTable[1] = elt.Successor
	...

	for elt.NextFinger < keySize &&
	between(elt.Address.GetHash(),elt.jump(),)

}


Notes uint8 and sha1:

package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"os"
	"reflect"
)

func main() {
	data := []byte("This page intentionally left blank.")
	sha := sha1.Sum(data)
	fmt.Printf("% x", sha)
	fmt.Println()
	fmt.Println(reflect.TypeOf(sha))
	fmt.Println(data)
	fmt.Println()

	something := make([]uint8, 20, 20)
	for i := range something {
		something[i] = uint8(255)
	}
	fmt.Printf("Something: % x", something)
	fmt.Println()

	fmt.Println()
	var b bytes.Buffer // A Buffer needs no initialization.
	b.Write(data)
	b.WriteTo(os.Stdout)

}
*/
