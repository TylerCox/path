package main

import (
	"bufio"
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type Data struct {
	vals map[string]string
}

type StringPair struct {
	Key   string
	Value string
}

type Node struct {
	port                   string
	data                   chan *Data
	self_addr              string
	successor_addr         chan string
	successor_contact_fail int
	predecessor_addr       chan string
	fingers 			   []string
	listening              bool
}

//Helper functions///////////////////////////////////////////
func get_second_string(command string, skip string) (string, string) {
	parts := strings.Split(command, " ")
	ret := ""
	remain := command //Whatever the sentence is after first skip word is removed
	repeated := false
	for key, what := range parts {
		if what != "" && (what != skip || repeated) {
			ret = what
			remain = strings.Join(parts[key:], " ")
			break
		}
		if what == skip{
			repeated = true
		}
	}
	return ret, remain
}

//////////////////////////////////////////////////////////////
//Server only commands////////////////////////////////////////
func hashString(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

const keySize = sha1.Size * 8

var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)

func (elt Node) jump(fingerentry int) *big.Int {
	n := hashString(elt.self_addr)
	two := big.NewInt(2)
	fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
	jump := new(big.Int).Exp(two, fingerentryminus1, nil)
	sum := new(big.Int).Add(n, jump)

	return new(big.Int).Mod(sum, hashMod)
}

func between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
	panic("impossible")
}

func fix_fingers(n *Node){ //Repeating Proccess
	for {
		time.Sleep(time.Second)
		//log.Println("Fixing fingers")
		start := hashString(n.self_addr)
		ad :=<-n.successor_addr
		//log.Println("Assigning first finger")
		n.fingers[1] = ad
		n.successor_addr<-ad
		if n.fingers[1] != ""{
			for i:=2; i<=160;i++{
				hash := n.jump(i)
				if between(start,hash,hashString(n.fingers[i-1]),true){
					//Next finger still between previous address
					//log.Println("Assigning",i,"finger")
					n.fingers[i] = n.fingers[i-1]
				}else{
					next:=Find("",hash,n)
					start = hashString(next)
					//log.Println("Assigning",i,"finger")
					n.fingers[i] = next
				}
			}
		}
	}
}

func set_node_pred(client *rpc.Client, addr_self string) {
	var reply bool
	err := client.Call("Node.Notify_Node", addr_self, &reply)
	if err != nil {
		log.Println("Notify Node Error:", err)
	}
}

func ask_for_pred(addr string) (string, *rpc.Client) {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Println("Stabilize connect error:", err)
	} else {
		var reply string
		err := client.Call("Node.Inform_of_predecessor", false, &reply)
		if err != nil {
			log.Println("Successor-Inform_of_predecessor error:", err)
			return "", client
		} else {
			return reply, client
		}
	}
	return "", nil

}

func confirm_exists(address string) bool{
	for i := 0; i < 3; i++ {
		answered := ping(address)
		if answered {
			return true
		}
	}
	return false
}

func stabilize(node *Node) { //Repeating Proccess
	const max_failures = 3
	stable:=false
	for {
		if stable {
			time.Sleep(time.Second)
		}
		ad := <-node.successor_addr
		if ad != "" && ad != node.self_addr{

			reply, client := ask_for_pred(ad)
			if client == nil {
				node.successor_contact_fail += 1
				if node.successor_contact_fail == max_failures {
					ad = node.self_addr
					stable = true
				}
			} else {
				node.successor_contact_fail = 0
				switch {
				case reply == "" || reply == ad: //Nothing or Node's self.
					//log.Println("Telling node Predecessor is self")
					set_node_pred(client, node.self_addr)
					stable = true
					
				case reply == node.self_addr:
					stable = true

				default:
					bet := between(hashString(node.self_addr), hashString(reply), hashString(ad), false)
					if bet {
						log.Println(reply, "is in between")
						log.Println()
						log.Println("Attepmting to contact said predesessor")
						answered:=confirm_exists(reply)
						if answered {
							log.Println("Contact Sucessful")
							ad = reply
							stable = false //Not sure if new node is actually the successor
							log.Println("About to ask new node for Predecessor")
						} else {
							//No responce from told predesessor
							//Tell node self is preddesossor
							log.Println(reply, "did not respond")
							log.Println("Node's Predesessor is missing")
							set_node_pred(client, node.self_addr)
							log.Println("Set Node's Predecessor to self")
							stable = true
						}
					} else {
						log.Println(reply, "is not in between")
						set_node_pred(client, node.self_addr)
						log.Println("Set Node's Predecessor to self")
						stable = true
					}
				}
				err := client.Close()
				if err != nil {
					log.Println("Closing rpc error:", err)
				}
			}

		}
		node.successor_addr <- ad
	}
}

func FixSuccessor(n *Node) { //Repeating Proccess
	for {
		time.Sleep(time.Second)
		add := <-n.successor_addr
		if add == "" || add == n.self_addr {
			//Should only run when ring is new. Or one left
			addr := <-n.predecessor_addr
			if (addr == "" || addr != add) && (add == "" || add == n.self_addr) {
				log.Println("Updating Successor from empty to Predecessor:", addr)
				answered := confirm_exists(addr)
				if answered{
					add = addr
				}else{
					log.Println("Predecessor does not answer")
					log.Println("No other nodes in ring")
					log.Println("Successor is self")
					addr = n.self_addr
				}
			}
			n.predecessor_addr <- addr
		}
		n.successor_addr <- add
	}
}

func (n Node) Inform_of_predecessor(none bool, reply *string) error {
	ad := <-n.predecessor_addr
	*reply = ad
	n.predecessor_addr <- ad
	return nil
}

func (n Node) Notify_Node(addr string, none *bool) error {
	ad := <-n.predecessor_addr
	log.Println("Updating self Predecessor to:", addr)
	ad = addr
	n.predecessor_addr <- ad
	return nil
}

func Notify(self_addr string, client *rpc.Client) {
	var reply string
	err := client.Call("Node.Notify_Node", self_addr, &reply)
	if err != nil {
		log.Println("Remote Notify Error:", err)
	}
}

func getLocalAddress() string {
	fmt.Println("\n\n\nGetting Local Address\n")
	var localaddress string

	ifaces, err := net.Interfaces()
	if err != nil {
		panic("init: failed to find network interfaces")
	}

	// find the first non-loopback interface with an IP address
	for _, elt := range ifaces {
		log.Println("!Interface!")
		addrs, _ := elt.Addrs()
		log.Println("Index:", elt.Index, "MTU:", elt.MTU, "Name:", elt.Name, "HardwareAddr:", elt.HardwareAddr, "Flags:", elt.Flags, "Addr:", addrs)
		if elt.Flags&net.FlagLoopback == 0 && elt.Flags&net.FlagUp != 0 {

			addrs, err := elt.Addrs()
			log.Println("Acctually looking at address", addrs)
			if err != nil {
				panic("init: failed to get addresses for network interface")
			}

			for _, addr := range addrs {
				log.Println("Auto setting ip to:", addr.String(), "Because if chain doesn't finish.")
				localaddress = addr.String()
				log.Println("Looping over addr list, viewing:", addr)
				ipnet, ok := addr.(*net.IPNet)
				log.Println("Ipnet:", ipnet)
				if ok {
					log.Println("first ok")
					if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
						log.Println("second ok")
						localaddress = ip4.String()
						break
					}
				}
			}
		}
	}
	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	fmt.Println("Finished Getting Local Address\n\n\n")

	return localaddress
}

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
	log.Println("New Key:",pair.Key,"=>",pair.Value)
	n.data <- m
	return nil
}

func put(command string,n *Node) { //Assuming that the string is correct length currently
	skey, remain := get_second_string(command, "put")
	svalue, _ := get_second_string(remain, skey)

	pair := &StringPair{
		Key:   skey,
		Value: svalue,
	}

	address:=Find(skey,nil,n)

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
		log.Println("Put format: put <key> <value>")
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

func get(command string,n *Node) {
	skey, _ := get_second_string(command, "get")

	address:=Find(skey,nil,n)

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
		delete(m.vals, key)
		n.data <- m
		log.Println("Deleted key:",key)
		return nil
	}
	n.data <- m
	return errors.New("Key [" + key + "] does not exist")
}

func delete_val(command string,n *Node) {
	skey, _ := get_second_string(command, "delete")

	address:=Find(skey,nil,n)

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

func(n Node) Give_successor(none bool, addr *string)error{
	ad := <-n.successor_addr
	if ping(ad){
		*addr = ad
		n.successor_addr <- ad
		return nil
	}
	*addr = ""
	n.successor_addr <- ad
	return nil
}

func keyboard_find(command string,n *Node){
	value, _ := get_second_string(command, "find")
	if value != ""{
		log.Println("Looking for value:",value)
		ret := Find(value,nil,n)
		if ret != ""{
			log.Println("Value lives at:",ret)
		}else{
			log.Println("Perhaps try again")
		}
	}else{
		log.Println("Please type in a non-empty value")
	}

}

func Find(value string,alt *big.Int,n *Node)string{ //returns an address
	max_failures := 20
	var hash *big.Int
	if alt == nil{
		hash = hashString(value)
	} else{
		hash = alt
	}
	start := n.self_addr
	succ := <- n.successor_addr
	end := succ
	n.successor_addr <- succ
	for{
		if max_failures < 0{
			log.Println("Too many failures, ending search for:", value)
			return ""
		}
		if between(hashString(start),hash,hashString(end),true){
			return end
		}else{
			client, err := rpc.DialHTTP("tcp", end)
			if err != nil {
				log.Println("Find connect:", err)
				log.Println("Double checking successor_addr")
				succ := <- n.successor_addr
				end = succ
				n.successor_addr <- succ
				max_failures--
			} else {
				var reply string
				err := client.Call("Node.Give_successor",true,&reply)
				if err != nil{
					log.Println("Find/Query of Successor error:",err)
					max_failures--
				}else{
					if reply != ""{
						start = end
						end = reply
					}else{
						max_failures--
					}
				}
			}
		}
	}

}

func (n Node) Ping_respond(empty bool, reply *bool) error {
	*reply = true
	return nil
}

func ping(address string) bool {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println("Ping:", err)
	} else {
		reply := false
		err := client.Call("Node.Ping_respond", true, &reply)
		if err != nil {
			log.Println("Remote Ping Error:", err)
		} else {
			
			errc := client.Close()
			if errc != nil {
				log.Println("Closeing rpc error:", errc)
			}
			return true
		}
	}
	return false
}

func ping_command(command string) {
	address, _ := get_second_string(command, "ping")
	if address != "" {
		reply := ping(address)
		log.Println("Ping Responce:", reply)
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
				node.self_addr = getLocalAddress() + ":" + what
				return
			}
		}
	}
	log.Println("Port remains:", node.port, "(No new port number read)")
}

func connect_to_ring(node *Node, command string) {
	address, _ := get_second_string(command, "join")
	address = strings.ToLower(address)
	if strings.HasPrefix(address,"localhost") || strings.HasPrefix(address,"127.0.0.1"){
		i := strings.Index(address,":")
		address = getLocalAddress()+address[i:]
	}
	_, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println("Ping:", err)
	} else {
		log.Println("Joining Ring at:", address)
		go listen(node)
		ad := <-node.successor_addr
		ad = address
		node.successor_addr <- ad
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

func print_hash(command string){
	value, _ := get_second_string(command, "hash")
	log.Println("Hashing value:",value)
	log.Println("Hash Position:", hashString(value))
}

func dump(node *Node) {
	ads := <-node.successor_addr
	adp := <-node.predecessor_addr
	fmt.Println()
	fmt.Println()
	log.Println("Listening port:", node.port)
	log.Println("predecessor_addr:", adp)
	log.Println("Successor_addr:", ads)
	log.Println("Listening:", node.listening)
	log.Println("Self_addr:", node.self_addr)
	log.Println("Hash Position:", hashString(node.self_addr))
	node.successor_addr <- ads
	node.predecessor_addr <- adp
	log.Println("-Fingers-------------------------------------")
	//log.Println(node.fingers)
	log.Println("-Data---------------------------------------")
	m := <-node.data
	log.Println(m.vals)
	node.data <- m
}

func main() {
	var line string
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)
	addr := getLocalAddress() + ":3410"
	data := &Data{
		vals: make(map[string]string),
	}
	node := &Node{
		port:                   "3410",
		data:                   make(chan *Data, 1),
		self_addr:              addr,
		successor_addr:         make(chan string, 1),
		successor_contact_fail: 0,
		predecessor_addr:       make(chan string, 1),
		fingers:				make([]string,161,161),
		listening:              false,
	}
	log.Println("location of node memory:", &node)
	node.data <- data
	node.successor_addr <- ""
	node.predecessor_addr <- ""
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
				go stabilize(node)
				go FixSuccessor(node) //Ocassionally checks if successor is empty and replaces with an adress it knows is closest.
				//go fix_fingers(node)
			} else {
				log.Println("Already listening on port:", node.port)
			}
		case strings.HasPrefix(line, "join "): //Join
			if node.listening == false {
				connect_to_ring(node, line)
				go stabilize(node)
				go FixSuccessor(node) //Ocassionally checks if successor is empty and replaces with an adress it knows is closest.
				//go fix_fingers(node)
			} else {
				log.Println("Already listening on port:", node.port)
			}
		case strings.HasPrefix(line, "ping "): //Ping
			ping_command(line)
		case strings.HasPrefix(line, "put "): //put
			put(line,node)
		case strings.HasPrefix(line, "get "): //get
			get(line,node)
		case strings.HasPrefix(line, "delete "): //delete
			delete_val(line,node)
		case strings.HasPrefix(line, "find "):
			keyboard_find(line,node)
		case strings.HasPrefix(line, "hash "):
			print_hash(line)
		default:
			fmt.Println("Not a recognized command, might be missing argument, type 'help' for assistance.")

		}

	}
}