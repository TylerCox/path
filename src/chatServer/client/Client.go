

package main

import (
	"bufio"
	"common"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

func shutdown(client *rpc.Client) {
	var resp string
	var none common.Nothing
	err := client.Call("Server.Shutdown", none, &resp)
	if err != nil {
		log.Fatal("connecting: ", err)
	}
	fmt.Println(resp)
}

func quit(client *rpc.Client, name string) {
	var resp string
	err := client.Call("Server.Quit", name, &resp)
	if err != nil {
		log.Fatal("connecting: ", err)
	}
	fmt.Println(resp)
}

func help(client *rpc.Client) {
	var list string
	var none common.Nothing
	err := client.Call("Server.Help", none, &list)
	if err != nil {
		log.Fatal("connecting: ", err)
	}
	fmt.Println(list)
}

func list(client *rpc.Client) {
	var list string
	var none common.Nothing
	err := client.Call("Server.List", none, &list)
	if err != nil {
		log.Fatal("connecting: ", err)
	}
	fmt.Println(list)
}

func say(client *rpc.Client, line string, user string) {
	mes := new(common.Message)
	mes.What = strings.SplitN(line, "say ", 2)[1]
	mes.From = user
	mes.To = "all"

	var reply string
	err := client.Call("Server.Say", &mes, &reply)
	if err != nil {
		log.Fatal("sending: ", err)
	}
	fmt.Println(reply)
}

func tell(client *rpc.Client, line string, user string) {
	mes := new(common.Message)
	parts := strings.Split(line, " ")
	//Find who message goes to.
	to := ""
	for _, second := range parts {
		if second != "" && second != "tell" {
			to = second
			break
		}
	}
	mes.To = to

	//Find content of message, and remove leading spaces.
	mes.What = strings.SplitN(line, to, 2)[1]
	mes.What = strings.Trim(mes.What, " ")
	mes.From = user
	var reply string

	err := client.Call("Server.Tell", &mes, &reply)
	if err != nil {
		log.Fatal("sending: ", err)
	}
	fmt.Println(reply)
}

func CheckMessages(client *rpc.Client, user string, serverAddress string) {
	for {
		var reply []common.Message
		err := client.Call("Server.CheckMessages", user, &reply)
		//fmt.Println(len(reply), reply)
		if err != nil {
			log.Fatal("Error recieving: ", err)
		}
		//Read off what was sent from reply
		if len(reply) > 0 {
			for _, val := range reply {
				if val.To == user {
					fmt.Println(val.From, " told you: ", val.What)
				} else if val.To == "all" {
					fmt.Println(val.From, " said: ", val.What)
				}
			}

		}
		time.Sleep(time.Second)
	}

}

func main() {
	var port string
	if len(os.Args) < 3 && len(os.Args) > 4 {
		fmt.Println("Usage: Client.exe <user> <server> <port>(Optional)")
		os.Exit(1)
	}
	if len(os.Args) == 4 {
		port = os.Args[3]
	} else {
		port = "3411"
	}
	serverAddress := os.Args[2]

	client, err := rpc.DialHTTP("tcp", serverAddress+":"+port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Synchronous call
	var reply string
	user := os.Args[1]
	err = client.Call("Server.Register", os.Args[1], &reply)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(reply)

	var line string
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines) //prepair to read command

	go CheckMessages(client, user, serverAddress)
	// Loop
	for scanner.Scan() {
		line = scanner.Text()
		if strings.HasPrefix(line, "say ") {
			say(client, line, user)
		} else if strings.HasPrefix(line, "tell ") {
			tell(client, line, user)
		} else if strings.HasPrefix(line, "list") {
			list(client)
		} else if strings.HasPrefix(line, "help") {
			help(client)
		} else if strings.HasPrefix(line, "quit") {
			quit(client, user)
			return
		} else if strings.HasPrefix(line, "shutdown") {
			shutdown(client)
		} else {
			fmt.Println("No command recognized(type help)")
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input: ", err)
	}

}
