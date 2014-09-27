package main

import (
	"fmt"
)

func findSecondWord(line []string) string {
	fmt.Println(line)
	for _, sec := range line {
		fmt.Println(sec)
		if sec != "" && sec != "say" {
			fmt.Println("breaking on: ", sec)
			return sec
		}
		fmt.Println("next")
	}
	return ""
}

type Members struct {
	m map[string][]string
}

var Stuff chan Members

func main() {
	/*
		data := "say to bob that i am here to say hi"
		split := strings.SplitN(data, "say", 2)[1]
		fmt.Println(split[1])
		//returns
		// to bob that i am here to say hi

		split2 := strings.Split(data, " ")
		fmt.Println(split2)
		fmt.Println(split2[0])

		data2 := "say    to   bob hello world"
		split3 := strings.Split(data2, " ")
		fmt.Println(split3)
		fmt.Println("len :", len(split3))
		to := findSecondWord(split3)
		fmt.Println("to: ", to)
	*/

	Stuff = make(chan Members)
	elt := <-Stuff
	elt.m["bob"] = make([]string, 5, 5)
	elt.m["bob"][0] = "hello world"
	Stuff <- elt
	fmt.Println(elt.m["bob"][0])

}
