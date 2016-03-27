// PATH=$PATH:$GOPATH/bin
// env GOPATH=$HOME/Projects/golang/goetl go install && ./bin/main
package main

import (
	"fmt"
	"log"

	"github.com/mattmc3/gofurther/stringsx"
)

func checkErr(err error, message string) {
	if err != nil {
		log.Fatal(message, err)
	}
}

func main() {
	fmt.Println("Hello!")
	fmt.Println(stringsx.Snip("arts", 1, -1))
}
