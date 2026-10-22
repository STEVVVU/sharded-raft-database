package main

import (
	"encoding/json"
	"flag"
	"github.com/otoolep/hraftd/sharddb"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	path := flag.String("config", "", "local fixed cluster configuration")
	id := flag.String("node", "", "node identifier")
	flag.Parse()
	b, e := os.ReadFile(*path)
	if e != nil {
		log.Fatal(e)
	}
	var c sharddb.Config
	if e = json.Unmarshal(b, &c); e != nil {
		log.Fatal(e)
	}
	n, e := sharddb.Open(c, *id)
	if e != nil {
		log.Fatal(e)
	}
	defer n.Close()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
}
