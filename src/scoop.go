package scoop

import (
   "flag"
   "io/ioutil"
   "bytes"
   "fmt"
   "github.com/ha/doozer"
)

var root = flag.String("r", "/tmp/scoop", "root of backup dir")
var doozerUri = flag.String("d", "localhost:8046", "doozer cluster to watch")


func backup(events chan doozer.Event){
   for{
       e :=  <-events
       buffer := bytes.NewBufferString(*root);
       fmt.Fprint(buffer, e.Path)
       err := ioutil.WriteFile(buffer.String(), e.Body, 0600)
       if( err != nil){
          panic("unable to write backup")
       }
   }    
}

func watch(conn *doozer.Conn, events chan doozer.Event, revs chan int64){
    for{
        rev := <-revs
        event, err := conn.Wait("/*",rev)
        if(err != nil){
            panic("error waiting for event, bailing")
        }
        revs <- event.Rev + 1
        events <- event
    }
}


func main() {
    flag.Parse()
    conn, err := doozer.Dial(*doozerUri)

    if err != nil {
        panic(err)
    }

    events := make(chan doozer.Event, 100)
    revs := make(chan int64, 100)
    curr, err := conn.Rev()

    if err != nil {
        panic(err)
    }

    list, err := conn.Walk("/*", curr, 0, -1)
    if err != nil  {
       panic(err)
    }

    go backup(events)
    
    for i := range list {
      events <- list[i]  
    }

    go watch(conn, events, revs)
    revs <- curr
}
