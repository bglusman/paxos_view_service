package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type Role int
const (
        Primary Role = iota
        Backup
)
type ClientState struct {
  name string
  TimeSeen time.Time
  ViewNum uint
  Status Role
}

type ViewServer struct {
  mu           sync.Mutex
  l            net.Listener
  dead         int32 // for testing
  rpccount     int32 // for testing
  me           string
  clientStates map[string]ClientState


  // Your declarations here.
  view View      // current view on ViewServer
  hotSpareClient string
}

func (vs *ViewServer) setupClientStates() {
  if vs.clientStates == nil {
    vs.clientStates = make(map[string]ClientState)
  }
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.setupClientStates()
  state := ClientState{name: args.Me, ViewNum: args.Viewnum, TimeSeen: time.Now()}

  _, hasOldState := vs.clientStates[args.Me]
  vs.clientStates[args.Me] = state
  if hasOldState && state.ViewNum == 0 {
    if vs.view.Primary == args.Me {
      vs.mu.Lock()
      fmt.Println("promoting backup:", args.Me)
      vs.view.PromoteBackup(vs.hotSpareClient)
      vs.mu.Unlock()
    }
    // if vs.view.Backup == args.Me {
    //   vs.mu.Lock()
    //   fmt.Println("dumping backup:", args.Me)
    //   vs.view.Backup = ""
    //   vs.mu.Unlock()
    // }
    //client died, remove as primary/backup until ack?
  }
  if vs.view.Viewnum == 0 {
    vs.mu.Lock()
    vs.view = View{Viewnum: 1, Primary: args.Me }
    vs.mu.Unlock()
  } else {
    if vs.view.Backup == "" && vs.view.Primary != args.Me  {
      vs.mu.Lock()
      vs.view.AddBackup(args.Me)
      vs.mu.Unlock()
    } else {
      if vs.view.Primary != args.Me && vs.view.Backup != args.Me {
        vs.hotSpareClient = args.Me
      }
    }
  }
  reply.View = vs.view


   fmt.Println("client states", vs.clientStates)
    // fmt.Println("inited")
  // vs.clientStates[args.Me] = state
  // if vs.view.Viewnum > 0 {
  //  args.Viewnum = vs.view.Viewnum
  //  } else {
  //    vs.view.Primary.Viewnum
  //  }
  // vs.view.Viewnum = args.Viewnum
  fmt.Println("state:", state)
  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  // fmt.Println("reply:", reply)
  reply.View = vs.view
  vs.mu.Unlock()
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  time := time.Now()
  for name, state:= range vs.clientStates{
    // fmt.Println("state:", state)
    fmt.Println("tick name:", name)
    cutoffTime := state.TimeSeen.Add(PingInterval * DeadPings)
    if time.After(cutoffTime)  {
      fmt.Println("after cutoff:", name)
      if vs.view.Primary == name {
        vs.view.PromoteBackup(vs.hotSpareClient)
      }
    }

   }
  // Your code here.


}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
  atomic.StoreInt32(&vs.dead, 1)
  vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
  return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
  return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me)
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.isdead() == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.isdead() == false {
        atomic.AddInt32(&vs.rpccount, 1)
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.isdead() == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.isdead() == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
