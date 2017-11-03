package pool

import (
	"log"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

const (
	minAddIdlePeriod = 5 * time.Second
	recycleCliPeriod = 15 * time.Second
)

type elem struct {
	ID      int
	Counter int
	Client  lib.RelayerClient
}

type CreateFunction func(*lib.RelayerConfig) lib.RelayerClient

// Pool keep a list of clients' elements
type Pool struct {
	sync.Mutex
	cf            CreateFunction
	config        lib.RelayerConfig
	clients       map[*elem]bool
	free          []*elem
	max           int
	lastCliUpdate time.Time
	clientsID     int
}

// New returns a new pool manager
func New(c *lib.RelayerConfig, cf CreateFunction) (p *Pool) {
	p = &Pool{
		cf: cf,
	}
	p.ReadConfig(c)
	p.clients = make(map[*elem]bool)
	p.free = make([]*elem, 0, p.max)
	return
}

func (p *Pool) ReadConfig(c *lib.RelayerConfig) {
	p.Lock()
	defer p.Unlock()

	p.max = c.MaxConnections
	if p.max <= 0 {
		p.max = 1 // Default number of connections
	}

	i := 0
	for e := range p.clients {
		e.Client.Reload(c)
		if i > p.max {
			lib.Debugf("Removing active client %d", e.ID)
			e.Client.Exit()
			delete(p.clients, e)
		}
		i++
	}

	if len(p.free) > p.max {
		f := p.free[p.max+1:]
		for _, e := range f {
			lib.Debugf("Removing free client %d", e.ID)
			e.Client.Exit()
		}

		p.free = p.free[0:p.max]
	}

	p.config = *c

}

func (p *Pool) Reset() {
	p.Lock()
	defer p.Unlock()

	for e := range p.clients {
		e.Client.Exit()
	}
	p.clients = nil
	p.free = nil
}

func (p *Pool) Get() (e *elem, ok bool) {
	p.Lock()
	defer p.Unlock()

	e, ok = p._get()
	if ok {
		// Check the element is ok
		if !e.Client.IsValid() {
			p._closeClient(e)

			log.Printf("Error in client %d, replacing it", e.ID)
			e = p._createElem(e.ID)
		}
	}
	return
}

func (p *Pool) _get() (e *elem, ok bool) {
	e, ok = p._pickFree()
	if ok {
		//lib.Debugf("Pool: pickFree %d/%d/%d", len(p.clients), len(p.free), p.max)
		return
	}

	if l := len(p.clients); l <= p.max {
		e = p._createElem(l)
		//lib.Debugf("Pool: create %d/%d/%d", len(p.clients), len(p.free), p.max)
		return e, true
	}

	return
}

func (p *Pool) Close(e *elem) {
	p.Lock()
	defer p.Unlock()

	if len(p.clients) > p.max {
		lib.Debugf("Pool: exceeded limit %d/%d/%d", len(p.clients), len(p.free), p.max)
		p._closeClient(e)
		return
	}

	// if _, ok := p.clients[e]; !ok {
	// 	lib.Debugf("Pool: tried to close a non existing client %d/%d/%d", len(p.clients), len(p.free), p.max)
	// 	return
	// }

	if p.lastCliUpdate.Add(recycleCliPeriod).Before(time.Now()) {
		// update last modification mark
		p.lastCliUpdate = time.Now()
		// close connection
		p._closeClient(e)
		// Remove from clients slice
		lib.Debugf("Pool: recycling client counter %d, ID %d, %d/%d/%d", e.Counter, e.ID, len(p.clients), len(p.free), p.max)
		return
	}

	p.free = append(p.free, e)
	delete(p.clients, e)
	//lib.Debugf("Pool: move to free clients %d free %d", len(p.clients), len(p.free))

}

func (p *Pool) _createElem(id int) (e *elem) {
	//cl := p.server.NewClient()
	p.clientsID++
	cl := p.cf(&p.config)
	e = &elem{
		ID:     p.clientsID,
		Client: cl,
	}
	p.clients[e] = true
	p.lastCliUpdate = time.Now()
	lib.Debugf("Pool: created new clients %d/%d/%d", len(p.clients), len(p.free), p.max)
	return
}

func (p *Pool) _pickFree() (*elem, bool) {
	if len(p.free) == 0 {
		return nil, false
	}

	var e *elem
	e, p.free = p.free[0], p.free[1:]

	return e, true
}

func (p *Pool) _closeClient(e *elem) {
	defer delete(p.clients, e)

	if e.Client == nil {
		return
	}

	e.Client.Exit()
}
