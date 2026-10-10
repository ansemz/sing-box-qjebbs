package interrupt

import (
	"io"
	"net"
	"sync"

	"github.com/sagernet/sing/common/x/list"
)

type Group struct {
	access      sync.Mutex
	connections list.List[*groupConnItem]
}

type groupConnItem struct {
	conn       io.Closer
	isExternal bool

	outboundTag string
}

func NewGroup() *Group {
	return &Group{}
}

func (g *Group) NewConn(conn net.Conn, isExternal bool, outboundTag string) net.Conn {
	g.access.Lock()
	defer g.access.Unlock()
	item := g.connections.PushBack(&groupConnItem{conn, isExternal, outboundTag})
	return &Conn{Conn: conn, group: g, element: item}
}

func (g *Group) NewPacketConn(conn net.PacketConn, isExternal bool, outboundTag string) net.PacketConn {
	g.access.Lock()
	defer g.access.Unlock()
	item := g.connections.PushBack(&groupConnItem{conn, isExternal, outboundTag})
	return newPacketConn(g, conn, item)
}

func (g *Group) Interrupt(interruptExternalConnections bool, currentOutboundTags []string) {
	g.access.Lock()
	var closers []io.Closer
	for element := g.connections.Front(); element != nil; {
		nextElement := element.Next()
		if !g.outboundOutdated(element.Value.outboundTag, currentOutboundTags) {
			element = nextElement
			continue
		}
		if !element.Value.isExternal || interruptExternalConnections {
			closers = append(closers, element.Value.conn)
			g.connections.Remove(element)
		}
		element = nextElement
	}
	g.access.Unlock()
	for _, closer := range closers {
		closer.Close()
	}
}

func (g *Group) outboundOutdated(outboundTag string, currentOutboundTags []string) bool {
	for _, tag := range currentOutboundTags {
		if tag == outboundTag {
			return false
		}
	}
	return true
}
