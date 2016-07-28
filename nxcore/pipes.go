package nxcore

import (
	"time"

	"github.com/jaracil/ei"
	"golang.org/x/net/context"
)

// PipeOpen Creates a new pipe from pipe identification string.
// Returns the new pipe object or error.
func (nc *NexusConn) PipeOpen(pipeId string) (*Pipe, error) {
	pipe := &Pipe{
		nc:     nc,
		pipeId: pipeId,
	}
	pipe.context, pipe.cancelFun = context.WithCancel(nc.context)
	return pipe, nil
}

// PipeCreate creates a new pipe.
// Returns the new pipe object or error.
func (nc *NexusConn) PipeCreate(opts ...*PipeOpts) (*Pipe, error) {
	par := ei.M{}
	if len(opts) > 0 {
		if opts[0].Length > 0 {
			par["len"] = opts[0].Length
		}
	}
	res, err := nc.Exec("pipe.create", par)
	if err != nil {
		return nil, err
	}
	return nc.PipeOpen(ei.N(res).M("pipeid").StringZ())
}

// Close closes pipe.
// Returns the response object from Nexus or error.
func (p *Pipe) Close() (interface{}, error) {
	p.cancelFun()
	par := map[string]interface{}{
		"pipeid": p.pipeId,
	}
	return p.nc.Exec("pipe.close", par)
}

// Write writes message to pipe.
// Returns the response object from Nexus or error.
func (p *Pipe) Write(msg interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"pipeid": p.pipeId,
		"msg":    msg,
	}
	return p.nc.Exec("pipe.write", par)
}

// Read reads up to (max) messages from pipe or until timeout occurs.
func (p *Pipe) Read(max int, timeout time.Duration) (*PipeData, error) {
	par := map[string]interface{}{
		"pipeid":  p.pipeId,
		"max":     max,
		"timeout": float64(timeout) / float64(time.Second),
	}
	res, err := p.nc.Exec("pipe.read", par)
	if err != nil {
		return nil, err
	}

	msgres := make([]*Msg, 0, 10)
	waiting := ei.N(res).M("waiting").IntZ()
	drops := ei.N(res).M("drops").IntZ()
	messages, ok := ei.N(res).M("msgs").RawZ().([]interface{})
	if !ok {
		return nil, NewJsonRpcErr(ErrInternal, "", nil)
	}

	for _, msg := range messages {
		m := &Msg{
			Count: ei.N(msg).M("count").Int64Z(),
			Msg:   ei.N(msg).M("msg").RawZ(),
		}

		msgres = append(msgres, m)
	}

	return &PipeData{Msgs: msgres, Waiting: waiting, Drops: drops}, nil
}

// TopicRead reads up to (max) topic messages from pipe or until timeout occurs.
func (p *Pipe) TopicRead(max int, timeout time.Duration) (*TopicData, error) {
	par := map[string]interface{}{
		"pipeid":  p.pipeId,
		"max":     max,
		"timeout": float64(timeout) / float64(time.Second),
	}
	res, err := p.nc.Exec("pipe.read", par)
	if err != nil {
		return nil, err
	}

	msgres := make([]*TopicMsg, 0, 10)
	waiting := ei.N(res).M("waiting").IntZ()
	drops := ei.N(res).M("drops").IntZ()
	messages, ok := ei.N(res).M("msgs").RawZ().([]interface{})
	if !ok {
		return nil, NewJsonRpcErr(ErrInternal, "", nil)
	}

	for _, msg := range messages {
		msgd, err := ei.N(msg).M("msg").MapStr()
		if err != nil {
			return nil, NewJsonRpcErr(ErrInternal, "", nil)
		}
		topic, err := ei.N(msgd).M("topic").String()
		if err != nil {
			return nil, NewJsonRpcErr(ErrInternal, "", nil)
		}
		msgmsg, err := ei.N(msgd).M("msg").Raw()
		if err != nil {
			return nil, NewJsonRpcErr(ErrInternal, "", nil)
		}
		m := &TopicMsg{
			Topic: topic,
			Count: ei.N(msg).M("count").Int64Z(),
			Msg:   msgmsg,
		}

		msgres = append(msgres, m)
	}

	return &TopicData{Msgs: msgres, Waiting: waiting, Drops: drops}, nil
}

// Listen returns a pipe reader channel.
// ch is the channel used and returned by Listen, if ch is nil Listen creates a new unbuffered channel.
// channel is closed when pipe is closed or error happens.
func (p *Pipe) Listen(ch chan *Msg) chan *Msg {
	if ch == nil {
		ch = make(chan *Msg)
	}
	go func() {
		for {
			data, err := p.Read(100000, 0)
			if err != nil {
				close(ch)
				return
			}
			for _, msg := range data.Msgs {
				select {
				case ch <- msg:
				case <-p.context.Done():
					close(ch)
					return
				}
			}
		}
	}()
	return ch
}

// TopicListen returns a pipe topic reader channel.
// ch is the channel used and returned by Listen, if ch is nil Listen creates a new unbuffered channel.
// channel is closed when pipe is closed or error happens.
func (p *Pipe) TopicListen(ch chan *TopicMsg) chan *TopicMsg {
	if ch == nil {
		ch = make(chan *TopicMsg)
	}
	go func() {
		for {
			data, err := p.TopicRead(100000, 0)
			if err != nil {
				close(ch)
				return
			}
			for _, msg := range data.Msgs {
				select {
				case ch <- msg:
				case <-p.context.Done():
					close(ch)
					return
				}
			}
		}
	}()
	return ch
}

// Id returns the pipe identification strring.
func (p *Pipe) Id() string {
	return p.pipeId
}
