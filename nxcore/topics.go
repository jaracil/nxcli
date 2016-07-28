package nxcore

import "github.com/jaracil/ei"

// TopicSubscribe subscribes a pipe to a topic.
// Returns the response object from Nexus or error.
func (nc *NexusConn) TopicSubscribe(pipe *Pipe, topic string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"topic":  topic,
	}
	return nc.Exec("topic.sub", par)
}

// TopicUnsubscribe unsubscribes a pipe from a topic.
// Returns the response object from Nexus or error.
func (nc *NexusConn) TopicUnsubscribe(pipe *Pipe, topic string) (interface{}, error) {
	par := ei.M{
		"pipeid": pipe.Id(),
		"topic":  topic,
	}
	return nc.Exec("topic.unsub", par)
}

// TopicPublish publishes message to a topic.
// Returns the response object from Nexus or error.
func (nc *NexusConn) TopicPublish(topic string, msg interface{}) (interface{}, error) {
	par := ei.M{
		"topic": topic,
		"msg":   msg,
	}
	return nc.Exec("topic.pub", par)
}
