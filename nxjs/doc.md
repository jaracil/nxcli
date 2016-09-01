	func SendResult(res {}, callback)
	func SendError(code int, msg string, data {}, callback)
	field Path
	field Method
	field Params
	field Tags

	func Close(callback)
	func Read(max int, timeout int, callback)
	func Write(msg {}, callback)
	func Id(callback) string

	func Login(user string, pass string, callback)
	func TaskPush(method string, params {}, timeout int, callback)
	func TaskPull(prefix string, timeout int, callback)
	func UserCreate(user string, pass string, callback)
	func UserDelete(user string, callback)
	func UserDelTags(user string, prefix string, tags []string, callback)
	func UserSetPass(user string, pass string, callback)
	func UserSetTags(user string, prefix string, tags map[string]{}, callback)
	func PipeCreate(jopts {}, callback)
	func PipeOpen(id string, callback)
	func ChanPublish(channel string, msg {}, callback)
	func ChanSubscribe(pipe Pipe, channel string, callback)
	func ChanUnsubscribe(pipe Pipe, channel string, callback)
	func Exec(method string, params {}, callback)
	func ExecNoWait(method string, params {}, callback)
	func Cancel(callback)
	func Closed(callback)
	func Ping(timeout int, callback)
