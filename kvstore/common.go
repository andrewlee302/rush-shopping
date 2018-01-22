package kvstore

type GetArgs struct {
	Key string
}

type DelArgs GetArgs

type PutArgs struct {
	Key   string
	Value string
}

type IncrArgs struct {
	Key  string
	Diff int
}

type CompareAndSetArgs struct {
	Key       string
	Base      int
	SetValue  int
	CompareOp func(int, int) bool
}

type CompareAndIncrArgs struct {
	Key       string
	Base      int
	Diff      int
	CompareOp func(int, int) bool
}

type SAddArgs struct {
	Key    string
	Member string
}

type SIsMemberArgs SAddArgs

type SDelArgs GetArgs

type HSetArgs struct {
	Key   string
	Field string
	Value string
}

type HGetArgs struct {
	Key   string
	Field string
}

type HGetAllArgs GetArgs

type HDelAllArgs GetArgs

type HDelArgs HGetArgs

type HIncrArgs struct {
	Key   string
	Field string
	Diff  int
}

type MapReply struct {
	Flag  bool
	Value map[string]string
}

type NoneStruct struct {
	A int
}

type MapReplyBinary struct {
	Flag  bool
	Value []byte // hashmap binary encoded by gob
}

type Reply struct {
	Flag  bool
	Value string
}
