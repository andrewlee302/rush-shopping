package kvstore

type GetArgs struct {
	Key string
}

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

type Reply struct {
	Flag  bool
	Value string
}
