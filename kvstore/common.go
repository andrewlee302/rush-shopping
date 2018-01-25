package kvstore

type GetArgs struct {
	Key string
}

type PutArgs struct {
	Key   string
	Value string
}

type IncrArgs struct {
	Key   string
	Delta int
}

type Reply struct {
	Flag  bool
	Value string
}
