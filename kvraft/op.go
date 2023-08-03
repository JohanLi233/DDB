package kvraft

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key     string
	Value   string
	Type    string
	ClerkId int64
	OpId    int
}
