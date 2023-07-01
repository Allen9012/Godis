package godis

type Reply interface {
	ToBytes() []byte
}
