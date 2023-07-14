package dict

import "testing"

func TestComputeCapacity(t *testing.T) {
	t.Log(computeCapacity(16))
	t.Log(computeCapacity(17))
	t.Log(computeCapacity(18))
	t.Log(computeCapacity(19))
	t.Log(computeCapacity(20))
	t.Log(computeCapacity(21))
}
