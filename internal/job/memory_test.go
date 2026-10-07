package job

import "testing"

func TestMemoryStore(t *testing.T) {
	testStoreContract(t, func(t *testing.T) Store { return NewMemoryStore() })
}
