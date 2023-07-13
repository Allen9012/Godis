package sortedset

import "testing"

func TestParseScoreBorder(t *testing.T) {
	Border, err := ParseScoreBorder("(3.14")
	if err != nil {
		t.Error(err)
	}
	t.Log(Border)
}
