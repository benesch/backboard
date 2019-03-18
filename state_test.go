package main

import (
	"reflect"
	"testing"
)

func TestSortBranches(t *testing.T) {
	x := []string{"release-2.1", "release-1.2", "release-19.1"}
	sortBranches(x)
	exp := []string{"release-19.1", "release-2.1", "release-1.2"}
	if !reflect.DeepEqual(x, exp) {
		t.Errorf("expected %v; got %v", exp, x)
	}
}
