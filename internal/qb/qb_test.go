package qb

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBuilder_Appendf(t *testing.T) {
	var b Builder
	b.Appendf("TEXT WITH %s PLACEHOLDER", CQL("some CQL"))
	require.Equal(t, "TEXT WITH some CQL PLACEHOLDER", b.String())
}

func TestString(t *testing.T) {
	assert.Equal(t, CQL(`'Joe''s string'`), String("Joe's string"))
}

func TestQName(t *testing.T) {
	assert.Equal(t, CQL(`"the_""cool""_identifier"`), QName(`the_"cool"_identifier`))
}
