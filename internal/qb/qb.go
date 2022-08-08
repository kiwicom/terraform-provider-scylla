package qb

import (
	"fmt"
	"strings"
)

// Builder builds CQL statements.
type Builder struct {
	stmt    strings.Builder
	onceMap map[string]struct{}
}

// Appendf appends a snippet of CQL to the query.
// format should only contain %s as placeholders.
func (b *Builder) Appendf(format string, a ...CQL) {
	cqls := make([]any, len(a))
	for i := range a {
		cqls[i] = string(a[i])
	}
	b.stmt.WriteString(fmt.Sprintf(format, cqls...))
}

// Append a CQL snippet to the Builder.
func (b *Builder) Append(a ...CQL) {
	for i := range a {
		b.stmt.WriteString(string(a[i]))
	}
}

// Once per key append text, otherwise append alt.
func (b *Builder) Once(key string, text, alt CQL) {
	if b.onceMap == nil {
		b.onceMap = make(map[string]struct{})
	}
	if _, ok := b.onceMap[key]; ok {
		b.Append(alt)
		return
	}
	b.onceMap[key] = struct{}{}
	b.Append(text)
}

func (b *Builder) String() string {
	return b.stmt.String()
}

type CQL string

// Bool returns CQL bool literal.
func Bool(b bool) CQL {
	if b {
		return "true"
	}
	return "false"
}

// String returns quoted CQL string literal.
func String(s string) CQL {
	var sb strings.Builder
	sb.WriteString("'")
	sb.WriteString(strings.ReplaceAll(s, "'", "''"))
	sb.WriteString("'")
	return CQL(sb.String())
}

// QName returns quoted CQL name literal.
func QName(s string) CQL {
	var sb strings.Builder
	sb.WriteString("\"")
	sb.WriteString(strings.ReplaceAll(s, "\"", "\"\""))
	sb.WriteString("\"")
	return CQL(sb.String())
}
