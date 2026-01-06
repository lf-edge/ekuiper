package delimited

import (
	"reflect"
	"testing"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestDynamicHeaderDecode(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "dynamic_header")

	// Test Case 1: Standard interleaved header/data
	input := `a,b
1,2
c,d
3,4`
	expected := []map[string]any{
		{"a": "1", "b": "2"},
		{"c": "3", "d": "4"},
	}

	c, err := NewConverter(map[string]any{
		"delimiter":     ",",
		"dynamicHeader": true,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := c.Decode(ctx, []byte(input))
	if err != nil {
		t.Fatal(err)
	}

	maps, ok := res.([]map[string]any)
	if !ok {
		t.Fatalf("expected []map[string]any, got %T", res)
	}

	if !reflect.DeepEqual(maps, expected) {
		t.Errorf("expected %v, got %v", expected, maps)
	}

	// Test Case 2: Empty data
	// h1\n\nh2\nd2\n
	inputEmpty := "h1\n\nh2\nd2\n"
	expectedEmpty := []map[string]any{
		{}, // Empty map from empty data line
		{"h2": "d2"},
	}
	resEmpty, err := c.Decode(ctx, []byte(inputEmpty))
	if err != nil {
		t.Fatal(err)
	}
	mapsEmpty, _ := resEmpty.([]map[string]any)
	if !reflect.DeepEqual(mapsEmpty, expectedEmpty) {
		t.Errorf("Empty data: expected %v, got %v", expectedEmpty, mapsEmpty)
	}
}

func TestEncodeSliceMapStrict(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "strict_encode_slice")
	data := []map[string]any{
		{"a": "1", "b": "2"},
		{"a": "3", "b": "4"},
	}

	// Case 1: HasHeader = true
	c1, err := NewConverter(map[string]any{
		"delimiter": ",",
		"hasHeader": true,
	})
	if err != nil {
		t.Fatal(err)
	}

	b1, err := c1.Encode(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	s1 := string(b1)

	// Expect Header without binary prefix
	expectedHeader := "a,b"
	// Output: Header\nRec1\nRec2\n
	expected1 := expectedHeader + "\n1,2\n3,4\n"

	if s1 != expected1 {
		t.Errorf("Slice Case 1: expected %q, got %q", expected1, s1)
	}
}
