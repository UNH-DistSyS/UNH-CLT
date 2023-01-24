package cql_data_model

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestIntValue_Serialize(t *testing.T) {
	i1 := NewInt(42)
	i1Bytes := i1.Serialize()
	assert.Equal(t, 4, len(i1Bytes))
	assert.Equal(t, uint8(0), i1Bytes[0])
	assert.Equal(t, uint8(0), i1Bytes[1])
	assert.Equal(t, uint8(0), i1Bytes[2])
	assert.Equal(t, uint8(42), i1Bytes[3])

	i2 := NewInt(257)
	i2Bytes := i2.Serialize()
	assert.Equal(t, 4, len(i2Bytes))
	assert.Equal(t, uint8(0), i2Bytes[0])
	assert.Equal(t, uint8(0), i2Bytes[1])
	assert.Equal(t, uint8(1), i2Bytes[2])
	assert.Equal(t, uint8(1), i2Bytes[3])

	i3 := NewInt(-2)
	i3Bytes := i3.Serialize()
	assert.Equal(t, 4, len(i3Bytes))

	reader := bytes.NewReader(i3Bytes)
	i3Reconstructed, _ := NewIntFromReader(reader)

	assert.Equal(t, i3.Val, i3Reconstructed.Val)
}

func BenchmarkIntValue_Serialize(b *testing.B) {
	i1 := NewInt(145794)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		i1.Serialize()
	}
}

func BenchmarkNewlocusdbIntFromReader(b *testing.B) {
	i1 := NewInt(145794)
	reader := bytes.NewReader(i1.Serialize())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Seek(0, io.SeekStart)
		NewIntFromReader(reader)
	}
}


func TestBigIntValue_Serialize(t *testing.T) {
	i1 := NewBigInt(42)
	i1Bytes := i1.Serialize()
	assert.Equal(t, 8, len(i1Bytes))
	assert.Equal(t, uint8(0), i1Bytes[0])
	assert.Equal(t, uint8(0), i1Bytes[1])
	assert.Equal(t, uint8(0), i1Bytes[2])
	assert.Equal(t, uint8(0), i1Bytes[3])
	assert.Equal(t, uint8(0), i1Bytes[4])
	assert.Equal(t, uint8(0), i1Bytes[5])
	assert.Equal(t, uint8(0), i1Bytes[6])
	assert.Equal(t, uint8(42), i1Bytes[7])

	i2 := NewBigInt(4294967298)
	i2Bytes := i2.Serialize()

	reader := bytes.NewReader(i2Bytes)
	i2Reconstructed, _ := NewBigIntValueFromReader(reader)
	assert.Equal(t, i2.Val, i2Reconstructed.Val)
	assert.Equal(t, 8, len(i2Bytes))
	assert.Equal(t, uint8(0), i2Bytes[0])
	assert.Equal(t, uint8(0), i2Bytes[1])
	assert.Equal(t, uint8(0), i2Bytes[2])
	assert.Equal(t, uint8(1), i2Bytes[3])
	assert.Equal(t, uint8(0), i2Bytes[4])
	assert.Equal(t, uint8(0), i2Bytes[5])
	assert.Equal(t, uint8(0), i2Bytes[6])
	assert.Equal(t, uint8(2), i2Bytes[7])

	i3 := NewBigInt(-2)
	i3Bytes := i3.Serialize()
	assert.Equal(t, 8, len(i3Bytes))

	reader = bytes.NewReader(i3Bytes)
	i3Reconstructed, _ := NewBigIntValueFromReader(reader)

	assert.Equal(t, i3.Val, i3Reconstructed.Val)
}

func TestTextValue_Serialize(t *testing.T) {
	t1 := NewTextValue("this is a test")
	t1Bytes := t1.Serialize()
	reader := bytes.NewReader(t1Bytes)
	t1Reconstructed, _ := NewTextValueFromReader(reader)

	assert.Equal(t, "this is a test", t1Reconstructed.Val)
}

func TestTextValue_Serialize_NonASCII(t *testing.T) {
	t1 := NewTextValue("это тест")
	t1Bytes := t1.Serialize()
	reader := bytes.NewReader(t1Bytes)
	t1Reconstructed, _ := NewTextValueFromReader(reader)

	assert.Equal(t, "это тест", t1Reconstructed.Val)
}

func BenchmarkTextValue_Serialize(b *testing.B) {
	t1 := NewTextValue("this is a test this is a test this is a test this is a test this is a test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t1.Serialize()
	}
}

func BenchmarkNewlocusdbTextFromReader(b *testing.B) {
	t1 := NewTextValue("hi there!")
	reader := bytes.NewReader(t1.Serialize())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Seek(0, io.SeekStart)
		NewTextValueFromReader(reader)
	}
}

func BenchmarkNewlocusdbFloatFromReader(b *testing.B) {
	f1 := NewFloat(42.42)
	reader := bytes.NewReader(f1.Serialize())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Seek(0, io.SeekStart)
		NewFloatValueFromReader(reader)
	}
}