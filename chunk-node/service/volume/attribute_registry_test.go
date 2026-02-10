package volume

import (
	"sort"
	"sync"
	"testing"

	"github.com/amari/mithril/chunk-node/domain"
	portvolume "github.com/amari/mithril/chunk-node/port/volume"
	"go.opentelemetry.io/otel/attribute"
)

// Verify VolumeAttributeRegistry implements the interface
var _ portvolume.VolumeAttributeRegistry = (*VolumeAttributeRegistry)(nil)

func TestNewVolumeAttributeRegistry(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	if r == nil {
		t.Fatal("NewVolumeAttributeRegistry() returned nil")
	}

	// Should have no attributes for any volume
	attrs := r.GetAttributes(domain.VolumeID(1))
	if attrs != nil {
		t.Errorf("new registry should have no attributes, got %v", attrs)
	}
}

func TestVolumeAttributeRegistry_SetAttributes(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	attrs := []attribute.KeyValue{
		attribute.String("path", "/data/volume1"),
		attribute.Int64("capacity", 1000000),
	}

	r.SetAttributes(volumeID, attrs)

	result := r.GetAttributes(volumeID)
	if len(result) != 2 {
		t.Fatalf("expected 2 attributes, got %d", len(result))
	}
}

func TestVolumeAttributeRegistry_SetAttributes_Replaces(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	// Set initial attributes
	r.SetAttributes(volumeID, []attribute.KeyValue{
		attribute.String("key1", "value1"),
		attribute.String("key2", "value2"),
	})

	// Replace with new attributes
	r.SetAttributes(volumeID, []attribute.KeyValue{
		attribute.String("key3", "value3"),
	})

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Fatalf("expected 1 attribute after replace, got %d", len(result))
	}

	if string(result[0].Key) != "key3" {
		t.Errorf("expected key3, got %s", result[0].Key)
	}
}

func TestVolumeAttributeRegistry_SetAttributes_EmptySlice(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	// Set initial attributes
	r.SetAttributes(volumeID, []attribute.KeyValue{
		attribute.String("key1", "value1"),
	})

	// Set empty slice should remove all attributes
	r.SetAttributes(volumeID, []attribute.KeyValue{})

	result := r.GetAttributes(volumeID)
	if result != nil {
		t.Errorf("expected nil after setting empty slice, got %v", result)
	}
}

func TestVolumeAttributeRegistry_SetAttributes_NilSlice(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	// Set initial attributes
	r.SetAttributes(volumeID, []attribute.KeyValue{
		attribute.String("key1", "value1"),
	})

	// Set nil should remove all attributes
	r.SetAttributes(volumeID, nil)

	result := r.GetAttributes(volumeID)
	if result != nil {
		t.Errorf("expected nil after setting nil slice, got %v", result)
	}
}

func TestVolumeAttributeRegistry_AddAttributes(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID, attribute.String("key1", "value1"))

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(result))
	}

	if string(result[0].Key) != "key1" {
		t.Errorf("expected key1, got %s", result[0].Key)
	}
}

func TestVolumeAttributeRegistry_AddAttributes_Multiple(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID,
		attribute.String("key1", "value1"),
		attribute.String("key2", "value2"),
		attribute.Int64("key3", 42),
	)

	result := r.GetAttributes(volumeID)
	if len(result) != 3 {
		t.Fatalf("expected 3 attributes, got %d", len(result))
	}
}

func TestVolumeAttributeRegistry_AddAttributes_Incremental(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID, attribute.String("key1", "value1"))
	r.AddAttributes(volumeID, attribute.String("key2", "value2"))

	result := r.GetAttributes(volumeID)
	if len(result) != 2 {
		t.Fatalf("expected 2 attributes after incremental adds, got %d", len(result))
	}
}

func TestVolumeAttributeRegistry_AddAttributes_Overwrites(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID, attribute.String("key1", "original"))
	r.AddAttributes(volumeID, attribute.String("key1", "updated"))

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(result))
	}

	if result[0].Value.AsString() != "updated" {
		t.Errorf("expected 'updated', got %s", result[0].Value.AsString())
	}
}

func TestVolumeAttributeRegistry_AddAttributes_Empty(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	// Should be a no-op
	r.AddAttributes(volumeID)

	result := r.GetAttributes(volumeID)
	if result != nil {
		t.Errorf("expected nil after adding no attributes, got %v", result)
	}
}

func TestVolumeAttributeRegistry_RemoveAttributes(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID,
		attribute.String("key1", "value1"),
		attribute.String("key2", "value2"),
		attribute.String("key3", "value3"),
	)

	r.RemoveAttributes(volumeID, "key2")

	result := r.GetAttributes(volumeID)
	if len(result) != 2 {
		t.Fatalf("expected 2 attributes after removal, got %d", len(result))
	}

	// Verify key2 is gone
	for _, attr := range result {
		if string(attr.Key) == "key2" {
			t.Error("key2 should have been removed")
		}
	}
}

func TestVolumeAttributeRegistry_RemoveAttributes_Multiple(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID,
		attribute.String("key1", "value1"),
		attribute.String("key2", "value2"),
		attribute.String("key3", "value3"),
	)

	r.RemoveAttributes(volumeID, "key1", "key3")

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(result))
	}

	if string(result[0].Key) != "key2" {
		t.Errorf("expected key2 to remain, got %s", result[0].Key)
	}
}

func TestVolumeAttributeRegistry_RemoveAttributes_NonExistent(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID, attribute.String("key1", "value1"))

	// Should not panic or error
	r.RemoveAttributes(volumeID, "nonexistent")

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(result))
	}
}

func TestVolumeAttributeRegistry_RemoveAttributes_NonExistentVolume(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	// Should not panic
	r.RemoveAttributes(domain.VolumeID(999), "key1")
}

func TestVolumeAttributeRegistry_RemoveAttributes_Empty(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID, attribute.String("key1", "value1"))

	// Should be a no-op
	r.RemoveAttributes(volumeID)

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Errorf("expected 1 attribute after empty removal, got %d", len(result))
	}
}

func TestVolumeAttributeRegistry_RemoveAttributes_CleansUpEmptyMap(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID, attribute.String("key1", "value1"))
	r.RemoveAttributes(volumeID, "key1")

	// Should return nil, not empty slice
	result := r.GetAttributes(volumeID)
	if result != nil {
		t.Errorf("expected nil when all attributes removed, got %v", result)
	}
}

func TestVolumeAttributeRegistry_RemoveAllAttributes(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID,
		attribute.String("key1", "value1"),
		attribute.String("key2", "value2"),
	)

	r.RemoveAllAttributes(volumeID)

	result := r.GetAttributes(volumeID)
	if result != nil {
		t.Errorf("expected nil after RemoveAllAttributes, got %v", result)
	}
}

func TestVolumeAttributeRegistry_RemoveAllAttributes_NonExistent(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	// Should not panic
	r.RemoveAllAttributes(domain.VolumeID(999))
}

func TestVolumeAttributeRegistry_GetAttributes_NonExistent(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	result := r.GetAttributes(domain.VolumeID(999))
	if result != nil {
		t.Errorf("expected nil for non-existent volume, got %v", result)
	}
}

func TestVolumeAttributeRegistry_MultipleVolumes(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	r.AddAttributes(domain.VolumeID(1), attribute.String("vol", "1"))
	r.AddAttributes(domain.VolumeID(2), attribute.String("vol", "2"))
	r.AddAttributes(domain.VolumeID(3), attribute.String("vol", "3"))

	// Each volume should have its own attributes
	result1 := r.GetAttributes(domain.VolumeID(1))
	result2 := r.GetAttributes(domain.VolumeID(2))
	result3 := r.GetAttributes(domain.VolumeID(3))

	if len(result1) != 1 || result1[0].Value.AsString() != "1" {
		t.Errorf("volume 1 attributes incorrect: %v", result1)
	}
	if len(result2) != 1 || result2[0].Value.AsString() != "2" {
		t.Errorf("volume 2 attributes incorrect: %v", result2)
	}
	if len(result3) != 1 || result3[0].Value.AsString() != "3" {
		t.Errorf("volume 3 attributes incorrect: %v", result3)
	}
}

func TestVolumeAttributeRegistry_MultipleVolumes_Independent(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	r.AddAttributes(domain.VolumeID(1), attribute.String("key", "value1"))
	r.AddAttributes(domain.VolumeID(2), attribute.String("key", "value2"))

	// Remove from one should not affect the other
	r.RemoveAllAttributes(domain.VolumeID(1))

	result1 := r.GetAttributes(domain.VolumeID(1))
	result2 := r.GetAttributes(domain.VolumeID(2))

	if result1 != nil {
		t.Errorf("volume 1 should have no attributes, got %v", result1)
	}
	if len(result2) != 1 {
		t.Errorf("volume 2 should still have 1 attribute, got %d", len(result2))
	}
}

func TestVolumeAttributeRegistry_ZeroVolumeID(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(0)

	r.AddAttributes(volumeID, attribute.String("key", "value"))

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Errorf("expected 1 attribute for zero VolumeID, got %d", len(result))
	}
}

func TestVolumeAttributeRegistry_MaxVolumeID(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(65535) // max uint16

	r.AddAttributes(volumeID, attribute.String("key", "value"))

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Errorf("expected 1 attribute for max VolumeID, got %d", len(result))
	}
}

func TestVolumeAttributeRegistry_AttributeTypes(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID,
		attribute.String("string_key", "string_value"),
		attribute.Int64("int64_key", 42),
		attribute.Float64("float64_key", 3.14),
		attribute.Bool("bool_key", true),
	)

	result := r.GetAttributes(volumeID)
	if len(result) != 4 {
		t.Fatalf("expected 4 attributes, got %d", len(result))
	}

	// Create map for easier verification
	attrMap := make(map[string]attribute.KeyValue)
	for _, attr := range result {
		attrMap[string(attr.Key)] = attr
	}

	if attrMap["string_key"].Value.AsString() != "string_value" {
		t.Errorf("string attribute mismatch")
	}
	if attrMap["int64_key"].Value.AsInt64() != 42 {
		t.Errorf("int64 attribute mismatch")
	}
	if attrMap["float64_key"].Value.AsFloat64() != 3.14 {
		t.Errorf("float64 attribute mismatch")
	}
	if attrMap["bool_key"].Value.AsBool() != true {
		t.Errorf("bool attribute mismatch")
	}
}

func TestVolumeAttributeRegistry_Concurrent_AddAndGet(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			volumeID := domain.VolumeID(id % 10)
			r.AddAttributes(volumeID, attribute.Int64("key", int64(id)))
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			volumeID := domain.VolumeID(id % 10)
			_ = r.GetAttributes(volumeID)
		}(i)
	}

	wg.Wait()
	// Test passes if no race condition
}

func TestVolumeAttributeRegistry_Concurrent_SetAndGet(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	var wg sync.WaitGroup

	// Concurrent sets
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			volumeID := domain.VolumeID(id % 10)
			r.SetAttributes(volumeID, []attribute.KeyValue{
				attribute.Int64("key", int64(id)),
			})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			volumeID := domain.VolumeID(id % 10)
			_ = r.GetAttributes(volumeID)
		}(i)
	}

	wg.Wait()
}

func TestVolumeAttributeRegistry_Concurrent_RemoveAndGet(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	// Pre-populate
	for i := 0; i < 10; i++ {
		r.AddAttributes(domain.VolumeID(i),
			attribute.String("key1", "value1"),
			attribute.String("key2", "value2"),
		)
	}

	var wg sync.WaitGroup

	// Concurrent removes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			volumeID := domain.VolumeID(id % 10)
			r.RemoveAttributes(volumeID, "key1")
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			volumeID := domain.VolumeID(id % 10)
			_ = r.GetAttributes(volumeID)
		}(i)
	}

	wg.Wait()
}

func TestVolumeAttributeRegistry_Concurrent_RemoveAllAndAdd(t *testing.T) {
	r := NewVolumeAttributeRegistry()

	// Pre-populate
	for i := 0; i < 10; i++ {
		r.AddAttributes(domain.VolumeID(i), attribute.String("key", "value"))
	}

	var wg sync.WaitGroup

	// Concurrent remove all
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.RemoveAllAttributes(domain.VolumeID(id % 10))
		}(i)
	}

	// Concurrent adds
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r.AddAttributes(domain.VolumeID(id%10), attribute.Int64("new", int64(id)))
		}(i)
	}

	wg.Wait()
}

func TestVolumeAttributeRegistry_GetAttributes_ReturnsNewSlice(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	r.AddAttributes(volumeID,
		attribute.String("key1", "value1"),
		attribute.String("key2", "value2"),
	)

	result1 := r.GetAttributes(volumeID)
	result2 := r.GetAttributes(volumeID)

	// Modify first result
	if len(result1) > 0 {
		result1[0] = attribute.String("modified", "modified")
	}

	// Second result should be unaffected
	hasModified := false
	for _, attr := range result2 {
		if string(attr.Key) == "modified" {
			hasModified = true
		}
	}
	if hasModified {
		t.Error("modifying returned slice should not affect subsequent calls")
	}
}

func TestVolumeAttributeRegistry_GetAttributes_Order(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	// Add multiple attributes
	r.AddAttributes(volumeID,
		attribute.String("c", "3"),
		attribute.String("a", "1"),
		attribute.String("b", "2"),
	)

	// Note: Map iteration order is not guaranteed, so we just verify all are present
	result := r.GetAttributes(volumeID)
	if len(result) != 3 {
		t.Fatalf("expected 3 attributes, got %d", len(result))
	}

	keys := make([]string, len(result))
	for i, attr := range result {
		keys[i] = string(attr.Key)
	}
	sort.Strings(keys)

	expected := []string{"a", "b", "c"}
	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("keys[%d] = %s, want %s", i, key, expected[i])
		}
	}
}

func TestVolumeAttributeRegistry_DuplicateKeyInSingleCall(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	// Add attributes with duplicate keys in single call
	r.AddAttributes(volumeID,
		attribute.String("key", "first"),
		attribute.String("key", "second"),
	)

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Fatalf("expected 1 attribute (deduplicated), got %d", len(result))
	}

	// Last one should win
	if result[0].Value.AsString() != "second" {
		t.Errorf("expected 'second', got %s", result[0].Value.AsString())
	}
}

func TestVolumeAttributeRegistry_SetAttributes_DuplicateKeys(t *testing.T) {
	r := NewVolumeAttributeRegistry()
	volumeID := domain.VolumeID(1)

	// Set attributes with duplicate keys
	r.SetAttributes(volumeID, []attribute.KeyValue{
		attribute.String("key", "first"),
		attribute.String("key", "second"),
	})

	result := r.GetAttributes(volumeID)
	if len(result) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(result))
	}

	// Last one should win
	if result[0].Value.AsString() != "second" {
		t.Errorf("expected 'second', got %s", result[0].Value.AsString())
	}
}
