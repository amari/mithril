package domain

import "testing"

func TestChunkState_Constants(t *testing.T) {
	// Verify iota ordering
	if ChunkStateTemp != 0 {
		t.Errorf("ChunkStateTemp = %d, want 0", ChunkStateTemp)
	}
	if ChunkStateAvailable != 1 {
		t.Errorf("ChunkStateAvailable = %d, want 1", ChunkStateAvailable)
	}
	if ChunkStateDeleted != 2 {
		t.Errorf("ChunkStateDeleted = %d, want 2", ChunkStateDeleted)
	}
}

func TestChunkState_String(t *testing.T) {
	testCases := []struct {
		state    ChunkState
		expected string
	}{
		{ChunkStateTemp, "pending"},
		{ChunkStateAvailable, "stored"},
		{ChunkStateDeleted, "deleted"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			if tc.state.String() != tc.expected {
				t.Errorf("String() = %q, want %q", tc.state.String(), tc.expected)
			}
		})
	}
}

func TestChunkState_String_InvalidValue(t *testing.T) {
	// Test with values outside defined constants
	invalidStates := []ChunkState{
		ChunkState(-1),
		ChunkState(3),
		ChunkState(100),
		ChunkState(1000),
	}

	for _, state := range invalidStates {
		result := state.String()
		if result != "unknown" {
			t.Errorf("String() for invalid state %d = %q, want \"unknown\"", state, result)
		}
	}
}

func TestChunkState_ZeroValue(t *testing.T) {
	var state ChunkState

	if state != ChunkStateTemp {
		t.Errorf("zero ChunkState = %v, want ChunkStateTemp", state)
	}
	if state.String() != "pending" {
		t.Errorf("zero ChunkState.String() = %q, want \"pending\"", state.String())
	}
}

func TestChunkState_Equality(t *testing.T) {
	state1 := ChunkStateAvailable
	state2 := ChunkStateAvailable
	state3 := ChunkStateDeleted

	if state1 != state2 {
		t.Error("identical ChunkStates should be equal")
	}
	if state1 == state3 {
		t.Error("different ChunkStates should not be equal")
	}
}

func TestChunkState_UsableInSwitch(t *testing.T) {
	testCases := []struct {
		state    ChunkState
		expected string
	}{
		{ChunkStateTemp, "temp"},
		{ChunkStateAvailable, "available"},
		{ChunkStateDeleted, "deleted"},
	}

	for _, tc := range testCases {
		var result string
		switch tc.state {
		case ChunkStateTemp:
			result = "temp"
		case ChunkStateAvailable:
			result = "available"
		case ChunkStateDeleted:
			result = "deleted"
		default:
			result = "other"
		}

		if result != tc.expected {
			t.Errorf("switch result for %v = %q, want %q", tc.state, result, tc.expected)
		}
	}
}

func TestChunkState_UsableAsMapKey(t *testing.T) {
	m := make(map[ChunkState]string)

	m[ChunkStateTemp] = "temp"
	m[ChunkStateAvailable] = "available"
	m[ChunkStateDeleted] = "deleted"

	if m[ChunkStateAvailable] != "available" {
		t.Errorf("map[ChunkStateAvailable] = %q, want \"available\"", m[ChunkStateAvailable])
	}
	if len(m) != 3 {
		t.Errorf("map length = %d, want 3", len(m))
	}
}

func TestChunkState_String_Deterministic(t *testing.T) {
	state := ChunkStateAvailable

	str1 := state.String()
	str2 := state.String()

	if str1 != str2 {
		t.Errorf("String() not deterministic: %q != %q", str1, str2)
	}
}

func TestChunkState_Comparable(t *testing.T) {
	// Verify states can be compared (useful for ordering)
	if ChunkStateTemp >= ChunkStateAvailable {
		t.Error("ChunkStateTemp should be less than ChunkStateAvailable")
	}
	if ChunkStateAvailable >= ChunkStateDeleted {
		t.Error("ChunkStateAvailable should be less than ChunkStateDeleted")
	}
}

func TestChunkState_AllStatesHaveUniqueStrings(t *testing.T) {
	states := []ChunkState{
		ChunkStateTemp,
		ChunkStateAvailable,
		ChunkStateDeleted,
	}

	seen := make(map[string]ChunkState)
	for _, state := range states {
		str := state.String()
		if existing, ok := seen[str]; ok {
			t.Errorf("duplicate string %q for states %d and %d", str, existing, state)
		}
		seen[str] = state
	}
}

func TestChunkState_StringNotEmpty(t *testing.T) {
	states := []ChunkState{
		ChunkStateTemp,
		ChunkStateAvailable,
		ChunkStateDeleted,
	}

	for _, state := range states {
		if state.String() == "" {
			t.Errorf("String() for state %d returned empty string", state)
		}
	}
}

func TestChunkState_TypeConversion(t *testing.T) {
	// Verify ChunkState can be converted to/from int
	state := ChunkStateAvailable
	asInt := int(state)

	if asInt != 1 {
		t.Errorf("int(ChunkStateAvailable) = %d, want 1", asInt)
	}

	fromInt := ChunkState(1)
	if fromInt != ChunkStateAvailable {
		t.Errorf("ChunkState(1) = %v, want ChunkStateAvailable", fromInt)
	}
}
