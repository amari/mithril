package domain

import "testing"

func TestVolumeID_Type(t *testing.T) {
	var id VolumeID = 42
	if uint16(id) != 42 {
		t.Errorf("VolumeID = %v, want 42", id)
	}
}

func TestVolumeID_ZeroValue(t *testing.T) {
	var id VolumeID
	if id != 0 {
		t.Errorf("zero VolumeID = %v, want 0", id)
	}
}

func TestVolumeID_MaxValue(t *testing.T) {
	var id VolumeID = 65535
	if uint16(id) != 65535 {
		t.Errorf("max VolumeID = %v, want 65535", id)
	}
}

func TestVolumeID_Equality(t *testing.T) {
	id1 := VolumeID(100)
	id2 := VolumeID(100)
	id3 := VolumeID(200)

	if id1 != id2 {
		t.Error("identical VolumeIDs should be equal")
	}
	if id1 == id3 {
		t.Error("different VolumeIDs should not be equal")
	}
}

func TestVolumeID_UsableAsMapKey(t *testing.T) {
	m := make(map[VolumeID]string)

	id1 := VolumeID(1)
	id2 := VolumeID(2)

	m[id1] = "first"
	m[id2] = "second"

	if m[id1] != "first" {
		t.Errorf("map[id1] = %q, want \"first\"", m[id1])
	}
	if m[id2] != "second" {
		t.Errorf("map[id2] = %q, want \"second\"", m[id2])
	}
}

func TestDirectoryVolume_Fields(t *testing.T) {
	vol := DirectoryVolume{
		VolumeID: VolumeID(42),
		Path:     "/data/volumes/42",
	}

	if vol.VolumeID != VolumeID(42) {
		t.Errorf("VolumeID = %v, want 42", vol.VolumeID)
	}
	if vol.Path != "/data/volumes/42" {
		t.Errorf("Path = %q, want \"/data/volumes/42\"", vol.Path)
	}
}

func TestDirectoryVolume_ZeroValue(t *testing.T) {
	var vol DirectoryVolume

	if vol.VolumeID != 0 {
		t.Errorf("zero DirectoryVolume.VolumeID = %v, want 0", vol.VolumeID)
	}
	if vol.Path != "" {
		t.Errorf("zero DirectoryVolume.Path = %q, want empty string", vol.Path)
	}
}

func TestDirectoryVolume_EmptyPath(t *testing.T) {
	vol := DirectoryVolume{
		VolumeID: VolumeID(1),
		Path:     "",
	}

	if vol.Path != "" {
		t.Errorf("Path = %q, want empty string", vol.Path)
	}
}

func TestDirectoryVolume_VariousPaths(t *testing.T) {
	testCases := []struct {
		name string
		path string
	}{
		{"absolute path", "/var/data/volume"},
		{"relative path", "data/volume"},
		{"with spaces", "/path/with spaces/volume"},
		{"with special chars", "/path/with-special_chars.123/volume"},
		{"root path", "/"},
		{"current dir", "."},
		{"parent dir", ".."},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vol := DirectoryVolume{
				VolumeID: VolumeID(1),
				Path:     tc.path,
			}
			if vol.Path != tc.path {
				t.Errorf("Path = %q, want %q", vol.Path, tc.path)
			}
		})
	}
}

func TestVolumeState_Constants(t *testing.T) {
	// Verify iota ordering
	if VolumeStateUnknown != 0 {
		t.Errorf("VolumeStateUnknown = %d, want 0", VolumeStateUnknown)
	}
	if VolumeStateOK != 1 {
		t.Errorf("VolumeStateOK = %d, want 1", VolumeStateOK)
	}
	if VolumeStateDegraded != 2 {
		t.Errorf("VolumeStateDegraded = %d, want 2", VolumeStateDegraded)
	}
	if VolumeStateFailed != 3 {
		t.Errorf("VolumeStateFailed = %d, want 3", VolumeStateFailed)
	}
}

func TestVolumeState_String(t *testing.T) {
	testCases := []struct {
		state    VolumeState
		expected string
	}{
		{VolumeStateUnknown, "Unknown"},
		{VolumeStateOK, "OK"},
		{VolumeStateDegraded, "Degraded"},
		{VolumeStateFailed, "Failed"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			if tc.state.String() != tc.expected {
				t.Errorf("String() = %q, want %q", tc.state.String(), tc.expected)
			}
		})
	}
}

func TestVolumeState_String_InvalidValue(t *testing.T) {
	// Test with values outside defined constants
	invalidStates := []VolumeState{
		VolumeState(-1),
		VolumeState(4),
		VolumeState(100),
		VolumeState(1000),
	}

	for _, state := range invalidStates {
		result := state.String()
		if result != "Unknown" {
			t.Errorf("String() for invalid state %d = %q, want \"Unknown\"", state, result)
		}
	}
}

func TestVolumeState_ZeroValue(t *testing.T) {
	var state VolumeState

	if state != VolumeStateUnknown {
		t.Errorf("zero VolumeState = %v, want VolumeStateUnknown", state)
	}
	if state.String() != "Unknown" {
		t.Errorf("zero VolumeState.String() = %q, want \"Unknown\"", state.String())
	}
}

func TestVolumeState_Equality(t *testing.T) {
	state1 := VolumeStateOK
	state2 := VolumeStateOK
	state3 := VolumeStateFailed

	if state1 != state2 {
		t.Error("identical VolumeStates should be equal")
	}
	if state1 == state3 {
		t.Error("different VolumeStates should not be equal")
	}
}

func TestVolumeState_UsableInSwitch(t *testing.T) {
	testCases := []struct {
		state    VolumeState
		expected string
	}{
		{VolumeStateUnknown, "unknown"},
		{VolumeStateOK, "ok"},
		{VolumeStateDegraded, "degraded"},
		{VolumeStateFailed, "failed"},
	}

	for _, tc := range testCases {
		var result string
		switch tc.state {
		case VolumeStateUnknown:
			result = "unknown"
		case VolumeStateOK:
			result = "ok"
		case VolumeStateDegraded:
			result = "degraded"
		case VolumeStateFailed:
			result = "failed"
		default:
			result = "other"
		}

		if result != tc.expected {
			t.Errorf("switch result for %v = %q, want %q", tc.state, result, tc.expected)
		}
	}
}

func TestVolumeHealth_Fields(t *testing.T) {
	health := VolumeHealth{
		State: VolumeStateOK,
	}

	if health.State != VolumeStateOK {
		t.Errorf("State = %v, want VolumeStateOK", health.State)
	}
}

func TestVolumeHealth_ZeroValue(t *testing.T) {
	var health VolumeHealth

	if health.State != VolumeStateUnknown {
		t.Errorf("zero VolumeHealth.State = %v, want VolumeStateUnknown", health.State)
	}
}

func TestVolumeHealth_AllStates(t *testing.T) {
	states := []VolumeState{
		VolumeStateUnknown,
		VolumeStateOK,
		VolumeStateDegraded,
		VolumeStateFailed,
	}

	for _, state := range states {
		health := VolumeHealth{State: state}
		if health.State != state {
			t.Errorf("VolumeHealth.State = %v, want %v", health.State, state)
		}
	}
}

func TestVolumeState_Comparable(t *testing.T) {
	// Verify states can be compared (useful for severity ordering)
	if VolumeStateUnknown >= VolumeStateOK {
		t.Error("VolumeStateUnknown should be less than VolumeStateOK")
	}
	if VolumeStateOK >= VolumeStateDegraded {
		t.Error("VolumeStateOK should be less than VolumeStateDegraded")
	}
	if VolumeStateDegraded >= VolumeStateFailed {
		t.Error("VolumeStateDegraded should be less than VolumeStateFailed")
	}
}

func TestVolumeState_UsableAsMapKey(t *testing.T) {
	m := make(map[VolumeState]string)

	m[VolumeStateUnknown] = "unknown"
	m[VolumeStateOK] = "ok"
	m[VolumeStateDegraded] = "degraded"
	m[VolumeStateFailed] = "failed"

	if m[VolumeStateOK] != "ok" {
		t.Errorf("map[VolumeStateOK] = %q, want \"ok\"", m[VolumeStateOK])
	}
	if len(m) != 4 {
		t.Errorf("map length = %d, want 4", len(m))
	}
}

func TestVolumeState_String_Deterministic(t *testing.T) {
	state := VolumeStateDegraded

	str1 := state.String()
	str2 := state.String()

	if str1 != str2 {
		t.Errorf("String() not deterministic: %q != %q", str1, str2)
	}
}

func TestDirectoryVolume_Equality(t *testing.T) {
	vol1 := DirectoryVolume{VolumeID: 1, Path: "/data"}
	vol2 := DirectoryVolume{VolumeID: 1, Path: "/data"}
	vol3 := DirectoryVolume{VolumeID: 2, Path: "/data"}
	vol4 := DirectoryVolume{VolumeID: 1, Path: "/other"}

	if vol1 != vol2 {
		t.Error("identical DirectoryVolumes should be equal")
	}
	if vol1 == vol3 {
		t.Error("DirectoryVolumes with different VolumeID should not be equal")
	}
	if vol1 == vol4 {
		t.Error("DirectoryVolumes with different Path should not be equal")
	}
}
