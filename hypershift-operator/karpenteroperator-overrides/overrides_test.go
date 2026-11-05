package karpenteroperatoroverrides

import (
	_ "embed"
	"io/fs"
	"testing"
	"testing/fstest"
)

//go:embed assets/overrides_sample.yaml
var overridesSampleYAML []byte

func TestImage(t *testing.T) {
	if _, err := Image(); err != nil {
		t.Fatalf("invalid embedded karpenter-operator override: %v", err)
	}
}

func TestImageFromFS(t *testing.T) {
	const pinnedImage = "registry.redhat.io/openshift4/karpenter-operator-rhel9@sha256:88c55ea554f7e62a64e34ff8d3be45ef85ef6b80fe4e9b0240b9a1aa226f9d98"
	tests := []struct {
		name      string
		files     fstest.MapFS
		wantImage string
		wantError bool
	}{
		{
			name: "When the override file is absent, it should leave the image unset",
		},
		{
			name:  "When the override file is empty, it should leave the image unset",
			files: fstest.MapFS{"assets/overrides.yaml": {Data: nil}},
		},
		{
			name:  "When the image is empty, it should leave the image unset",
			files: fstest.MapFS{"assets/overrides.yaml": {Data: []byte("image: \"\"\n")}},
		},
		{
			name:      "When the sample override pins an image by digest, it should return the image",
			files:     fstest.MapFS{"assets/overrides.yaml": {Data: overridesSampleYAML}},
			wantImage: pinnedImage,
		},
		{
			name:      "When the YAML is malformed, it should return an error",
			files:     fstest.MapFS{"assets/overrides.yaml": {Data: []byte("image: [")}},
			wantError: true,
		},
		{
			name:      "When the field name is misspelled, it should return an error",
			files:     fstest.MapFS{"assets/overrides.yaml": {Data: []byte("images: " + pinnedImage)}},
			wantError: true,
		},
		{
			name:      "When the image uses a tag instead of a digest, it should return an error",
			files:     fstest.MapFS{"assets/overrides.yaml": {Data: []byte("image: registry.redhat.io/openshift4/karpenter-operator-rhel9:latest")}},
			wantError: true,
		},
		{
			name:      "When the image digest is invalid, it should return an error",
			files:     fstest.MapFS{"assets/overrides.yaml": {Data: []byte("image: registry.redhat.io/openshift4/karpenter-operator-rhel9@sha256:invalid")}},
			wantError: true,
		},
		{
			name:      "When the override path is a directory, it should return a read error",
			files:     fstest.MapFS{"assets/overrides.yaml": {Mode: fs.ModeDir}},
			wantError: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := imageFromFS(tc.files)
			if (err != nil) != tc.wantError {
				t.Fatalf("imageFromFS() error = %v, want error %t", err, tc.wantError)
			}
			if got != tc.wantImage {
				t.Errorf("imageFromFS() = %q, want %q", got, tc.wantImage)
			}
		})
	}
}
