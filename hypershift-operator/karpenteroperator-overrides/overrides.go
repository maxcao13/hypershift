package karpenteroperatoroverrides

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/openshift/hypershift/support/thirdparty/library-go/pkg/image/reference"

	"gopkg.in/yaml.v2"
)

// Embed the directory so builds still work when overrides.yaml is absent.
//
//go:embed assets
var assets embed.FS

// Image returns the karpenter-operator image pinned to this HyperShift release.
// An absent file or empty image leaves image selection to the release payload.
func Image() (string, error) {
	return imageFromFS(assets)
}

func imageFromFS(assets fs.FS) (string, error) {
	content, err := fs.ReadFile(assets, "assets/overrides.yaml")
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read karpenter-operator override: %w", err)
	}
	var override struct {
		Image string `yaml:"image"`
	}
	if err := yaml.UnmarshalStrict(content, &override); err != nil {
		return "", fmt.Errorf("parse karpenter-operator override: %w", err)
	}
	if override.Image != "" {
		ref, err := reference.Parse(override.Image)
		if err != nil {
			return "", fmt.Errorf("parse karpenter-operator image: %w", err)
		}
		if ref.ID == "" {
			return "", fmt.Errorf("karpenter-operator override must be pinned by digest")
		}
	}
	return override.Image, nil
}
