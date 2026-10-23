package azure

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/gomega"
)

func TestHandleIdentitiesAndRBAC(t *testing.T) {
	const clientID = "12345678-1234-1234-1234-123456789012"
	tests := map[string]struct {
		managed  string
		workload string
		wantID   string
		wantKMS  string
		wantErr  string
	}{
		"When managed identities include Karpenter, it should read its client ID": {
			managed: `{"cloudProvider":{"clientID":"12345678-1234-1234-1234-123456789012"},"karpenterClientID":"12345678-1234-1234-1234-123456789012"}`,
			wantID:  clientID,
		},
		"When managed identities use YAML, it should read component and Karpenter client IDs": {
			managed: "cloudProvider:\n  clientID: " + clientID + "\nkarpenterClientID: " + clientID + "\n",
			wantID:  clientID,
		},
		"When managed identities omit Karpenter, it should leave its client ID empty": {
			managed: `{}`,
		},
		"When workload identities include Karpenter, it should read its client ID": {
			workload: `{"cloudProvider":{"clientID":"12345678-1234-1234-1234-123456789012"},"karpenterClientID":"12345678-1234-1234-1234-123456789012","kmsClientID":"23456789-1234-1234-1234-123456789012"}`,
			wantID:   clientID,
			wantKMS:  "23456789-1234-1234-1234-123456789012",
		},
		"When workload identities omit Karpenter, it should leave its client ID empty": {
			workload: `{}`,
		},
		"When the workload Karpenter client ID is not a string, it should return an error": {
			workload: `{"karpenterClientID":{}}`,
			wantErr:  "failed to unmarshal --workload-identities-file",
		},
		"When the workload KMS client ID is not a string, it should return an error": {
			workload: `{"kmsClientID":{}}`,
			wantErr:  "failed to unmarshal --workload-identities-file",
		},
		"When no identities are supplied, it should leave its client ID empty": {},
		"When the Karpenter client ID is not a string, it should return an error": {
			managed: `{"karpenterClientID":{}}`,
			wantErr: "failed to unmarshal --managed-identities-file",
		},
		"When managed identities contain invalid JSON, it should return an error": {
			managed: `{"`,
			wantErr: "failed to unmarshal --managed-identities-file",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			opts := &CreateInfraOptions{}
			if tc.managed != "" {
				opts.ManagedIdentitiesFile = filepath.Join(t.TempDir(), "managed-identities.json")
				g.Expect(os.WriteFile(opts.ManagedIdentitiesFile, []byte(tc.managed), 0600)).To(Succeed())
			}
			if tc.workload != "" {
				opts.WorkloadIdentitiesFile = filepath.Join(t.TempDir(), "workload-identities.json")
				g.Expect(os.WriteFile(opts.WorkloadIdentitiesFile, []byte(tc.workload), 0600)).To(Succeed())
			}
			result := &CreateInfraOutput{}
			// A nil RBAC manager also checks that disabled role assignment makes no Azure calls.
			err := opts.handleIdentitiesAndRBAC(context.Background(), nil, result, "cluster-rg", "nsg-rg", "vnet-rg")
			if tc.wantErr != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.wantErr)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.KarpenterClientID).To(Equal(tc.wantID))
			g.Expect(result.KMSClientID).To(Equal(tc.wantKMS))
			if tc.workload != "" {
				g.Expect(result.WorkloadIdentities).NotTo(BeNil())
				g.Expect(result.ControlPlaneMIs).To(BeNil())
				if tc.wantID != "" {
					g.Expect(string(result.WorkloadIdentities.CloudProvider.ClientID)).To(Equal(clientID))
				}
			}
			if tc.managed != "" {
				g.Expect(result.ControlPlaneMIs).NotTo(BeNil())
				g.Expect(result.WorkloadIdentities).To(BeNil())
				if tc.wantID != "" {
					g.Expect(string(result.ControlPlaneMIs.ControlPlane.CloudProvider.ClientID)).To(Equal(clientID))
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := map[string]struct {
		opts          CreateInfraOptions
		expectedError bool
		errorContains string
		description   string
	}{
		"When base domain is provided it should pass validation": {
			opts: CreateInfraOptions{
				BaseDomain: "example.com",
			},
			expectedError: false,
			description:   "Should pass when base domain is provided",
		},
		"When base domain is missing it should return an error": {
			opts:          CreateInfraOptions{},
			expectedError: true,
			errorContains: "--base-domain is required",
			description:   "Should require base domain",
		},
		"When assign-identity-roles is set without dns-zone-rg-name it should return an error": {
			opts: CreateInfraOptions{
				BaseDomain:                  "example.com",
				AssignServicePrincipalRoles: true,
				DNSZoneRG:                   "",
			},
			expectedError: true,
			errorContains: "--dns-zone-rg-name is required when --assign-identity-roles or --assign-custom-hcp-roles is set",
			description:   "Should require dns-zone-rg-name when assign-identity-roles is set",
		},
		"When assign-custom-hcp-roles is set without dns-zone-rg-name it should return an error": {
			opts: CreateInfraOptions{
				BaseDomain:           "example.com",
				AssignCustomHCPRoles: true,
				DNSZoneRG:            "",
			},
			expectedError: true,
			errorContains: "--dns-zone-rg-name is required when --assign-identity-roles or --assign-custom-hcp-roles is set",
			description:   "Should require dns-zone-rg-name when assign-custom-hcp-roles is set",
		},
		"When assign-identity-roles is set with dns-zone-rg-name it should pass validation": {
			opts: CreateInfraOptions{
				BaseDomain:                  "example.com",
				AssignServicePrincipalRoles: true,
				DNSZoneRG:                   "my-dns-rg",
			},
			expectedError: false,
			description:   "Should pass when both assign-identity-roles and dns-zone-rg-name are set",
		},
		"When assign-identity-roles is not set and dns-zone-rg-name is empty it should pass validation": {
			opts: CreateInfraOptions{
				BaseDomain:                  "example.com",
				AssignServicePrincipalRoles: false,
				DNSZoneRG:                   "",
			},
			expectedError: false,
			description:   "Should pass when assign-identity-roles is not set regardless of dns-zone-rg-name",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			g := NewGomegaWithT(t)

			err := test.opts.Validate()

			if test.expectedError {
				g.Expect(err).To(HaveOccurred(), test.description)
				g.Expect(err.Error()).To(ContainSubstring(test.errorContains), test.description)
			} else {
				g.Expect(err).ToNot(HaveOccurred(), test.description)
			}
		})
	}
}

func TestValidateDeploymentModelFlags(t *testing.T) {
	tests := map[string]struct {
		opts          CreateInfraOptions
		expectedError bool
		description   string
	}{
		"When both ARO HCP managed and data plane identities are provided it should pass validation": {
			opts: CreateInfraOptions{
				ManagedIdentitiesFile:   "mi.json",
				DataPlaneIdentitiesFile: "dp.json",
			},
			expectedError: false,
			description:   "Should allow both ARO HCP identity files together",
		},
		"When workload identities file is provided alone it should pass validation": {
			opts: CreateInfraOptions{
				WorkloadIdentitiesFile: "wi.json",
			},
			expectedError: false,
			description:   "Should allow workload identities file alone",
		},
		"When no identity configuration is provided it should return an error": {
			opts:          CreateInfraOptions{},
			expectedError: true,
			description:   "Should reject empty configuration without any identity settings",
		},
		"When ARO HCP managed identities are mixed with workload identities it should return an error": {
			opts: CreateInfraOptions{
				ManagedIdentitiesFile:  "mi.json",
				WorkloadIdentitiesFile: "wi.json",
			},
			expectedError: true,
			description:   "Should reject mixing ARO HCP managed identities with self-managed workload identities",
		},
		"When ARO HCP data plane identities are mixed with workload identities it should return an error": {
			opts: CreateInfraOptions{
				DataPlaneIdentitiesFile: "dp.json",
				WorkloadIdentitiesFile:  "wi.json",
			},
			expectedError: true,
			description:   "Should reject mixing ARO HCP data plane identities with self-managed workload identities",
		},
		"When all deployment models are mixed together it should return an error": {
			opts: CreateInfraOptions{
				ManagedIdentitiesFile:   "mi.json",
				DataPlaneIdentitiesFile: "dp.json",
				WorkloadIdentitiesFile:  "wi.json",
			},
			expectedError: true,
			description:   "Should reject mixing all deployment models together",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			g := NewGomegaWithT(t)

			err := test.opts.validateDeploymentModelFlags()

			if test.expectedError {
				g.Expect(err).To(HaveOccurred(), test.description)
				// Verify the error mentions either mutual exclusion or missing configuration
				g.Expect(err.Error()).To(SatisfyAny(
					ContainSubstring("mutually exclusive"),
					ContainSubstring("--workload-identities-file is required"),
				), "Error should mention validation failure: %s", test.description)
			} else {
				g.Expect(err).ToNot(HaveOccurred(), test.description)
			}
		})
	}
}
