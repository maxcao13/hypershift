package karpenterignition

import (
	"context"
	"fmt"

	hyperkarpenterv1 "github.com/openshift/hypershift/api/karpenter/v1"
	"github.com/openshift/hypershift/hypershift-operator/controllers/nodepool"
	karpenterutil "github.com/openshift/hypershift/support/karpenter"
	"github.com/openshift/hypershift/support/util"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *KarpenterIgnitionReconciler) getUserDataSecret(ctx context.Context, expectedNodePoolName, nodeClassName string) (*corev1.Secret, error) {
	labelSelector := labels.SelectorFromSet(labels.Set{karpenterutil.ManagedByKarpenterLabel: "true"})
	secretList := &corev1.SecretList{}
	if err := r.ManagementClient.List(ctx, secretList, &client.ListOptions{
		LabelSelector: labelSelector,
		Namespace:     r.Namespace,
	}); err != nil {
		return nil, fmt.Errorf("failed to list secrets: %w", err)
	}

	for i := range secretList.Items {
		secret := &secretList.Items[i]
		annotations := secret.GetAnnotations()
		if annotations == nil || annotations[hyperkarpenterv1.TokenSecretNodePoolAnnotation] == "" {
			continue
		}
		if annotations[nodepool.TokenSecretAnnotation] == "true" {
			continue
		}
		nodePoolAnnotation := util.ParseNamespacedName(annotations[hyperkarpenterv1.TokenSecretNodePoolAnnotation])
		if nodePoolAnnotation.Name == expectedNodePoolName {
			return secret, nil
		}
	}

	return nil, fmt.Errorf("userData secret not found for nodePool %q nodeClass %q", expectedNodePoolName, nodeClassName)
}
