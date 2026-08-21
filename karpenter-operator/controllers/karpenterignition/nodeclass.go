package karpenterignition

import (
	"context"
	"fmt"

	hyperkarpenterv1 "github.com/openshift/hypershift/api/karpenter/v1"
	karpenterutil "github.com/openshift/hypershift/support/karpenter"

	azurekarpenterv1beta1 "github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type nodeClassKind string

const (
	nodeClassKindOpenshiftEC2 nodeClassKind = "openshiftec2nodeclass"
	nodeClassKindAKS          nodeClassKind = "aksnodeclass"
)

// ignitionNodeClass is the platform-neutral view of a Karpenter NodeClass for ignition reconciliation.
type ignitionNodeClass interface {
	Kind() nodeClassKind
	Name() string
	NodePoolName() string
	SpecVersion() string
	StatusVersion() string
	KubeletIsZero() bool
	SupportsVersionStatus() bool
	GetObjectMeta() *metav1.ObjectMeta
	GetAnnotations() map[string]string
	SetAnnotations(map[string]string)
	GetGeneration() int64
	GetDeletionTimestamp() *metav1.Time
	GetClientObject() client.Object
	GetStatusConditions() *[]metav1.Condition
	GetStatusReleaseImage() *string
	GetStatusVersion() *string
	GetSpecVersionField() *string
}

type openshiftEC2NodeClass struct {
	*hyperkarpenterv1.OpenshiftEC2NodeClass
}

func (n *openshiftEC2NodeClass) Kind() nodeClassKind { return nodeClassKindOpenshiftEC2 }
func (n *openshiftEC2NodeClass) Name() string        { return n.OpenshiftEC2NodeClass.Name }
func (n *openshiftEC2NodeClass) NodePoolName() string {
	return karpenterutil.KarpenterNodePoolName(n.OpenshiftEC2NodeClass)
}
func (n *openshiftEC2NodeClass) SpecVersion() string  { return n.OpenshiftEC2NodeClass.Spec.Version }
func (n *openshiftEC2NodeClass) StatusVersion() string { return n.OpenshiftEC2NodeClass.Status.Version }
func (n *openshiftEC2NodeClass) KubeletIsZero() bool  { return n.OpenshiftEC2NodeClass.Spec.Kubelet.IsZero() }
func (n *openshiftEC2NodeClass) SupportsVersionStatus() bool { return true }
func (n *openshiftEC2NodeClass) GetObjectMeta() *metav1.ObjectMeta {
	return &n.OpenshiftEC2NodeClass.ObjectMeta
}
func (n *openshiftEC2NodeClass) GetAnnotations() map[string]string {
	return n.OpenshiftEC2NodeClass.GetAnnotations()
}
func (n *openshiftEC2NodeClass) SetAnnotations(annotations map[string]string) {
	n.OpenshiftEC2NodeClass.SetAnnotations(annotations)
}
func (n *openshiftEC2NodeClass) GetGeneration() int64 { return n.OpenshiftEC2NodeClass.GetGeneration() }
func (n *openshiftEC2NodeClass) GetDeletionTimestamp() *metav1.Time {
	return n.OpenshiftEC2NodeClass.GetDeletionTimestamp()
}
func (n *openshiftEC2NodeClass) GetClientObject() client.Object { return n.OpenshiftEC2NodeClass }
func (n *openshiftEC2NodeClass) GetStatusConditions() *[]metav1.Condition {
	return &n.OpenshiftEC2NodeClass.Status.Conditions
}
func (n *openshiftEC2NodeClass) GetStatusReleaseImage() *string {
	return &n.OpenshiftEC2NodeClass.Status.ReleaseImage
}
func (n *openshiftEC2NodeClass) GetStatusVersion() *string {
	return &n.OpenshiftEC2NodeClass.Status.Version
}
func (n *openshiftEC2NodeClass) GetSpecVersionField() *string {
	return &n.OpenshiftEC2NodeClass.Spec.Version
}

type aksNodeClass struct {
	*azurekarpenterv1beta1.AKSNodeClass
}

func (n *aksNodeClass) Kind() nodeClassKind { return nodeClassKindAKS }
func (n *aksNodeClass) Name() string        { return n.AKSNodeClass.Name }
func (n *aksNodeClass) NodePoolName() string {
	return karpenterutil.KarpenterNodePoolNameFromNodeClassName(n.AKSNodeClass.Name)
}
func (n *aksNodeClass) SpecVersion() string   { return "" }
func (n *aksNodeClass) StatusVersion() string { return "" }
func (n *aksNodeClass) KubeletIsZero() bool   { return n.AKSNodeClass.Spec.Kubelet == nil }
func (n *aksNodeClass) SupportsVersionStatus() bool { return false }
func (n *aksNodeClass) GetObjectMeta() *metav1.ObjectMeta { return &n.AKSNodeClass.ObjectMeta }
func (n *aksNodeClass) GetAnnotations() map[string]string { return n.AKSNodeClass.GetAnnotations() }
func (n *aksNodeClass) SetAnnotations(annotations map[string]string) {
	n.AKSNodeClass.SetAnnotations(annotations)
}
func (n *aksNodeClass) GetGeneration() int64 { return n.AKSNodeClass.GetGeneration() }
func (n *aksNodeClass) GetDeletionTimestamp() *metav1.Time { return n.AKSNodeClass.GetDeletionTimestamp() }
func (n *aksNodeClass) GetClientObject() client.Object     { return n.AKSNodeClass }
func (n *aksNodeClass) GetStatusConditions() *[]metav1.Condition { return nil }
func (n *aksNodeClass) GetStatusReleaseImage() *string           { return nil }
func (n *aksNodeClass) GetStatusVersion() *string                { return nil }
func (n *aksNodeClass) GetSpecVersionField() *string             { return nil }

func (r *KarpenterIgnitionReconciler) loadOpenshiftEC2NodeClass(ctx context.Context, key types.NamespacedName) (ignitionNodeClass, error) {
	obj := &hyperkarpenterv1.OpenshiftEC2NodeClass{}
	if err := r.GuestClient.Get(ctx, key, obj); err != nil {
		return nil, err
	}
	return &openshiftEC2NodeClass{OpenshiftEC2NodeClass: obj}, nil
}

func (r *KarpenterIgnitionReconciler) loadAKSNodeClass(ctx context.Context, key types.NamespacedName) (ignitionNodeClass, error) {
	obj := &azurekarpenterv1beta1.AKSNodeClass{}
	if err := r.GuestClient.Get(ctx, key, obj); err != nil {
		return nil, err
	}
	return &aksNodeClass{AKSNodeClass: obj}, nil
}

func (r *KarpenterIgnitionReconciler) syncAKSNodeClassUserData(ctx context.Context, nc *azurekarpenterv1beta1.AKSNodeClass, nodePoolName string) error {
	userDataSecret, err := r.getUserDataSecret(ctx, nodePoolName, nc.Name)
	if err != nil {
		return fmt.Errorf("failed to get userData secret for AKSNodeClass %q: %w", nc.Name, err)
	}

	userData := string(userDataSecret.Data["value"])
	if nc.Spec.UserData != nil && *nc.Spec.UserData == userData {
		return nil
	}

	original := nc.DeepCopy()
	nc.Spec.UserData = &userData
	if err := r.GuestClient.Patch(ctx, nc, client.MergeFromWithOptions(original, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to patch AKSNodeClass userData: %w", err)
	}
	return nil
}

func isNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}
