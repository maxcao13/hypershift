package karpenterignition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	hyperkarpenterv1 "github.com/openshift/hypershift/api/karpenter/v1"
	"github.com/openshift/hypershift/control-plane-operator/controllers/hostedcontrolplane/common"
	"github.com/openshift/hypershift/hypershift-operator/controllers/nodepool"
	haproxy "github.com/openshift/hypershift/hypershift-operator/controllers/nodepool/apiserver-haproxy"
	"github.com/openshift/hypershift/hypershift-operator/featuregate"
	"github.com/openshift/hypershift/support/k8sutil"
	karpenterutil "github.com/openshift/hypershift/support/karpenter"
	"github.com/openshift/hypershift/support/releaseinfo"
	"github.com/openshift/hypershift/support/supportedversion"
	"github.com/openshift/hypershift/support/upsert"
	supportutil "github.com/openshift/hypershift/support/util"

	azurekarpenterv1beta1 "github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	configv1 "github.com/openshift/api/config/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/yaml"

	"github.com/blang/semver"
)

const (
	openshiftEC2NodeClassAnnotationCurrentConfigVersion = "hypershift.openshift.io/nodeClassCurrentConfigVersion"
	aksNodeClassAnnotationCurrentConfigVersion          = "hypershift.openshift.io/aksNodeClassCurrentConfigVersion"

	// nodePoolAnnotationCurrentConfigVersion mirrors the annotation from nodepool_controller.go
	// It's used to track the current config version for outdated token cleanup
	nodePoolAnnotationCurrentConfigVersion = "hypershift.openshift.io/nodePoolCurrentConfigVersion"

	kubeletConfigFinalizer = "hypershift.openshift.io/karpenter-kubelet-config-finalizer"
)

type KarpenterIgnitionReconciler struct {
	ManagementClient        client.Client
	GuestClient             client.Client
	ReleaseProvider         releaseinfo.Provider
	VersionResolver         releaseinfo.VersionResolver
	ImageMetadataProvider   supportutil.ImageMetadataProvider
	HypershiftOperatorImage string
	IgnitionEndpoint        string
	Namespace               string
	Platform                hyperv1.PlatformType
	upsert.CreateOrUpdateProvider
}

func (r *KarpenterIgnitionReconciler) SetupWithManager(mgr ctrl.Manager, managementCluster cluster.Cluster) error {
	r.GuestClient = mgr.GetClient()
	r.ManagementClient = managementCluster.GetClient()
	r.CreateOrUpdateProvider = upsert.New(false)

	switch r.Platform {
	case hyperv1.AWSPlatform:
		return r.setupOpenshiftEC2NodeClassController(mgr, managementCluster)
	case hyperv1.AzurePlatform:
		return r.setupAKSNodeClassController(mgr, managementCluster)
	default:
		return fmt.Errorf("unsupported platform %q for ignition controller", r.Platform)
	}
}

func (r *KarpenterIgnitionReconciler) setupOpenshiftEC2NodeClassController(mgr ctrl.Manager, managementCluster cluster.Cluster) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("karpenter-ignition-openshiftec2nodeclass-controller").
		For(&hyperkarpenterv1.OpenshiftEC2NodeClass{}).
		WatchesRawSource(source.Kind[client.Object](managementCluster.GetCache(), &hyperv1.HostedControlPlane{},
			handler.EnqueueRequestsFromMapFunc(r.mapToOpenshiftEC2NodeClasses),
			r.hcpPredicate())).
		WithOptions(controller.Options{
			RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](1*time.Second, 10*time.Second),
			MaxConcurrentReconciles: 10,
		}).
		Complete(r.reconcileOpenshiftEC2NodeClass())
}

func (r *KarpenterIgnitionReconciler) setupAKSNodeClassController(mgr ctrl.Manager, managementCluster cluster.Cluster) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("karpenter-ignition-aksnodeclass-controller").
		For(&azurekarpenterv1beta1.AKSNodeClass{}).
		WatchesRawSource(source.Kind[client.Object](managementCluster.GetCache(), &hyperv1.HostedControlPlane{},
			handler.EnqueueRequestsFromMapFunc(r.mapToAKSNodeClasses),
			r.hcpPredicate())).
		WithOptions(controller.Options{
			RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](1*time.Second, 10*time.Second),
			MaxConcurrentReconciles: 10,
		}).
		Complete(r.reconcileAKSNodeClass())
}

func (r *KarpenterIgnitionReconciler) reconcileOpenshiftEC2NodeClass() reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		nodeClass, err := r.loadOpenshiftEC2NodeClass(ctx, req.NamespacedName)
		if err != nil {
			if isNotFound(err) {
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("failed to get OpenshiftEC2NodeClass: %w", err)
		}
		return r.reconcileNodeClass(ctx, nodeClass)
	})
}

func (r *KarpenterIgnitionReconciler) reconcileAKSNodeClass() reconcile.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		nodeClass, err := r.loadAKSNodeClass(ctx, req.NamespacedName)
		if err != nil {
			if isNotFound(err) {
				return reconcile.Result{}, nil
			}
			return reconcile.Result{}, fmt.Errorf("failed to get AKSNodeClass: %w", err)
		}
		return r.reconcileNodeClass(ctx, nodeClass)
	})
}

func (r *KarpenterIgnitionReconciler) reconcileNodeClass(ctx context.Context, nodeClass ignitionNodeClass) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Reconciling Karpenter ignition config", "kind", nodeClass.Kind(), "nodeclass", nodeClass.Name())

	hcp, err := karpenterutil.GetHCP(ctx, r.ManagementClient, r.Namespace)
	if err != nil {
		if errors.Is(err, karpenterutil.ErrHCPNotFound) {
			log.Info("HostedControlPlane not found, re-queueing")
			return ctrl.Result{RequeueAfter: time.Second * 5}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get HostedControlPlane: %w", err)
	}

	// In practice, this shouldn't happen since the karpenter-operator pod would not exist if karpenter is not enabled
	// TODO(maxcao13): if we ever support disablement, that may change
	if !karpenterutil.IsKarpenterEnabled(hcp.Spec.AutoNode) {
		log.Info("Karpenter is not enabled, skipping reconcile")
		return ctrl.Result{}, nil
	}

	if nodeClass.GetDeletionTimestamp() != nil {
		return r.reconcileDeletedNodeClass(ctx, hcp, nodeClass)
	}

	if err := r.reconcileTaintConfigMap(ctx, hcp); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile taint configmap: %w", err)
	}

	hostedCluster, err := hostedClusterFromHCP(hcp, r.IgnitionEndpoint)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get HostedCluster: %w", err)
	}

	releaseImage := hcp.Spec.ReleaseImage
	version := currentClusterVersion(hostedCluster)

	var skewErr error
	if nodeClass.SpecVersion() != "" {
		version = nodeClass.SpecVersion()

		if err := validateVersion(hostedCluster, version, nodeClass.StatusVersion()); err != nil {
			if updateErr := r.updateVersionStatus(ctx, nodeClass, "", version, err); updateErr != nil {
				log.Error(updateErr, "failed to update version status after validation error")
			}
			return ctrl.Result{}, fmt.Errorf("failed to validate version for %s %q: %w", nodeClass.Kind(), nodeClass.Name(), err)
		}

		releaseImage, err = r.resolveReleaseImage(ctx, hcp, version)
		if err != nil {
			if updateErr := r.updateVersionStatus(ctx, nodeClass, "", version, err); updateErr != nil {
				log.Error(updateErr, "failed to update version status after resolve error")
			}
			return ctrl.Result{}, fmt.Errorf("failed to resolve version for %s %q: %w", nodeClass.Kind(), nodeClass.Name(), err)
		}
		log.Info("Resolved version to release image", "version", version, "channel", hcp.Spec.Channel, "releaseImage", releaseImage)

		skewErr = detectVersionSkew(hostedCluster, version)
	}

	if !nodeClass.KubeletIsZero() {
		if !controllerutil.ContainsFinalizer(nodeClass.GetClientObject(), kubeletConfigFinalizer) {
			original := nodeClass.GetClientObject().DeepCopyObject()
			controllerutil.AddFinalizer(nodeClass.GetClientObject(), kubeletConfigFinalizer)
			if err := r.GuestClient.Patch(ctx, nodeClass.GetClientObject(),
				client.MergeFromWithOptions(original.(client.Object), client.MergeFromWithOptimisticLock{})); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to add kubelet config finalizer: %w", err)
			}
		}
	}

	if err := r.reconcileKubeletConfigMap(ctx, hcp, nodeClass); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile kubelet config configmap: %w", err)
	}

	if nodeClass.KubeletIsZero() && controllerutil.ContainsFinalizer(nodeClass.GetClientObject(), kubeletConfigFinalizer) {
		original := nodeClass.GetClientObject().DeepCopyObject()
		controllerutil.RemoveFinalizer(nodeClass.GetClientObject(), kubeletConfigFinalizer)
		if err := r.GuestClient.Patch(ctx, nodeClass.GetClientObject(),
			client.MergeFromWithOptions(original.(client.Object), client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to remove kubelet config finalizer: %w", err)
		}
	}

	if err := r.reconcileNodeClassToken(ctx, hcp, hostedCluster, nodeClass, releaseImage); err != nil {
		log.Error(err, "failed to reconcile token", "kind", nodeClass.Kind(), "name", nodeClass.Name())
		if getErr := r.GuestClient.Get(ctx, client.ObjectKeyFromObject(nodeClass.GetClientObject()), nodeClass.GetClientObject()); getErr != nil {
			log.Error(getErr, "failed to re-fetch nodeclass after token reconciliation error")
		} else {
			setVersionSkewCondition(nodeClass, skewErr)
			if updateErr := r.updateVersionStatus(ctx, nodeClass, releaseImage, version, nil); updateErr != nil {
				log.Error(updateErr, "failed to update version status after token reconciliation error")
			}
		}
		return ctrl.Result{}, err
	}

	if aksNC, ok := nodeClass.(*aksNodeClass); ok {
		if err := r.syncAKSNodeClassUserData(ctx, aksNC.AKSNodeClass, nodeClass.NodePoolName()); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to sync AKSNodeClass userData: %w", err)
		}
	}

	setVersionSkewCondition(nodeClass, skewErr)
	if err := r.updateVersionStatus(ctx, nodeClass, releaseImage, version, nil); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update version status: %w", err)
	}

	return ctrl.Result{}, nil
}

// reconcileDeletedNodeClass handles cleanup when a NodeClass is being deleted.
func (r *KarpenterIgnitionReconciler) reconcileDeletedNodeClass(
	ctx context.Context,
	hcp *hyperv1.HostedControlPlane,
	nodeClass ignitionNodeClass,
) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	if !controllerutil.ContainsFinalizer(nodeClass.GetClientObject(), kubeletConfigFinalizer) {
		return ctrl.Result{}, nil
	}

	configMapName := karpenterutil.KarpenterNodeClassKubeletConfigName(nodeClass.Name())
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: hcp.Namespace,
		},
	}
	if _, err := k8sutil.DeleteIfNeeded(ctx, r.ManagementClient, cm); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete kubelet config configmap %s: %w", configMapName, err)
	}
	log.Info("Deleted kubelet config ConfigMap", "name", configMapName)

	original := nodeClass.GetClientObject().DeepCopyObject()
	controllerutil.RemoveFinalizer(nodeClass.GetClientObject(), kubeletConfigFinalizer)
	if err := r.GuestClient.Patch(ctx, nodeClass.GetClientObject(),
		client.MergeFromWithOptions(original.(client.Object), client.MergeFromWithOptimisticLock{})); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove kubelet config finalizer: %w", err)
	}
	log.Info("Removed kubelet config finalizer from NodeClass", "kind", nodeClass.Kind(), "name", nodeClass.Name())

	return ctrl.Result{}, nil
}

// reconcileNodeClassToken reconciles the ignition token and user-data secrets for a NodeClass.
func (r *KarpenterIgnitionReconciler) reconcileNodeClassToken(
	ctx context.Context,
	hcp *hyperv1.HostedControlPlane,
	hostedCluster *hyperv1.HostedCluster,
	nodeClass ignitionNodeClass,
	releaseImage string,
) error {
	log := ctrl.LoggerFrom(ctx).WithValues("kind", nodeClass.Kind(), "nodeclass", nodeClass.Name())

	np := r.createInMemoryNodePool(hcp, nodeClass, releaseImage)

	cg, err := r.buildConfigGenerator(ctx, hostedCluster, np, hcp.Namespace)
	if err != nil {
		return fmt.Errorf("failed to build config generator: %w", err)
	}

	token, err := nodepool.NewToken(ctx, cg, &nodepool.CPOCapabilities{
		DecompressAndDecodeConfig: true,
	})
	if err != nil {
		return fmt.Errorf("failed to create token: %w", err)
	}

	currentConfigVersion := nodeClass.GetAnnotations()[nodeClassConfigVersionAnnotation(nodeClass)]
	if currentConfigVersion == "" {
		np.GetAnnotations()[nodePoolAnnotationCurrentConfigVersion] = cg.Hash()
	} else {
		np.GetAnnotations()[nodePoolAnnotationCurrentConfigVersion] = currentConfigVersion
	}

	if err := token.Reconcile(ctx); err != nil {
		return fmt.Errorf("failed to reconcile token: %w", err)
	}

	if currentConfigVersion != cg.Hash() {
		if err := r.updateConfigVersionAnnotation(ctx, nodeClass, cg.Hash()); err != nil {
			return err
		}
		log.Info("Updated config version annotation", "oldVersion", currentConfigVersion, "newVersion", cg.Hash())
	}

	return nil
}

func nodeClassConfigVersionAnnotation(nodeClass ignitionNodeClass) string {
	switch nodeClass.Kind() {
	case nodeClassKindAKS:
		return aksNodeClassAnnotationCurrentConfigVersion
	default:
		return openshiftEC2NodeClassAnnotationCurrentConfigVersion
	}
}

func (r *KarpenterIgnitionReconciler) createInMemoryNodePool(
	hcp *hyperv1.HostedControlPlane,
	nodeClass ignitionNodeClass,
	releaseImage string,
) *hyperv1.NodePool {
	var configRefs []corev1.LocalObjectReference
	if !nodeClass.KubeletIsZero() {
		configRefs = []corev1.LocalObjectReference{
			{Name: karpenterutil.KarpenterNodeClassKubeletConfigName(nodeClass.Name())},
		}
	} else {
		configRefs = []corev1.LocalObjectReference{
			{Name: karpenterutil.KarpenterTaintConfigMapName},
		}
	}

	return &hyperv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:        nodeClass.NodePoolName(),
			Namespace:   hcp.Namespace,
			Annotations: map[string]string{},
			Labels: map[string]string{
				karpenterutil.ManagedByKarpenterLabel: "true",
			},
		},
		Spec: hyperv1.NodePoolSpec{
			ClusterName: hcp.Name,
			Replicas:    ptr.To[int32](0),
			Release: hyperv1.Release{
				Image: releaseImage,
			},
			Platform: hyperv1.NodePoolPlatform{
				Type: hcp.Spec.Platform.Type,
			},
			Config: configRefs,
			Arch:   hyperv1.ArchitectureAMD64,
		},
	}
}

// currentClusterVersion returns the version of the most recently completed update from the
// HostedCluster's version history. It searches history entries for the one with state=Completed
// and the most recent CompletionTime. If no completed entries exist and there is exactly one
// history entry, it falls back to the desired version. This handles the case where a cluster
// is still rolling out its initial version.
func currentClusterVersion(hostedCluster *hyperv1.HostedCluster) string {
	if hostedCluster.Status.Version == nil {
		return ""
	}

	var latest *configv1.UpdateHistory
	for i := range hostedCluster.Status.Version.History {
		entry := &hostedCluster.Status.Version.History[i]
		if entry.State != configv1.CompletedUpdate {
			continue
		}
		if latest == nil {
			latest = entry
			continue
		}
		if entry.CompletionTime != nil && latest.CompletionTime != nil && entry.CompletionTime.After(latest.CompletionTime.Time) {
			latest = entry
		}
	}

	if latest != nil {
		return latest.Version
	}

	// If there are no completed entries but exactly one history entry exists, the cluster
	// is likely still rolling out its first version. Fall back to the desired version.
	if len(hostedCluster.Status.Version.History) == 1 {
		return hostedCluster.Status.Version.Desired.Version
	}

	return hostedCluster.Status.Version.Desired.Version
}

// validateVersion checks whether the requested version is valid for the given HostedCluster.
// It validates semver parsing, range bounds, and y-stream downgrade detection.
// currentStatusVersion is the NodeClass's current status.version, used to detect y-stream downgrades.
// Returns nil when version is empty (nodes use the control plane image).
func validateVersion(hostedCluster *hyperv1.HostedCluster, version string, currentStatusVersion string) error {
	if version == "" {
		return nil
	}

	nodeClassVersion, err := semver.Parse(version)
	if err != nil {
		return fmt.Errorf("failed to parse OpenshiftEC2NodeClass version %q: %w", version, err)
	}

	hostedClusterVersion, err := semver.Parse(hostedCluster.Status.Version.Desired.Version)
	if err != nil {
		return fmt.Errorf("failed to parse HostedCluster version %q: %w", hostedCluster.Status.Version.Desired.Version, err)
	}

	// maxSupportedVersion is the current control plane version, as nodes can't use a version greater than the control plane.
	// The n-3 skew policy is enforced separately by detectVersionSkew.
	maxSupportedVersion := hostedClusterVersion
	minSupportedVersion := supportedversion.GetMinSupportedVersion(hostedCluster)

	// If the NodeClass has a previously resolved version, use it as currentVersion so that
	// IsValidReleaseVersion can detect y-stream downgrades.
	var currentVersion *semver.Version
	if currentStatusVersion != "" {
		parsed, parseErr := semver.Parse(currentStatusVersion)
		if parseErr == nil {
			currentVersion = &parsed
		}
	}

	if err = supportedversion.IsValidReleaseVersion(
		&nodeClassVersion,
		currentVersion,
		&maxSupportedVersion,
		&minSupportedVersion,
		hostedCluster.Spec.Networking.NetworkType,
		hostedCluster.Spec.Platform.Type,
	); err != nil {
		return fmt.Errorf("failed to validate if version %q is valid: %w", version, err)
	}

	return nil
}

// resolveReleaseImage resolves a version string to a release image via Cincinnati.
// When version is empty, it returns the control plane's release image.
func (r *KarpenterIgnitionReconciler) resolveReleaseImage(
	ctx context.Context,
	hcp *hyperv1.HostedControlPlane,
	version string,
) (string, error) {
	if version == "" {
		return hcp.Spec.ReleaseImage, nil
	}

	resolved, err := r.VersionResolver.Resolve(ctx, version, hcp.Spec.Channel)
	if err != nil {
		return "", fmt.Errorf("failed to resolve version %q: %w", version, err)
	}

	return resolved, nil
}

// detectVersionSkew checks whether the NodeClass version falls outside the supported skew policy
// relative to the control plane version. Returns nil when version is empty (nodes use the control
// plane image) or when the skew is within policy.
func detectVersionSkew(hostedCluster *hyperv1.HostedCluster, version string) error {
	if version == "" {
		return nil
	}

	nodeClassVersion, err := semver.Parse(version)
	if err != nil {
		return fmt.Errorf("failed to parse OpenshiftEC2NodeClass version %q: %w", version, err)
	}

	hostedClusterVersion, err := semver.Parse(hostedCluster.Status.Version.Desired.Version)
	if err != nil {
		return fmt.Errorf("failed to parse HostedCluster version %q: %w", hostedCluster.Status.Version.Desired.Version, err)
	}

	return supportedversion.ValidateVersionSkew(&hostedClusterVersion, &nodeClassVersion)
}

// reconcileKubeletConfigMap creates, updates, or deletes the per-NodeClass KubeletConfig ConfigMap
// in the HCP namespace based on whether the nodeclass has KubeletConfig set.
func (r *KarpenterIgnitionReconciler) reconcileKubeletConfigMap(
	ctx context.Context,
	hcp *hyperv1.HostedControlPlane,
	nodeClass ignitionNodeClass,
) error {
	if nodeClass.Kind() != nodeClassKindOpenshiftEC2 {
		return nil
	}

	openshiftEC2NodeClass := nodeClass.(*openshiftEC2NodeClass).OpenshiftEC2NodeClass
	configMapName := karpenterutil.KarpenterNodeClassKubeletConfigName(nodeClass.Name())
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: hcp.Namespace,
		},
	}

	// Delete the configmap if there are no kubelet settings on the nodeclass
	if openshiftEC2NodeClass.Spec.Kubelet.IsZero() {
		if _, err := k8sutil.DeleteIfNeeded(ctx, r.ManagementClient, cm); err != nil {
			return fmt.Errorf("failed to delete kubelet config configmap %s: %w", configMapName, err)
		}
		return nil
	}

	// Convert user KubeletConfiguration to a plain map (via JSON round-trip to honor json tags
	// and omitempty)
	nodeClassKubeletRaw, err := json.Marshal(openshiftEC2NodeClass.Spec.Kubelet)
	if err != nil {
		return fmt.Errorf("failed to marshal user kubelet config: %w", err)
	}
	var nodeClassKubeletMap map[string]interface{}
	if err := json.Unmarshal(nodeClassKubeletRaw, &nodeClassKubeletMap); err != nil {
		return fmt.Errorf("failed to unmarshal user kubelet config: %w", err)
	}

	// Merge nodeclass with the taint base, our taints go in last so they always win —
	// registerWithTaints is not currently user-settable but this ordering ensures our taint
	// can never be accidentally overridden
	merged := mergeKubeletConfigMaps(nodeClassKubeletMap, karpenterutil.KarpenterBaseTaintMap())
	manifest, err := kubeletConfigManifest(configMapName, merged)
	if err != nil {
		return fmt.Errorf("failed to generate kubelet config manifest: %w", err)
	}

	_, err = r.CreateOrUpdate(ctx, r.ManagementClient, cm, func() error {
		if cm.Labels == nil {
			cm.Labels = map[string]string{}
		}
		cm.Labels[karpenterutil.KarpenterNodeClassKubeletConfigLabel] = "true"
		cm.Data = map[string]string{
			"config": manifest,
		}
		return nil
	})
	return err
}

// reconcileTaintConfigMap ensures the set-karpenter-taint ConfigMap exists in the HCP namespace.
// Ignition references this ConfigMap when generating node bootstrap config so Karpenter nodes
// register with the not-ready taint until Karpenter clears it.
func (r *KarpenterIgnitionReconciler) reconcileTaintConfigMap(ctx context.Context, hcp *hyperv1.HostedControlPlane) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      karpenterutil.KarpenterTaintConfigMapName,
			Namespace: hcp.Namespace,
		},
	}
	_, err := r.CreateOrUpdate(ctx, r.ManagementClient, cm, func() error {
		manifest, err := karpenterutil.KarpenterTaintConfigManifest()
		if err != nil {
			return fmt.Errorf("failed to generate taint config manifest: %w", err)
		}
		cm.Data = map[string]string{"config": manifest}
		return nil
	})
	return err
}

// mergeKubeletConfigMaps merges two kubeletConfig maps. Base keys are included unless
// overlay defines the same key, in which case overlay wins. Callers should pass our
// required fields (e.g. taint base) as overlay so they are never clobbered by user config.
func mergeKubeletConfigMaps(base, overlay map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(base)+len(overlay))
	for k, v := range base {
		result[k] = v
	}
	for k, v := range overlay {
		result[k] = v
	}
	return result
}

// kubeletConfigManifest serializes a pre-merged kubeletConfig map into a KubeletConfig CR YAML
// string suitable for storage in a ConfigMap "config" key.
func kubeletConfigManifest(name string, kubeletConfig map[string]interface{}) (string, error) {
	cr := map[string]interface{}{
		"apiVersion": "machineconfiguration.openshift.io/v1",
		"kind":       "KubeletConfig",
		"metadata":   map[string]interface{}{"name": name},
		"spec":       map[string]interface{}{"kubeletConfig": kubeletConfig},
	}
	out, err := yaml.Marshal(cr)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(out), "\n"), nil
}

// buildConfigGenerator creates a ConfigGenerator for the in-memory NodePool
func (r *KarpenterIgnitionReconciler) buildConfigGenerator(
	ctx context.Context,
	hostedCluster *hyperv1.HostedCluster,
	np *hyperv1.NodePool,
	controlPlaneNamespace string,
) (*nodepool.ConfigGenerator, error) {
	pullSecret := common.PullSecret(controlPlaneNamespace)
	if err := r.ManagementClient.Get(ctx, client.ObjectKeyFromObject(pullSecret), pullSecret); err != nil {
		return nil, fmt.Errorf("failed to get pull secret: %w", err)
	}

	pullSecretBytes, ok := pullSecret.Data[corev1.DockerConfigJsonKey]
	if !ok {
		return nil, fmt.Errorf("expected %s key in pull secret", corev1.DockerConfigJsonKey)
	}

	releaseImage, err := r.ReleaseProvider.Lookup(ctx, np.Spec.Release.Image, pullSecretBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup release image: %w", err)
	}

	haProxyImage, ok := releaseImage.ComponentImages()[haproxy.HAProxyRouterImageName]
	if !ok {
		return nil, fmt.Errorf("release image doesn't have %s image", haproxy.HAProxyRouterImageName)
	}

	haproxyClient := haproxy.HAProxy{
		Client:                  r.ManagementClient,
		HAProxyImage:            haProxyImage,
		HypershiftOperatorImage: r.HypershiftOperatorImage,
		ReleaseProvider:         r.ReleaseProvider,
		ImageMetadataProvider:   r.ImageMetadataProvider,
	}
	haproxyRawConfig, err := haproxyClient.GenerateHAProxyRawConfig(ctx, hostedCluster, controlPlaneNamespace)
	if err != nil {
		return nil, fmt.Errorf("failed to generate HAProxy raw config: %w", err)
	}

	osStreamsEnabled := featuregate.Gate().Enabled(featuregate.OSStreams)
	resolvedRHELStream, err := nodepool.GetRHELStreamForBootImage(ctx, r.ManagementClient, np, releaseImage, osStreamsEnabled)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve RHEL stream for boot image: %w", err)
	}
	return nodepool.NewConfigGenerator(ctx, r.ManagementClient, hostedCluster, np, releaseImage, haproxyRawConfig, controlPlaneNamespace, resolvedRHELStream)
}

// hostedClusterFromHCP creates a barebones in-memory HostedCluster from a HostedControlPlane.
// Note that the Namespace field is set to the HCP namespace rather than the original HC namespace.
// This ensures object lookups the configGenerator does internally which reference the HostedCluster object have necessary permissions since the operator is only allowed to read from the HCP namespace.
// This works since these objects are mirrored by hypershift-operator in both namespaces.
// 1. pullSecret lookup in https://github.com/openshift/hypershift/blob/825484eb33d14b4ab849b428d134582320655fcf/hypershift-operator/controllers/nodepool/nodepool_controller.go#L958
// 2. additionalTrustBundle lookup in https://github.com/openshift/hypershift/blob/825484eb33d14b4ab849b428d134582320655fcf/hypershift-operator/controllers/nodepool/nodepool_controller.go#L985
func hostedClusterFromHCP(hcp *hyperv1.HostedControlPlane, ignitionEndpoint string) (*hyperv1.HostedCluster, error) {
	if hcp == nil {
		return nil, fmt.Errorf("cannot create HostedCluster from nil HostedControlPlane object")
	}
	hc := &hyperv1.HostedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        hcp.Name,
			Namespace:   hcp.Namespace,
			Annotations: hcp.Annotations,
			Labels:      hcp.Labels,
		},
		Spec: hyperv1.HostedClusterSpec{
			Release: hyperv1.Release{
				Image: hcp.Spec.ReleaseImage,
			},
			ClusterID:             hcp.Spec.ClusterID,
			InfraID:               hcp.Spec.InfraID,
			Platform:              hcp.Spec.Platform,
			Networking:            hcp.Spec.Networking,
			PullSecret:            hcp.Spec.PullSecret,
			Services:              hcp.Spec.Services,
			Configuration:         hcp.Spec.Configuration,
			AdditionalTrustBundle: hcp.Spec.AdditionalTrustBundle,
			ImageContentSources:   hcp.Spec.ImageContentSources,
			Capabilities:          hcp.Spec.Capabilities,
			AutoNode:              hcp.Spec.AutoNode,
		},
		Status: hyperv1.HostedClusterStatus{
			IgnitionEndpoint: ignitionEndpoint,
			Version: &hyperv1.ClusterVersionStatus{
				Desired: configv1.Release{},
			},
		},
	}

	if hcp.Status.VersionStatus != nil {
		hc.Status.Version.Desired.Version = hcp.Status.VersionStatus.Desired.Version
		hc.Status.Version.History = hcp.Status.VersionStatus.History
	}

	if hcp.Spec.ControlPlaneReleaseImage != nil {
		hc.Spec.ControlPlaneRelease = &hyperv1.Release{
			Image: *hcp.Spec.ControlPlaneReleaseImage,
		}
	}

	if hcp.Status.KubeConfig != nil {
		hc.Status.KubeConfig = &corev1.LocalObjectReference{
			Name: hcp.Status.KubeConfig.Name,
		}
	}

	return hc, nil
}

func (r *KarpenterIgnitionReconciler) updateConfigVersionAnnotation(ctx context.Context, nodeClass ignitionNodeClass, newVersion string) error {
	original := nodeClass.GetClientObject().DeepCopyObject()
	annotations := nodeClass.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[nodeClassConfigVersionAnnotation(nodeClass)] = newVersion
	nodeClass.SetAnnotations(annotations)
	if err := r.GuestClient.Patch(ctx, nodeClass.GetClientObject(), client.MergeFromWithOptions(original.(client.Object), client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to update config version annotation on %s: %w", nodeClass.Kind(), err)
	}
	return nil
}

func (r *KarpenterIgnitionReconciler) updateVersionStatus(ctx context.Context, nodeClass ignitionNodeClass, resolvedImage string, resolvedVersion string, resolveErr error) error {
	if !nodeClass.SupportsVersionStatus() {
		return nil
	}

	openshiftEC2NodeClass := nodeClass.(*openshiftEC2NodeClass).OpenshiftEC2NodeClass
	original := openshiftEC2NodeClass.DeepCopy()
	openshiftEC2NodeClass.Status.ReleaseImage = resolvedImage
	openshiftEC2NodeClass.Status.Version = resolvedVersion

	condition := metav1.Condition{
		Type:               hyperkarpenterv1.ConditionTypeVersionResolved,
		ObservedGeneration: openshiftEC2NodeClass.Generation,
		LastTransitionTime: metav1.Now(),
	}

	if openshiftEC2NodeClass.Spec.Version == "" {
		condition.Status = metav1.ConditionTrue
		condition.Reason = hyperkarpenterv1.ConditionReasonVersionNotSpecified
		condition.Message = "No version specified, using control plane release image"
	} else if resolveErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Reason = hyperkarpenterv1.ConditionReasonResolutionFailed
		condition.Message = fmt.Sprintf("Failed to resolve version %q: %v", openshiftEC2NodeClass.Spec.Version, resolveErr)
	} else {
		condition.Status = metav1.ConditionTrue
		condition.Reason = hyperkarpenterv1.ConditionReasonVersionResolved
		condition.Message = fmt.Sprintf("Version %q resolved to %s", openshiftEC2NodeClass.Spec.Version, resolvedImage)
	}

	conditionChanged := meta.SetStatusCondition(&openshiftEC2NodeClass.Status.Conditions, condition)

	releaseImageChanged := original.Status.ReleaseImage != resolvedImage
	versionChanged := original.Status.Version != resolvedVersion
	if conditionChanged || releaseImageChanged || versionChanged {
		if err := r.GuestClient.Status().Patch(ctx, openshiftEC2NodeClass, client.MergeFrom(original)); err != nil {
			return fmt.Errorf("failed to update version status on OpenshiftEC2NodeClass: %w", err)
		}
	}

	return nil
}

// setVersionSkewCondition sets the SupportedVersionSkew condition on the in-memory NodeClass
// without patching. The condition is persisted when updateVersionStatus patches status.
func setVersionSkewCondition(nodeClass ignitionNodeClass, skewErr error) {
	if !nodeClass.SupportsVersionStatus() {
		return
	}

	openshiftEC2NodeClass := nodeClass.(*openshiftEC2NodeClass).OpenshiftEC2NodeClass
	condition := metav1.Condition{
		Type:               hyperkarpenterv1.ConditionTypeSupportedVersionSkew,
		ObservedGeneration: openshiftEC2NodeClass.Generation,
		LastTransitionTime: metav1.Now(),
	}

	if openshiftEC2NodeClass.Spec.Version == "" {
		condition.Status = metav1.ConditionTrue
		condition.Reason = hyperkarpenterv1.ConditionReasonVersionNotSpecified
		condition.Message = "No version specified, nodes use the control plane release image"
	} else if skewErr != nil {
		condition.Status = metav1.ConditionFalse
		condition.Reason = hyperkarpenterv1.ConditionReasonUnsupportedSkew
		condition.Message = skewErr.Error()
	} else {
		condition.Status = metav1.ConditionTrue
		condition.Reason = hyperkarpenterv1.ConditionReasonAsExpected
		condition.Message = fmt.Sprintf("Version %q is within supported skew", openshiftEC2NodeClass.Spec.Version)
	}

	meta.SetStatusCondition(&openshiftEC2NodeClass.Status.Conditions, condition)
}

// mapToOpenshiftEC2NodeClasses maps HCP events to all OpenshiftEC2NodeClass reconcile requests.
func (r *KarpenterIgnitionReconciler) mapToOpenshiftEC2NodeClasses(ctx context.Context, obj client.Object) []reconcile.Request {
	nodeClassList := &hyperkarpenterv1.OpenshiftEC2NodeClassList{}
	if err := r.GuestClient.List(ctx, nodeClassList); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to list OpenshiftEC2NodeClasses")
		return nil
	}

	requests := make([]reconcile.Request, 0, len(nodeClassList.Items))
	for _, nc := range nodeClassList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&nc),
		})
	}

	return requests
}

func (r *KarpenterIgnitionReconciler) mapToAKSNodeClasses(ctx context.Context, obj client.Object) []reconcile.Request {
	nodeClassList := &azurekarpenterv1beta1.AKSNodeClassList{}
	if err := r.GuestClient.List(ctx, nodeClassList); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to list AKSNodeClasses")
		return nil
	}

	requests := make([]reconcile.Request, 0, len(nodeClassList.Items))
	for _, nc := range nodeClassList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&nc),
		})
	}

	return requests
}

// hcpPredicate filters HCP events to only watch HCPs in our namespace.
func (r *KarpenterIgnitionReconciler) hcpPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == r.Namespace
	})
}
