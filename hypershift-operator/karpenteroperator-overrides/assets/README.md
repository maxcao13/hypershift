# Karpenter Operator image pin

Set `image` in `overrides.yaml` to the full pullspec of an ART-promoted HCP
Karpenter Operator build, including its digest. Commit the digest bump to
HyperShift and run the AutoNode presubmits before merging.

The file is embedded in the HyperShift Operator at build time. When
`--enable-standalone-karpenter-operator` and the HO `KarpenterOperator` feature
gate enable the standalone path, image selection uses this order:

1. The HostedCluster `hypershift.openshift.io/karpenter-operator-image` annotation.
2. The image pinned in this file.
3. The `karpenter-operator` image from the hosted cluster's release payload.

An absent file or empty image uses the payload. The embedded operator path is
unchanged. Keep this README when removing `overrides.yaml` so the embedded
directory remains present.

A digest bump rolls the Karpenter Operator Deployment across existing hosted
clusters using the standalone path, except those with an annotation override.
It does not change NodePool ignition inputs or their config hashes.
