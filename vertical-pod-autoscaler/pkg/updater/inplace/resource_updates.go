/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package inplace

import (
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	resource_admission "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource/pod/patch"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource/pod/recommendation"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

type resourcesInplaceUpdatesPatchCalculator struct {
	recommendationProvider recommendation.Provider
}

// NewResourceInPlaceUpdatesCalculator returns a calculator for
// in-place resource update patches.
func NewResourceInPlaceUpdatesCalculator(recommendationProvider recommendation.Provider) patch.Calculator {
	return &resourcesInplaceUpdatesPatchCalculator{
		recommendationProvider: recommendationProvider,
	}
}

// PatchResourceTarget returns the resize subresource to apply calculator patches.
func (*resourcesInplaceUpdatesPatchCalculator) PatchResourceTarget() patch.PatchResourceTarget {
	return patch.Resize
}

// CalculatePatches calculates a JSON patch from a VPA's recommendation to send to the pod "resize" subresource as an in-place resize.
func (c *resourcesInplaceUpdatesPatchCalculator) CalculatePatches(pod *core.Pod, vpa *vpa_types.VerticalPodAutoscaler) ([]resource_admission.PatchRecord, error) {
	result := []resource_admission.PatchRecord{}

	containersResources, _, err := c.recommendationProvider.GetContainersResourcesForPod(pod, vpa)
	if err != nil {
		return []resource_admission.PatchRecord{}, fmt.Errorf("failed to calculate resource patch for pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}

	// MM custom code
	// Handle both regular containers and native sidecar init containers (like istio-proxy)
	regularContainerCount := len(pod.Spec.Containers)

	klog.V(2).InfoS("Processing containers for in-place update",
		"pod", klog.KObj(pod),
		"totalContainerResources", len(containersResources),
		"regularContainerCount", regularContainerCount,
		"initContainerCount", len(pod.Spec.InitContainers))

	for i, containerResources := range containersResources {
		var newPatches []resource_admission.PatchRecord

		klog.V(2).InfoS("Processing container resource",
			"pod", klog.KObj(pod),
			"index", i,
			"containerResources", containerResources)

		if i < regularContainerCount {
			// Regular container
			klog.V(4).InfoS("Processing regular container for in-place update", "pod", klog.KObj(pod), "containerIndex", i, "container", pod.Spec.Containers[i].Name)
			newPatches = getContainerPatch(pod, i, containerResources)
		} else {
			// Init container - find the istio-proxy container by name
			// Note: containersResources includes regular containers + init containers with recommendations
			// We need to find which init container this resource recommendation is for

			// The container name should be in the containerResources somehow, but we need to determine
			// which init container this is for. Let's find istio-proxy specifically.
			var istioProxyIndex = -1
			for idx, initContainer := range pod.Spec.InitContainers {
				if initContainer.Name == "istio-proxy" && isNativeSidecar(initContainer) {
					istioProxyIndex = idx
					break
				}
			}

			if istioProxyIndex >= 0 {
				klog.V(2).InfoS("Processing istio-proxy native sidecar for in-place update",
					"pod", klog.KObj(pod),
					"istioProxyIndex", istioProxyIndex,
					"resourceIndex", i)
				newPatches = getInitContainerPatch(pod, istioProxyIndex, containerResources)
			} else {
				klog.V(4).InfoS("Skipping - istio-proxy init container not found or not a native sidecar",
					"pod", klog.KObj(pod),
					"resourceIndex", i)
				continue
			}
		}

		klog.V(2).InfoS("Generated patches for container",
			"pod", klog.KObj(pod),
			"containerIndex", i,
			"patchCount", len(newPatches))

		result = append(result, newPatches...)
	}
	// MM end custom code

	return result, nil
}

func getContainerPatch(pod *core.Pod, i int, containerResources vpa_api_util.ContainerResources) []resource_admission.PatchRecord {
	var patches []resource_admission.PatchRecord

	// Add empty resources object if missing.
	if pod.Spec.Containers[i].Resources.Limits == nil &&
		pod.Spec.Containers[i].Resources.Requests == nil {
		patches = append(patches, patch.GetPatchInitializingEmptyResources(i))
	}

	patches = appendPatches(patches, pod.Spec.Containers[i].Resources.Requests, i, containerResources.Requests, "requests")
	patches = appendPatches(patches, pod.Spec.Containers[i].Resources.Limits, i, containerResources.Limits, "limits")

	return patches
}

// MM custom code
// getInitContainerPatch generates patches for init containers using init container-specific patch functions
func getInitContainerPatch(pod *core.Pod, i int, containerResources vpa_api_util.ContainerResources) []resource_admission.PatchRecord {
	var patches []resource_admission.PatchRecord

	// Add empty resources object if missing.
	if pod.Spec.InitContainers[i].Resources.Limits == nil &&
		pod.Spec.InitContainers[i].Resources.Requests == nil {
		patches = append(patches, patch.GetPatchInitializingEmptyInitContainerResources(i))
	}

	patches = appendInitContainerPatches(patches, pod.Spec.InitContainers[i].Resources.Requests, i, containerResources.Requests, "requests")
	patches = appendInitContainerPatches(patches, pod.Spec.InitContainers[i].Resources.Limits, i, containerResources.Limits, "limits")

	return patches
}

// appendInitContainerPatches generates patches for init container resources
func appendInitContainerPatches(patches []resource_admission.PatchRecord, current core.ResourceList, containerIndex int, resources core.ResourceList, fieldName string) []resource_admission.PatchRecord {
	// Add empty object if it's missing and we're about to fill it.
	if current == nil && len(resources) > 0 {
		patches = append(patches, patch.GetPatchInitializingEmptyInitContainerResourcesSubfield(containerIndex, fieldName))
	}
	for resource, request := range resources {
		patches = append(patches, patch.GetAddInitContainerResourceRequirementValuePatch(containerIndex, fieldName, resource, request))
	}
	return patches
}

// MM end custom code

// isNativeSidecar checks if an init container is configured as a native sidecar
// Native sidecars have restartPolicy set to "Always" and support in-place updates
func isNativeSidecar(container core.Container) bool {
	// Native sidecars are init containers with restartPolicy: Always
	// This is the key difference that allows them to be updated in-place
	return container.RestartPolicy != nil && *container.RestartPolicy == core.ContainerRestartPolicyAlways
}

func appendPatches(patches []resource_admission.PatchRecord, current core.ResourceList, containerIndex int, resources core.ResourceList, fieldName string) []resource_admission.PatchRecord {
	// Add empty object if it's missing and we're about to fill it.
	if current == nil && len(resources) > 0 {
		patches = append(patches, patch.GetPatchInitializingEmptyResourcesSubfield(containerIndex, fieldName))
	}
	for resource, request := range resources {
		patches = append(patches, patch.GetAddResourceRequirementValuePatch(containerIndex, fieldName, resource, request))
	}
	return patches
}
