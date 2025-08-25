/*
Copyright 2018 The Kubernetes Authors.

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

package recommendation

import (
	"fmt"

	core "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/limitrange"
	resourcehelpers "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/resources"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

// Provider gets current recommendation, annotations and vpaName for the given pod.
type Provider interface {
	GetContainersResourcesForPod(pod *core.Pod, vpa *vpa_types.VerticalPodAutoscaler) ([]vpa_api_util.ContainerResources, vpa_api_util.ContainerToAnnotationsMap, error)
}

type recommendationProvider struct {
	limitsRangeCalculator   limitrange.LimitRangeCalculator
	recommendationProcessor vpa_api_util.RecommendationProcessor
}

// NewProvider constructs the recommendation provider that can be used to determine recommendations for pods.
func NewProvider(calculator limitrange.LimitRangeCalculator,
	recommendationProcessor vpa_api_util.RecommendationProcessor) Provider {
	return &recommendationProvider{
		limitsRangeCalculator:   calculator,
		recommendationProcessor: recommendationProcessor,
	}
}

// GetContainersResources returns the recommended resources for each container in the given pod in the same order they are specified in the pod.Spec.
// If addAll is set to true, containers w/o a recommendation are also added to the list (and their non-recommended requests and limits will always be preserved if present),
// otherwise they're skipped (default behaviour).
// MM custom code
// Also includes istio-proxy init containers when they have recommendations.
// MM End custom code
func GetContainersResources(pod *core.Pod, vpaResourcePolicy *vpa_types.PodResourcePolicy, podRecommendation vpa_types.RecommendedPodResources, limitRange *core.LimitRangeItem,
	addAll bool, annotations vpa_api_util.ContainerToAnnotationsMap) []vpa_api_util.ContainerResources {

	// MM custom code
	// Count istio-proxy init containers with recommendations
	istioProxyHasRecommendation := false
	for _, initContainer := range pod.Spec.InitContainers {
		if initContainer.Name == "istio-proxy" {
			recommendation := vpa_api_util.GetRecommendationForContainer(initContainer.Name, &podRecommendation)
			if recommendation != nil {
				istioProxyHasRecommendation = true
				break
			}
		}
	}

	// Calculate total resources needed (regular containers + istio-proxy if has recommendation)
	totalContainers := len(pod.Spec.Containers)
	if istioProxyHasRecommendation {
		totalContainers++
	}

	resources := make([]vpa_api_util.ContainerResources, totalContainers)
	// MM End custom code
	resourceIndex := 0

	// Process regular containers
	for _, container := range pod.Spec.Containers {
		containerRequests, containerLimits := resourcehelpers.ContainerRequestsAndLimits(container.Name, pod)
		recommendation := vpa_api_util.GetRecommendationForContainer(container.Name, &podRecommendation)
		if recommendation == nil {
			if !addAll {
				klog.V(2).InfoS("No recommendation found for container, skipping", "container", container.Name)
				continue
			}
			klog.V(2).InfoS("No match found for container, using Pod request", "container", container.Name)
			resources[resourceIndex].Requests = containerRequests
		} else {
			resources[resourceIndex].Requests = recommendation.Target
		}
		defaultLimit := core.ResourceList{}
		if limitRange != nil {
			defaultLimit = limitRange.Default
		}
		containerControlledValues := vpa_api_util.GetContainerControlledValues(container.Name, vpaResourcePolicy)
		if containerControlledValues == vpa_types.ContainerControlledValuesRequestsAndLimits {
			proportionalLimits, limitAnnotations := vpa_api_util.GetProportionalLimit(containerLimits, containerRequests, resources[resourceIndex].Requests, defaultLimit)
			if proportionalLimits != nil {
				resources[resourceIndex].Limits = proportionalLimits
				if len(limitAnnotations) > 0 {
					annotations[container.Name] = append(annotations[container.Name], limitAnnotations...)
				}
			}
		}
		// If the recommendation only contains CPU or Memory (if the VPA was configured this way), we need to make sure we "backfill" the other.
		// Only do this when the addAll flag is true.
		if addAll {
			if resources[resourceIndex].Requests == nil {
				resources[resourceIndex].Requests = core.ResourceList{}
			}
			if resources[resourceIndex].Limits == nil {
				resources[resourceIndex].Limits = core.ResourceList{}
			}

			cpuRequest, hasCpuRequest := containerRequests[core.ResourceCPU]
			if _, ok := resources[resourceIndex].Requests[core.ResourceCPU]; !ok && hasCpuRequest {
				resources[resourceIndex].Requests[core.ResourceCPU] = cpuRequest
			}
			memRequest, hasMemRequest := containerRequests[core.ResourceMemory]
			if _, ok := resources[resourceIndex].Requests[core.ResourceMemory]; !ok && hasMemRequest {
				resources[resourceIndex].Requests[core.ResourceMemory] = memRequest
			}
			cpuLimit, hasCpuLimit := containerLimits[core.ResourceCPU]
			if _, ok := resources[resourceIndex].Limits[core.ResourceCPU]; !ok && hasCpuLimit {
				resources[resourceIndex].Limits[core.ResourceCPU] = cpuLimit
			}
			memLimit, hasMemLimit := containerLimits[core.ResourceMemory]
			if _, ok := resources[resourceIndex].Limits[core.ResourceMemory]; !ok && hasMemLimit {
				resources[resourceIndex].Limits[core.ResourceMemory] = memLimit
			}
		}
		resourceIndex++
	}

	// MM custom code
	// Process istio-proxy init container if it has recommendations
	if istioProxyHasRecommendation {
		for _, initContainer := range pod.Spec.InitContainers {
			if initContainer.Name == "istio-proxy" {
				containerRequests, containerLimits := resourcehelpers.InitContainerRequestsAndLimits(initContainer.Name, pod)
				recommendation := vpa_api_util.GetRecommendationForContainer(initContainer.Name, &podRecommendation)

				klog.V(2).InfoS("Processing istio-proxy init container with recommendation", "container", initContainer.Name)
				resources[resourceIndex].Requests = recommendation.Target

				defaultLimit := core.ResourceList{}
				if limitRange != nil {
					defaultLimit = limitRange.Default
				}
				containerControlledValues := vpa_api_util.GetContainerControlledValues(initContainer.Name, vpaResourcePolicy)
				if containerControlledValues == vpa_types.ContainerControlledValuesRequestsAndLimits {
					klog.V(2).InfoS("Calculating proportional limits for istio-proxy", 
						"container", initContainer.Name,
						"originalLimits", containerLimits,
						"originalRequests", containerRequests,
						"newRequests", resources[resourceIndex].Requests,
						"defaultLimit", defaultLimit)
					proportionalLimits, limitAnnotations := vpa_api_util.GetProportionalLimit(containerLimits, containerRequests, resources[resourceIndex].Requests, defaultLimit)
					klog.V(2).InfoS("GetProportionalLimit result for istio-proxy", 
						"container", initContainer.Name,
						"proportionalLimits", proportionalLimits,
						"limitAnnotations", limitAnnotations)
					if proportionalLimits != nil {
						resources[resourceIndex].Limits = proportionalLimits
						if len(limitAnnotations) > 0 {
							annotations[initContainer.Name] = append(annotations[initContainer.Name], limitAnnotations...)
						}
					}
				}
				// If the recommendation only contains CPU or Memory (if the VPA was configured this way), we need to make sure we "backfill" the other.
				// Only do this when the addAll flag is true.
				if addAll {
					if resources[resourceIndex].Requests == nil {
						resources[resourceIndex].Requests = core.ResourceList{}
					}
					if resources[resourceIndex].Limits == nil {
						resources[resourceIndex].Limits = core.ResourceList{}
					}

					cpuRequest, hasCpuRequest := containerRequests[core.ResourceCPU]
					if _, ok := resources[resourceIndex].Requests[core.ResourceCPU]; !ok && hasCpuRequest {
						resources[resourceIndex].Requests[core.ResourceCPU] = cpuRequest
					}
					memRequest, hasMemRequest := containerRequests[core.ResourceMemory]
					if _, ok := resources[resourceIndex].Requests[core.ResourceMemory]; !ok && hasMemRequest {
						resources[resourceIndex].Requests[core.ResourceMemory] = memRequest
					}
					cpuLimit, hasCpuLimit := containerLimits[core.ResourceCPU]
					if _, ok := resources[resourceIndex].Limits[core.ResourceCPU]; !ok && hasCpuLimit {
						resources[resourceIndex].Limits[core.ResourceCPU] = cpuLimit
					}
					memLimit, hasMemLimit := containerLimits[core.ResourceMemory]
					if _, ok := resources[resourceIndex].Limits[core.ResourceMemory]; !ok && hasMemLimit {
						resources[resourceIndex].Limits[core.ResourceMemory] = memLimit
					}
				}
				break
			}
		}
	}
	// MM End custom code

	return resources
}

// GetContainersResourcesForPod returns recommended request for a given pod and associated annotations.
// The returned slice corresponds 1-1 to containers in the Pod.
func (p *recommendationProvider) GetContainersResourcesForPod(pod *core.Pod, vpa *vpa_types.VerticalPodAutoscaler) ([]vpa_api_util.ContainerResources, vpa_api_util.ContainerToAnnotationsMap, error) {
	if vpa == nil || pod == nil {
		klog.V(2).InfoS("Can't calculate recommendations, one of VPA or Pod is nil", "vpa", vpa, "pod", pod)
		return nil, nil, nil
	}
	klog.V(2).InfoS("Updating requirements for pod", "pod", klog.KObj(pod))

	var annotations vpa_api_util.ContainerToAnnotationsMap
	recommendedPodResources := &vpa_types.RecommendedPodResources{}

	if vpa.Status.Recommendation != nil {
		var err error
		recommendedPodResources, annotations, err = p.recommendationProcessor.Apply(vpa, pod)
		if err != nil {
			klog.V(2).InfoS("Cannot process recommendation for pod", "pod", klog.KObj(pod))
			return nil, annotations, err
		}
	}
	containerLimitRange, err := p.limitsRangeCalculator.GetContainerLimitRangeItem(pod.Namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("error getting containerLimitRange: %s", err)
	}
	var resourcePolicy *vpa_types.PodResourcePolicy
	if vpa.Spec.UpdatePolicy == nil || vpa.Spec.UpdatePolicy.UpdateMode == nil || *vpa.Spec.UpdatePolicy.UpdateMode != vpa_types.UpdateModeOff {
		resourcePolicy = vpa.Spec.ResourcePolicy
	}
	containerResources := GetContainersResources(pod, resourcePolicy, *recommendedPodResources, containerLimitRange, false, annotations)

	// Ensure that we are not propagating empty resource key if any.
	for _, resource := range containerResources {
		if resource.RemoveEmptyResourceKeyIfAny() {
			klog.InfoS("An empty resource key was found and purged", "pod", klog.KObj(pod), "vpa", klog.KObj(vpa))
		}
	}

	return containerResources, annotations, nil
}
