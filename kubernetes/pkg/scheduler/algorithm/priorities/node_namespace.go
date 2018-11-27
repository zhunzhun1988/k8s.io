/*
Copyright 2016 The Kubernetes Authors.

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

package priorities

import (
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

const (
	criticalPodAnnotation = "scheduler.alpha.kubernetes.io/critical-pod"
	createdByAnnotation   = "kubernetes.io/created-by"
)

type NameSpacePriority struct {
	serviceLister    algorithm.ServiceLister
	controllerLister algorithm.ControllerLister
	replicaSetLister algorithm.ReplicaSetLister
}

func NewNameSpacePriority(
	serviceLister algorithm.ServiceLister,
	controllerLister algorithm.ControllerLister,
	replicaSetLister algorithm.ReplicaSetLister) algorithm.PriorityFunction {
	nameSpacePriority := &NameSpacePriority{
		serviceLister:    serviceLister,
		controllerLister: controllerLister,
		replicaSetLister: replicaSetLister,
	}
	return nameSpacePriority.CalculateNodeNameSpacePriority
}

// Returns selectors of services, RCs and RSs matching the given pod.

func (nsp *NameSpacePriority) getSelectors(pod *v1.Pod) []labels.Selector {
	selectors := make([]labels.Selector, 0, 3)
	if services, err := nsp.serviceLister.GetPodServices(pod); err == nil {
		for _, service := range services {
			selectors = append(selectors, labels.SelectorFromSet(service.Spec.Selector))
		}
	}
	if rcs, err := nsp.controllerLister.GetPodControllers(pod); err == nil {
		for _, rc := range rcs {
			selectors = append(selectors, labels.SelectorFromSet(rc.Spec.Selector))
		}
	}
	if rss, err := nsp.replicaSetLister.GetPodReplicaSets(pod); err == nil {
		for _, rs := range rss {
			if selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector); err == nil {
				selectors = append(selectors, selector)
			}
		}
	}
	return selectors
}

func isCriticalPod(pod *v1.Pod) bool {
	_, found := pod.ObjectMeta.Annotations[criticalPodAnnotation]
	return found
}

func creatorRefKind(pod *v1.Pod) (string, error) {
	if pod != nil && len(pod.OwnerReferences) > 0 {
		if len(pod.OwnerReferences) == 1 {
			return string(pod.OwnerReferences[0].Kind), nil
		} else {
			for _, or := range pod.OwnerReferences {
				if or.Kind == "ReplicationController" || or.Kind == "ReplicaSet" || or.Kind == "StatefulSet" || or.Kind == "DaemonSet" || or.Kind == "Job" {
					return string(or.Kind), nil
				}
			}
			return string(pod.OwnerReferences[0].Kind), nil
		}
	}
	return "", nil
}

// isMirrorPod checks whether the pod is a mirror pod.
func isMirrorPod(pod *v1.Pod) bool {
	_, found := pod.ObjectMeta.Annotations[types.ConfigMirrorAnnotationKey]
	return found
}

func isPodShouldSkip(pod *v1.Pod) bool {
	creatorRef, err := creatorRefKind(pod)
	if err != nil {
		glog.Errorf("pod %s:%s CreatorRefKind error %v", pod.Namespace, pod.Name, err)
		return false
	}
	if isMirrorPod(pod) || creatorRef == "DaemonSet" || creatorRef == "Job" || isCriticalPod(pod) {
		return true
	}
	return false
}

// CalculateNodeNameSpacePriority prioritizes nodes according to the pods' namespace running on the node
// the more same namespace pods run the higher score the node gets.
func (nsp *NameSpacePriority) CalculateNodeNameSpacePriority(pod *v1.Pod, nodeNameToInfo map[string]*schedulercache.NodeInfo, nodes []*v1.Node) (schedulerapi.HostPriorityList, error) {
	var count int
	var skipPodCount int
	selectors := nsp.getSelectors(pod)
	result := []schedulerapi.HostPriority{}
	for _, node := range nodes {
		count = 0
		skipPodCount = 0
		matches := false
		for _, nodePod := range nodeNameToInfo[node.Name].Pods() {
			if nodePod.Namespace == "kube-system" || isPodShouldSkip(nodePod) {
				skipPodCount++
				continue
			}
			for _, selector := range selectors {
				if selector.Matches(labels.Set(nodePod.ObjectMeta.Labels)) {
					matches = true
					glog.V(4).Infof("skipping node %s because pod %s:%s", node.Name, nodePod.Namespace, nodePod.Name)
					break
				}
			}

			if pod.Namespace == nodePod.Namespace {
				count++
			}
		}
		fScore := float64(0)

		// has an another pod on the node belong to the same rc, rs or service
		if matches == false {
			var nodePodsCount int
			nodePodsCount = len(nodeNameToInfo[node.Name].Pods()) - skipPodCount

			fScore = 10 * (float64(count+1) / float64(nodePodsCount+1))
		}

		result = append(result, schedulerapi.HostPriority{Host: node.Name, Score: int(fScore)})
	}
	return result, nil
}

/*func (nsp *NameSpacePriority) ComputeNodeNameSpacePriorityMap(pod *api.Pod, meta interface{}, nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	node := nodeInfo.Node()
	if node == nil {
		return schedulerapi.HostPriority{}, fmt.Errorf("node not found")
	}

	count := 0
	systemPodCount := 0
	for _, nodePod := range nodeInfo.Pods() {
		if pod.Namespace == nodePod.Namespace {
			count++
		}
		if nodePod.Namespace == "kube-system" {
			systemPodCount++
		}
	}
	fScore := float64(0)
	var nodePodsCount int
	nodePodsCount = len(nodeInfo.Pods()) - systemPodCount

	fScore = 10 * (float64(count+1) / float64(nodePodsCount+1))

	return schedulerapi.HostPriority{
		Host:  node.Name,
		Score: int(fScore),
	}, nil
}*/
