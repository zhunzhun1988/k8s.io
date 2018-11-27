/*
Copyright 2017 The Kubernetes Authors.

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
package rescheduler

import (
	"encoding/json"
	"fmt"
	"time"

	ca_simulator "k8s.io/kubernetes/cmd/kube-rescheduler/simulator"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/rescheduler/metrics"
	"k8s.io/kubernetes/pkg/rescheduler/utils"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"

	"github.com/golang/glog"
)

const (
	criticalPodAnnotation = "scheduler.alpha.kubernetes.io/critical-pod"

	TaintsAnnotationKey string = "scheduler.alpha.kubernetes.io/taints"
)

type CriticalRescheduler struct {
	apiClient           utils.ApiClientInterface
	podsBeingProcessed  *utils.PodSet
	recorder            kube_record.EventRecorder
	predicateChecker    *ca_simulator.PredicateChecker
	podScheduledTimeout time.Duration
	stopChannel         chan struct{}
}

func NewCriticalRescheduler(
	apiClient utils.ApiClientInterface,
	podsBeingProcessed *utils.PodSet,
	recorder kube_record.EventRecorder,
	podScheduledTimeout time.Duration,
	predicateChecker *ca_simulator.PredicateChecker) *CriticalRescheduler {
	return &CriticalRescheduler{
		apiClient:           apiClient,
		podsBeingProcessed:  podsBeingProcessed,
		recorder:            recorder,
		predicateChecker:    predicateChecker,
		podScheduledTimeout: podScheduledTimeout,
		stopChannel:         make(chan struct{}),
	}
}

func getHostpathPVCMap(pvs []*apiv1.PersistentVolume) map[string]bool {
	ret := make(map[string]bool)
	for _, pv := range pvs {
		if pv.Spec.HostPath != nil && pv.Spec.ClaimRef != nil {
			ret[fmt.Sprintf("%s:%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)] = true
		}
	}
	return ret
}
func (crs *CriticalRescheduler) CheckAndReschedule() error {
	allUnschedulablePods, err := crs.apiClient.GetUnschedulablePod(apiv1.NamespaceAll)
	if err != nil {
		return fmt.Errorf("Failed to list unscheduled pods: %v", err)
	}

	criticalPods := filterCriticalPods(allUnschedulablePods, crs.podsBeingProcessed)

	for i, pod := range criticalPods {
		fmt.Printf("CheckAndReschedule criticalPods[%d]: %s:%s\n", i, pod.Namespace, pod.Name)
	}
	if len(criticalPods) > 0 {
		hostpathpvcmap := getHostpathPVCMap(crs.apiClient.ListPVS())
		for _, pod := range criticalPods {
			glog.Infof("Critical pod %s is unschedulable. Trying to find a spot for it.", utils.PodId(pod))
			k8sApp := "unknown"
			if l, found := pod.ObjectMeta.Labels["k8s-app"]; found {
				k8sApp = l
			}
			metrics.UnschedulableCriticalPodsCount.WithLabelValues(k8sApp).Inc()
			nodes := crs.apiClient.ListReadyNodes()

			node := findNodeForPod(crs.apiClient, crs.predicateChecker, nodes, pod, hostpathpvcmap)
			if node == nil {
				glog.Errorf("Pod %s can't be scheduled on any existing node.", utils.PodId(pod))
				crs.recorder.Eventf(pod, apiv1.EventTypeNormal, "PodDoestFitAnyNode",
					"Critical pod %s doesn't fit on any node.", utils.PodId(pod))
				continue
			}
			glog.Infof("Trying to place the pod on node %v", node.Name)

			errPrepare, deletePods := prepareNodeForPod(crs.apiClient, crs.recorder, crs.predicateChecker, node, pod, hostpathpvcmap)

			if errPrepare != nil {
				glog.Warningf("%+v", errPrepare)
			} else {
				crs.podsBeingProcessed.Add(pod)
				go waitForScheduled(crs.apiClient, crs.recorder, crs.podsBeingProcessed, pod, crs.podScheduledTimeout, deletePods)
			}
		}
	}
	return nil
}

func waitForScheduled(client utils.ApiClientInterface, recorder kube_record.EventRecorder,
	podsBeingProcessed *utils.PodSet, pod *apiv1.Pod,
	podScheduledTimeout time.Duration, deletePods []*apiv1.Pod) {
	glog.Infof("Waiting for pod %s to be scheduled", utils.PodId(pod))
	err := wait.Poll(time.Second, podScheduledTimeout, func() (bool, error) {
		p, err := client.GetPod(pod.Namespace, pod.Name)
		if err != nil {
			glog.Warningf("Error while getting pod %s: %v", utils.PodId(pod), err)
			return false, nil
		}
		return p.Spec.NodeName != "", nil
	})
	if err != nil {
		glog.Warningf("Timeout while waiting for pod %s to be scheduled after %v.", utils.PodId(pod), podScheduledTimeout)
	} else {
		glog.Infof("Pod %v was successfully scheduled.", utils.PodId(pod))
		deletePodsStr := make([]string, 0, len(deletePods))
		for _, deletePod := range deletePods {
			deletePodsStr = append(deletePodsStr, utils.PodId(deletePod))
		}
		recorder.Eventf(pod, apiv1.EventTypeNormal, "ScheduledByRescheduler",
			"Scheduler by rescheduler by delete pods[%v].", deletePodsStr)

	}
	podsBeingProcessed.Remove(pod)
}

// copied from Kubernetes 1.5.4
func getTaintsFromNodeAnnotations(annotations map[string]string) ([]apiv1.Taint, error) {
	var taints []apiv1.Taint
	if len(annotations) > 0 && annotations[TaintsAnnotationKey] != "" {
		err := json.Unmarshal([]byte(annotations[TaintsAnnotationKey]), &taints)
		if err != nil {
			return []apiv1.Taint{}, err
		}
	}
	return taints, nil
}

// The caller of this function must remove the taint if this function returns error.
func prepareNodeForPod(client utils.ApiClientInterface, recorder kube_record.EventRecorder,
	predicateChecker *ca_simulator.PredicateChecker, originalNode *apiv1.Node, criticalPod *apiv1.Pod,
	hostpathpvcmap map[string]bool) (err error, deletePods []*apiv1.Pod) {
	// Operate on a copy of the node to ensure pods running on the node will pass CheckPredicates below.
	node, err := copyNode(originalNode)
	if err != nil {
		return fmt.Errorf("Error while copying node: %v", err), nil
	}
	err = utils.AddTaint(client.UpdateNode, originalNode, utils.PodId(criticalPod))
	if err != nil {
		return fmt.Errorf("Error while adding taint: %v", err), nil
	}

	requiredPods, otherPods, err := groupPods(client, node, hostpathpvcmap)
	if err != nil {
		return err, nil
	}

	nodeInfo := schedulercache.NewNodeInfo(requiredPods...)
	nodeInfo.SetNode(node)

	// check whether critical pod still fit
	if err := predicateChecker.CheckPredicates(criticalPod, nodeInfo); err != nil {
		return fmt.Errorf("Pod %s doesn't fit to node %v: %v", utils.PodId(criticalPod), node.Name, err), nil
	}
	requiredPods = append(requiredPods, criticalPod)
	nodeInfo = schedulercache.NewNodeInfo(requiredPods...)
	nodeInfo.SetNode(node)

	for _, p := range otherPods {
		if err := predicateChecker.CheckPredicates(p, nodeInfo); err != nil {
			glog.Infof("Pod %s will be deleted in order to schedule critical pod %s.", utils.PodId(p), utils.PodId(criticalPod))
			recorder.Eventf(p, apiv1.EventTypeNormal, "DeletedByRescheduler",
				"Deleted by rescheduler in order to schedule critical pod %s.", utils.PodId(criticalPod))
			// TODO(piosz): add better support of graceful deletion
			delErr := client.DeletePod(p.Namespace, p.Name, 10)
			if delErr != nil {
				return fmt.Errorf("Failed to delete pod %s: %v", utils.PodId(p), delErr), nil
			} else {
				deletePods = append(deletePods, p)
			}
			metrics.DeletedPodsCount.Inc()
		} else {
			newPods := append(nodeInfo.Pods(), p)
			nodeInfo = schedulercache.NewNodeInfo(newPods...)
			nodeInfo.SetNode(node)
		}
	}
	err = utils.AddTolerations(client.UpdatePod, criticalPod)
	if err != nil {
		return fmt.Errorf("Error while adding toleration: %v", err), nil
	}
	// TODO(piosz): how to reset scheduler backoff?
	return nil, deletePods
}

func copyNode(node *apiv1.Node) (*apiv1.Node, error) {
	copied := node.DeepCopy()
	return copied, nil
}

// Currently the logic choose a random node which satisfies requirements (a critical pod fits there).
// TODO(piosz): add a prioritization to this logic
func findNodeForPod(client utils.ApiClientInterface, predicateChecker *ca_simulator.PredicateChecker,
	nodes []*apiv1.Node, pod *apiv1.Pod, hostpathpvcmap map[string]bool) *apiv1.Node {
	for _, node := range nodes {
		// ignore nodes with taints
		if err := checkTaints(node); err != nil {
			glog.Warningf("Skipping node %v due to %v", node.Name, err)
		}

		requiredPods, _, err := groupPods(client, node, hostpathpvcmap)
		if err != nil {
			glog.Warningf("Skipping node %v due to error: %v", node.Name, err)
			continue
		}

		nodeInfo := schedulercache.NewNodeInfo(requiredPods...)
		nodeInfo.SetNode(node)

		if err := predicateChecker.CheckPredicates(pod, nodeInfo); err == nil {
			return node
		}
	}
	return nil
}

func checkTaints(node *apiv1.Node) error {
	for _, taint := range node.Spec.Taints {
		if taint.Key == utils.CriticalPodAnnotation {
			return fmt.Errorf("CriticalAddonsOnly taint with value: %v", taint.Value)
		}
	}
	return nil
}

// groupPods divides pods running on <node> into those which can't be deleted and the others
func groupPods(client utils.ApiClientInterface, node *apiv1.Node, hostpathpvcmap map[string]bool) ([]*apiv1.Pod, []*apiv1.Pod, error) {
	podsOnNode, err := client.GetPods(apiv1.NamespaceAll, node.Name)
	if err != nil {
		return []*apiv1.Pod{}, []*apiv1.Pod{}, err
	}

	requiredPods := make([]*apiv1.Pod, 0)
	otherPods := make([]*apiv1.Pod, 0)
	for _, pod := range podsOnNode {
		creatorRef, err := utils.CreatorRefKind(pod)
		if err != nil {
			return []*apiv1.Pod{}, []*apiv1.Pod{}, err
		}

		if utils.IsMirrorPod(pod) || creatorRef == "DaemonSet" || isCriticalPod(pod) ||
			creatorRef == "" /*alone pod should not be deleted*/ || isPodUseHostPathPV(pod, hostpathpvcmap) {
			requiredPods = append(requiredPods, pod)
		} else {
			otherPods = append(otherPods, pod)
		}
	}

	return requiredPods, otherPods, nil
}

func isPodUseHostPathPV(pod *apiv1.Pod, hostpathpvcmap map[string]bool) bool {
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			if isHostpath, _ := hostpathpvcmap[fmt.Sprintf("%s:%s", pod.Namespace, pvcSource.ClaimName)]; isHostpath {
				return true
			}
		}
	}

	return false
}

func filterCriticalPods(allPods []*apiv1.Pod, podsBeingProcessed *utils.PodSet) []*apiv1.Pod {
	criticalPods := []*apiv1.Pod{}
	for _, pod := range allPods {
		if isCriticalPod(pod) && !podsBeingProcessed.Has(pod) {
			criticalPods = append(criticalPods, pod)
		}
	}
	return criticalPods
}

func isCriticalPod(pod *apiv1.Pod) bool {
	_, found := pod.ObjectMeta.Annotations[criticalPodAnnotation]
	return found
}
