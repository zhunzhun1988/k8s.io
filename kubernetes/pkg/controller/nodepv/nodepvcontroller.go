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

package nodepv

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	clientv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	v1node "k8s.io/kubernetes/pkg/api/v1/node"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
	"k8s.io/kubernetes/pkg/util/metrics"
)

type volume struct {
	pvName, pvUID string
}

type podInfo struct {
	podName, podNS, podUID string
}

type nodePVTimeOutMap map[string]map[volume]uint

const (
	PVUpdateRetryCount   = 5
	PVUpdateInterval     = 100 * time.Millisecond
	DefaultPVNodeTimeout = 3 * 60
	SyncAllInterval      = 30 * time.Minute
)

type NodePVController struct {
	kubeClient         clientset.Interface
	recorder           record.EventRecorder
	nodeLister         corelisters.NodeLister
	nodeInformerSynced cache.InformerSynced
	pvLister           corelisters.PersistentVolumeLister
	pvInformerSynced   cache.InformerSynced

	runMutex         sync.Mutex
	nodePVMapMutex   sync.Mutex
	nodeSyncMap      map[string]chan struct{}
	nodePVTimeOutMap nodePVTimeOutMap
	stop             <-chan struct{}
}

func StringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	if (a == nil) != (b == nil) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

// NewNodePVController returns a new node pv controller to sync pv hostpath.
func NewNodePVController(
	nodeInformer coreinformers.NodeInformer,
	volumeInformer coreinformers.PersistentVolumeInformer,
	kubeClient clientset.Interface,
	stop <-chan struct{}) *NodePVController {

	eventBroadcaster := record.NewBroadcaster()
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, clientv1.EventSource{Component: "controllermanager"})
	eventBroadcaster.StartLogging(glog.Infof)
	if kubeClient != nil {
		glog.V(0).Infof("Sending events to api server.")
		eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.Core().RESTClient()).Events("")})
	} else {
		glog.V(0).Infof("No api server defined - no events will be sent to API server.")
	}

	if kubeClient != nil && kubeClient.Core().RESTClient().GetRateLimiter() != nil {
		metrics.RegisterMetricAndTrackRateLimiterUsage("nodepv_controller", kubeClient.Core().RESTClient().GetRateLimiter())
	}

	npvc := &NodePVController{
		kubeClient:       kubeClient,
		recorder:         recorder,
		nodeSyncMap:      make(map[string]chan struct{}),
		nodePVTimeOutMap: make(nodePVTimeOutMap),
		stop:             stop,
	}

	nodeEventHandlerFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			newNode := obj.(*v1.Node)
			npvc.syncNode(newNode, false)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newNode := newObj.(*v1.Node)
			oldNode := oldObj.(*v1.Node)
			_, newReadyCondition := v1node.GetNodeCondition(&newNode.Status, v1.NodeReady)
			_, oldReadyCondition := v1node.GetNodeCondition(&oldNode.Status, v1.NodeReady)
			if oldReadyCondition.Status != newReadyCondition.Status {
				npvc.syncNode(newNode, false)
			}

		},
		DeleteFunc: func(originalObj interface{}) {

			node, isNode := originalObj.(*v1.Node)
			if !isNode {
				deletedState, ok := originalObj.(cache.DeletedFinalStateUnknown)
				if !ok {
					glog.Errorf("Received unexpected object: %v", originalObj)
					return
				}
				node, ok = deletedState.Obj.(*v1.Node)
				if !ok {
					glog.Errorf("DeletedFinalStateUnknown contained non-Node object: %v", deletedState.Obj)
					return
				}
			}
			npvc.syncNode(node, true)
		},
	}
	nodeInformer.Informer().AddEventHandler(nodeEventHandlerFuncs)
	npvc.nodeLister = nodeInformer.Lister()
	npvc.nodeInformerSynced = nodeInformer.Informer().HasSynced

	pvEventHandlerFuncs := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			newPV := obj.(*v1.PersistentVolume)
			npvc.syncPV(newPV)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPV := newObj.(*v1.PersistentVolume)
			oldPV := oldObj.(*v1.PersistentVolume)
			oldNeedSync, timeOutOld := isPVNeedSync(oldPV)
			newNeedSync, timeOutNew := isPVNeedSync(newPV)
			if oldNeedSync == newNeedSync {
				if newNeedSync == false {
					return
				} else if timeOutOld == timeOutNew {
					nodesOld := getPVNodes(oldPV)
					nodesNew := getPVNodes(newPV)
					sort.Strings(nodesOld)
					sort.Strings(nodesNew)
					if StringSliceEqual(nodesOld, nodesNew) == true { // nodes not change
						return
					}
				}
			}
			npvc.syncPV(newPV)
		},
		DeleteFunc: func(obj interface{}) {
			var volume *v1.PersistentVolume
			var ok bool
			volume, ok = obj.(*v1.PersistentVolume)
			if !ok {
				if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
					volume, ok = unknown.Obj.(*v1.PersistentVolume)
					if !ok {
						glog.Errorf("Expected PersistentVolume but deleteVolume received %#v", unknown.Obj)
						return
					}
				} else {
					glog.Errorf("Expected PersistentVolume but deleteVolume received %+v", obj)
					return
				}
			}

			if volume == nil {
				return
			}
			npvc.deletePV(volume)
		},
	}
	volumeInformer.Informer().AddEventHandler(pvEventHandlerFuncs)
	npvc.pvLister = volumeInformer.Lister()
	npvc.pvInformerSynced = volumeInformer.Informer().HasSynced
	return npvc
}
func (npvc *NodePVController) syncPV(pv *v1.PersistentVolume) {
	if ok, timeout := isPVNeedSync(pv); ok == true {
		nodes := getPVNodes(pv)
		npvc.nodePVMapMutex.Lock()
		defer npvc.nodePVMapMutex.Unlock()
		for _, node := range nodes {
			nodeMap, exists := npvc.nodePVTimeOutMap[node]
			if exists == false {
				nodeMap = make(map[volume]uint)
			}
			nodeMap[volume{pvName: pv.Name, pvUID: string(pv.UID)}] = timeout
			npvc.nodePVTimeOutMap[node] = nodeMap
		}
	} else {
		npvc.deletePV(pv)
	}
}
func (npvc *NodePVController) isNodeNeedSync(nodeName string) bool {
	npvc.nodePVMapMutex.Lock()
	defer npvc.nodePVMapMutex.Unlock()
	_, exists := npvc.nodePVTimeOutMap[nodeName]
	return exists
}
func (npvc *NodePVController) getNodePVTimeOutMapNodes() []string {
	npvc.nodePVMapMutex.Lock()
	defer npvc.nodePVMapMutex.Unlock()
	ret := make([]string, 0, len(npvc.nodePVTimeOutMap))
	for node, nodeMap := range npvc.nodePVTimeOutMap {
		if len(nodeMap) > 0 {
			ret = append(ret, node)
		}
	}
	return ret
}
func (npvc *NodePVController) deletePV(pv *v1.PersistentVolume) {
	npvc.nodePVMapMutex.Lock()
	defer npvc.nodePVMapMutex.Unlock()
	v := volume{pvName: pv.Name, pvUID: string(pv.UID)}
	for node, nodeMap := range npvc.nodePVTimeOutMap {
		if _, exists := nodeMap[v]; exists == true {
			delete(nodeMap, v)
		}
		if len(nodeMap) == 0 {
			delete(npvc.nodePVTimeOutMap, node)
		}
	}
}
func (npvc *NodePVController) syncNode(node *v1.Node, force bool) {
	if node == nil {
		return
	}
	_, currentReadyCondition := v1node.GetNodeCondition(&node.Status, v1.NodeReady)
	if (force == true || v1.ConditionTrue != currentReadyCondition.Status) && npvc.isNodeNeedSync(node.Name) {
		diedTime := currentReadyCondition.LastTransitionTime.Time
		if force == true { // force == true only because of node was deleted
			diedTime = time.Now()
		}
		npvc.addNodeSyncWorker(node.Name, diedTime)
	} else {
		npvc.deleteNodeSyncWorker(node.Name)
	}
}

func isHostPathPV(pv *v1.PersistentVolume) bool {
	if pv == nil {
		return false
	}
	if pv.Spec.CSI != nil && strings.Contains(strings.ToLower(pv.Spec.CSI.Driver), "hostpath") == true {
		return true
	} else if pv.Spec.HostPath != nil {
		return true
	}
	return false
}

func isPVCanBeRemovedFromNode(pv *v1.PersistentVolume, nodeName string) bool {
	if isHostPathPV(pv) && pv.Annotations != nil &&
		pv.Annotations[xfs.PVHostPathMountPolicyAnn] != xfs.PVHostPathNone {
		nodes := getPVNodes(pv)
		for _, node := range nodes {
			if node == nodeName { // the pv has mount on the node
				return false
			}
		}
	}
	return true
}
func isPVNeedSync(pv *v1.PersistentVolume) (bool, uint) {
	if isHostPathPV(pv) == false || pv.Annotations == nil ||
		pv.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathNone || // default keep
		pv.Annotations[xfs.PVHostPathMountTimeoutAnn] == "" {
		return false, 0
	}
	timeout, err := strconv.Atoi(pv.Annotations[xfs.PVHostPathMountTimeoutAnn])
	if err != nil {
		timeout = DefaultPVNodeTimeout
	}
	return true, uint(timeout)
}

func getPVNodes(pv *v1.PersistentVolume) []string {
	ret := make([]string, 0)
	if pv == nil || pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
		return ret
	}
	mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
	if errUmarshal != nil {
		return ret
	}
	for _, item := range mountList {
		ret = append(ret, item.NodeName)

	}
	return ret
}

func deletePVHistory(pv *v1.PersistentVolume, nodeName string) (changed bool, deletePodInfos []podInfo) {
	if pv == nil || pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
		return false, []podInfo{}
	}
	mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
	if errUmarshal != nil {
		return false, []podInfo{}
	}

	for i, item := range mountList {
		if item.NodeName == nodeName {
			deletePodInfos = getPVMountPodsInfo(item)
			mountList = append(mountList[:i], mountList[i+1:]...)
			buf, err := json.Marshal(mountList)
			if err == nil {
				pv.Annotations[xfs.PVCVolumeHostPathMountNode] = string(buf)
			}
			return err == nil, deletePodInfos
		}
	}
	return false, []podInfo{}
}
func recordPVEvent(recorder record.EventRecorder, pvName, pvUID, eventtype, reason, event string) {
	ref := &clientv1.ObjectReference{
		Kind:      "PersistentVolume",
		Name:      pvName,
		UID:       kubetypes.UID(pvUID),
		Namespace: "",
	}
	glog.V(2).Infof("Recording %s event message for pv %s", event, pvName)
	recorder.Eventf(ref, eventtype, reason, "pv %s event: %s", pvName, event)
}

// getPVCExtractPV fetches the PVC object with the given namespace and name from
// the API server extracts the name of the PV it is pointing to and returns it.
// An error is returned if the PVC object's phase is not "Bound".
func (npvc *NodePVController) getPVCExtractPV(
	namespace string, claimName string) (string, kubetypes.UID, error) {
	pvc, err :=
		npvc.kubeClient.Core().PersistentVolumeClaims(namespace).Get(claimName, metav1.GetOptions{})
	if err != nil || pvc == nil {
		return "", "", fmt.Errorf(
			"failed to fetch PVC %s/%s from API server. err=%v",
			namespace,
			claimName,
			err)
	}

	if pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName == "" {
		return "", "", fmt.Errorf(
			"PVC %s/%s has non-bound phase (%q) or empty pvc.Spec.VolumeName (%q)",
			namespace,
			claimName,
			pvc.Status.Phase,
			pvc.Spec.VolumeName)
	}

	return pvc.Spec.VolumeName, pvc.UID, nil
}
func (npvc *NodePVController) getPVByName(name string, expectedClaimUID kubetypes.UID) (*v1.PersistentVolume, error) {
	pv, err := npvc.kubeClient.Core().PersistentVolumes().Get(name, metav1.GetOptions{})
	if err != nil || pv == nil {
		return nil, fmt.Errorf("getPVByName %s from API server. err=%v", name, err)
	}

	if pv.Spec.ClaimRef == nil {
		return nil, fmt.Errorf("getPVByName found PV object %q but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim", name)
	}

	if pv.Spec.ClaimRef.UID != expectedClaimUID {
		return nil, fmt.Errorf(
			"getPVByName found PV object %q but its pv.Spec.ClaimRef.UID (%q) does not point to claim.UID (%q)",
			name,
			pv.Spec.ClaimRef.UID,
			expectedClaimUID)
	}
	return pv, nil
}
func (npvc *NodePVController) getPVByClaimName(namespace, claimName string) (pv *v1.PersistentVolume, err error) {
	pvName, pvcUID, err := npvc.getPVCExtractPV(namespace, claimName)
	if err != nil {
		return nil, fmt.Errorf("getPVByClaimName %q/%q: %v", namespace, claimName, err)
	}
	return npvc.getPVByName(pvName, pvcUID)
}

func (npvc *NodePVController) isPodCanBeDeleted(podInfo podInfo, nodeName string) (bool, error) {
	curPod, err := npvc.kubeClient.Core().Pods(podInfo.podNS).Get(podInfo.podName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	for _, podVolume := range curPod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			pv, err := npvc.getPVByClaimName(curPod.Namespace, pvcSource.ClaimName)
			if pv == nil || err != nil {
				return false, err
			}
			if isPVCanBeRemovedFromNode(pv, nodeName) == false {
				return false, nil
			}
		}
	}
	return true, nil
}
func (npvc *NodePVController) deletePod(podInfo podInfo) (bool, error) {
	curPod, err := npvc.kubeClient.Core().Pods(podInfo.podNS).Get(podInfo.podName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	// UID is changed the pod is recreated
	if string(curPod.UID) != podInfo.podUID {
		return true, nil
	}
	var gracePeriod int64 = 0
	var UID kubetypes.UID = kubetypes.UID(curPod.UID)
	if err := npvc.kubeClient.Core().Pods(podInfo.podNS).Delete(podInfo.podName,
		&metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
			Preconditions:      &metav1.Preconditions{UID: &UID},
		}); err != nil {
		return false, err
	}
	return true, nil
}
func (npvc *NodePVController) deletePods(podInfos []podInfo, nodeName string, recordFunc func(podNS, podName string, err error)) {
	for _, pod := range podInfos {
		if ok, err := npvc.isPodCanBeDeleted(pod, nodeName); ok {
			_, err := npvc.deletePod(pod)
			fmt.Printf("patrick NodePVController need delete pod %s:%s :%v\n", pod.podNS, pod.podName, err)
			if recordFunc != nil {
				recordFunc(pod.podNS, pod.podName, err)
			}
		} else {
			fmt.Printf("patrick NodePVController not need delete pod %s:%s :%v\n", pod.podNS, pod.podName, err)
			if recordFunc != nil {
				recordFunc(pod.podNS, pod.podName, err)
			}
		}
	}
}
func (npvc *NodePVController) tryDeletePVNodeHostPathHistory(pvName volume, nodeName string) error {
	var latestPv *v1.PersistentVolume
	for i := 0; i < PVUpdateRetryCount; i++ {
		pv, needRetry, deletePodInfos, err := npvc.deletePVNodeHostPathHistory(pvName, nodeName)
		if needRetry == false {
			if err == nil && pv != nil {
				recordPVEvent(npvc.recorder, pv.Name, string(pv.UID), v1.EventTypeNormal, "Node not available",
					fmt.Sprintf("deletePVNodeHostPathHistory pv %s %s hostpath success", pv.Name, nodeName))

				if isTimeoutNeedDeletePod(pv) {
					fmt.Printf("patrick NodePVController need delete pv %s pods: %d\n", pv.Name, len(deletePodInfos))
					// delete pod should after delete pv hostpath history
					npvc.deletePods(deletePodInfos, nodeName, func(podNs, podName string, err error) {
						if err == nil {
							recordPVEvent(npvc.recorder, pv.Name, string(pv.UID), v1.EventTypeNormal, "NodeTimeOutDeletePods",
								fmt.Sprintf("delete pod %s:%s success", podNs, podName))
						} else {
							recordPVEvent(npvc.recorder, pv.Name, string(pv.UID), v1.EventTypeWarning, "NodeTimeOutDeletePods",
								fmt.Sprintf("delete pod %s:%s fail %v", podNs, podName, err))
						}
					})
				} else {
					fmt.Printf("patrick NodePVController node need delete pv %s pods\n", pv.Name)
				}
			}
			return err
		}
		if pv != nil {
			latestPv = pv
		}
		time.Sleep(PVUpdateInterval)
	}
	if latestPv != nil {
		recordPVEvent(npvc.recorder, latestPv.Name, string(latestPv.UID), v1.EventTypeWarning, "Node not available",
			fmt.Sprintf("deletePVNodeHostPathHistory pv %s %s hostpath fail", latestPv.Name, nodeName))
	}
	return fmt.Errorf("deletePVNodeHostPathHistory pv %s %s hostpath fail", pvName.pvName, nodeName)
}

func (npvc *NodePVController) deletePVNodeHostPathHistory(pvName volume, nodeName string) (pv *v1.PersistentVolume, needRetry bool, deletePodInfos []podInfo, err error) {
	newVolume, err := npvc.kubeClient.Core().PersistentVolumes().Get(pvName.pvName, metav1.GetOptions{})
	if err != nil || string(newVolume.UID) != pvName.pvUID {
		glog.V(3).Infof("error reading peristent volume %v: %v", pvName, err)
		return nil, false, []podInfo{}, err
	}
	if ok, _ := isPVNeedSync(newVolume); ok == false {
		return newVolume, false, []podInfo{}, fmt.Errorf("pv %s is not need sync", pvName)
	}
	changed, podInfos := deletePVHistory(newVolume, nodeName)
	if changed {
		_, err := npvc.kubeClient.Core().PersistentVolumes().Update(newVolume)
		if err != nil {
			glog.V(4).Infof("updating PersistentVolume[%s]: rollback failed: %v", newVolume.Name, err)
			return newVolume, true, []podInfo{}, err
		} else {
			return newVolume, false, podInfos, nil
		}
	}
	return newVolume, false, []podInfo{}, nil
}

func (npvc *NodePVController) addNodeSyncWorker(nodeName string, diedTime time.Time) {
	npvc.runMutex.Lock()
	defer npvc.runMutex.Unlock()
	if _, exists := npvc.nodeSyncMap[nodeName]; exists == false {
		workStop := make(chan struct{}, 0)
		npvc.nodeSyncMap[nodeName] = workStop
		go func() {
			defer func() {
				npvc.deleteNodeSyncWorker(nodeName)
			}()
			for {
				exit := false
				select {
				case <-npvc.stop:
					exit = true
				case <-workStop:
					exit = true
				case <-time.After(time.Second):
					fmt.Printf("NodePVController debug sync node %s\n", nodeName)
					run := func() bool {
						npvc.nodePVMapMutex.Lock()
						defer npvc.nodePVMapMutex.Unlock()
						nodeMap, find := npvc.nodePVTimeOutMap[nodeName]
						if find == false || len(nodeMap) == 0 {
							return true
						}
						currentTimeOut := time.Now().Sub(diedTime)
						newNodeMap := make(map[volume]uint)
						for pv, timeout := range nodeMap {
							if currentTimeOut >= time.Duration(timeout)*time.Second {
								err := npvc.tryDeletePVNodeHostPathHistory(pv, nodeName)
								if err != nil { // should retry later
									newNodeMap[pv] = timeout + 60 // retry after 60s
								}
							} else {
								newNodeMap[pv] = timeout
							}
						}
						if len(newNodeMap) == 0 {
							delete(npvc.nodePVTimeOutMap, nodeName)
							return true
						} else {
							npvc.nodePVTimeOutMap[nodeName] = newNodeMap
						}
						return false
					}
					flag := run()
					if flag == true {
						exit = true
					}
				}
				if exit == true {
					break
				}
			}
		}()
	}
}

func (npvc *NodePVController) deleteNodeSyncWorker(nodeName string) {
	npvc.runMutex.Lock()
	defer npvc.runMutex.Unlock()
	if stop, exists := npvc.nodeSyncMap[nodeName]; exists == true {
		close(stop)
		delete(npvc.nodeSyncMap, nodeName)
	}
}
func (npvc *NodePVController) work() {
	pvs, err := npvc.pvLister.List(labels.Everything())
	if err != nil {
		glog.V(4).Infof("List PersistentVolume failed: %v", err)
		return
	}
	for _, pv := range pvs {
		npvc.syncPV(pv)
	}

	nodes, err := npvc.nodeLister.List(labels.Everything())
	if err != nil {
		glog.V(4).Infof("List Nodes failed: %v", err)
		return
	}
	nodeMap := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		npvc.syncNode(node, false)
		nodeMap[node.Name] = true
	}

	for _, node := range npvc.getNodePVTimeOutMapNodes() {
		if ok, _ := nodeMap[node]; ok == false { // node is deleted
			npvc.addNodeSyncWorker(node, time.Now())
		}
	}

}
func (npvc *NodePVController) debug() {
	func() {
		npvc.runMutex.Lock()
		defer npvc.runMutex.Unlock()
		fmt.Printf("NodePVController debug +++++++++++++++++++++++++++++++\n")
		for node, _ := range npvc.nodeSyncMap {
			fmt.Printf("NodePVController debug ++++++ node[%s] is syncing\n", node)
		}
		fmt.Printf("NodePVController debug +++++++++++++++++++++++++++++++\n")
	}()

	func() {
		npvc.nodePVMapMutex.Lock()
		defer npvc.nodePVMapMutex.Unlock()
		fmt.Printf("NodePVController debug -------------------------------\n")
		for node, nodeMap := range npvc.nodePVTimeOutMap {
			tmpstr := ""
			for v, t := range nodeMap {
				tmpstr += fmt.Sprintf("[%s:%d]", v.pvName, t)
			}
			fmt.Printf("NodePVController debug ---- node[%s]: %s\n", node, tmpstr)
		}
		fmt.Printf("NodePVController debug -------------------------------\n")
	}()
}

// Run starts an asynchronous loop that monitors the status of cluster nodes.
func (npvc *NodePVController) Run(stopCh <-chan struct{}) {

	defer utilruntime.HandleCrash()

	glog.Infof("Starting NodePVController")
	defer glog.Infof("Shutting down NodePVController")

	if !cache.WaitForCacheSync(wait.NeverStop, npvc.nodeInformerSynced, npvc.pvInformerSynced) {
		utilruntime.HandleError(errors.New("NodePVController timed out while waiting for informers to sync..."))
		return
	}
	go wait.Until(npvc.work, SyncAllInterval, stopCh)
	//go wait.Until(npvc.debug, 10*time.Second, stopCh)
	<-stopCh
}

func isTimeoutNeedDeletePod(pv *v1.PersistentVolume) bool {
	if pv == nil || pv.Annotations == nil || pv.Annotations[xfs.PVHostPathTimeoutDelPodAnn] != "true" {
		return false
	}
	return true
}

func getPVMountPodsInfo(info pvnodeaffinity.HostPathPVMountInfo) (podInfos []podInfo) {
	for _, mountInfo := range info.MountInfos {
		if mountInfo.PodInfo != nil {
			strs := strings.Split(mountInfo.PodInfo.Info, ":")
			if len(strs) != 3 {
				continue
			}
			podInfos = append(podInfos, podInfo{
				podName: strs[1],
				podNS:   strs[0],
				podUID:  strs[2],
			})
		}
	}
	return
}
