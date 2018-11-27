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

package priorities

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

type NodePVDiskUsePriority struct {
	pvInfo  predicates.PersistentVolumeInfo
	pvcInfo predicates.PersistentVolumeClaimInfo
}

func NewNodePVDiskUsePriority(
	pvInfo predicates.PersistentVolumeInfo,
	pvcInfo predicates.PersistentVolumeClaimInfo) (algorithm.PriorityMapFunction, algorithm.PriorityReduceFunction) {
	nodePVDiskUsePriority := &NodePVDiskUsePriority{
		pvInfo:  pvInfo,
		pvcInfo: pvcInfo,
	}
	return nodePVDiskUsePriority.CalculateNodePVUseMap, nodePVDiskUsePriority.CalculateNodePVUseReduce
}
func (npvdup *NodePVDiskUsePriority) getPVUsedAndQuotaSize(node *v1.Node) (map[string]int64, map[string]int64) {
	nodename := node.Name
	used := make(map[string]int64)
	quota := make(map[string]int64)
	pvs, err := npvdup.pvInfo.List()
	if err != nil {
		return used, quota
	}
	for _, pv := range pvs {
		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			continue
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			glog.Errorf("getNodeUsedAndQuotaSize Unmarshal[%s] error %v", mountInfo, errUmarshal)
			continue
		}
		for _, mount := range mountList {
			if mount.NodeName == nodename {
				for _, mountinfo := range mount.MountInfos {
					pdir := getStandardPathString(getParentDir(mountinfo.HostPath))
					used[pdir] += mountinfo.VolumeCurrentSize
					quota[pdir] += mountinfo.VolumeQuotaSize
				}
				break
			}
		}
	}
	return used, quota
}
func (npvdup *NodePVDiskUsePriority) getNodeDiskAllocableSize(node *v1.Node) map[string]int64 {
	ret := make(map[string]int64)
	if node == nil || node.Annotations == nil || node.Annotations[xfs.NodeDiskQuotaInfoAnn] == "" {
		return ret
	}
	diskinfo := node.Annotations[xfs.NodeDiskQuotaInfoAnn]
	list := make(xfs.NodeDiskQuotaInfoList, 0)
	errUmarshal := json.Unmarshal([]byte(diskinfo), &list)
	if errUmarshal != nil {
		glog.Errorf("getNodeDiskAllocableSize Unmarshal[%s] error %v", diskinfo, errUmarshal)
		return ret
	}
	for _, disk := range list {
		ret[getStandardPathString(disk.MountPath)] = disk.Allocable
	}
	return ret
}
func (npvdup *NodePVDiskUsePriority) isPodHasHostPathPV(pod *v1.Pod) bool {
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			pvc, err := npvdup.pvcInfo.GetPersistentVolumeClaimInfo(pod.Namespace, pvcSource.ClaimName)
			if err != nil || pvc == nil {
				continue
			}

			pv, err := npvdup.pvInfo.GetPersistentVolumeInfo(pvc.Spec.VolumeName)
			if err != nil || pv == nil {
				continue
			}

			if pv.Spec.HostPath == nil && isHostPathCSIPV(pv) == false {
				continue
			}
			return true
		}
	}
	return false
}

func isHostPathCSIPV(pv *v1.PersistentVolume) bool {
	if pv.Spec.CSI != nil && strings.Contains(strings.ToLower(pv.Spec.CSI.Driver), "hostpath") == true {
		return true
	}
	return false
}

func (npvdup *NodePVDiskUsePriority) CalculateNodePVUseMap(pod *v1.Pod, meta interface{},
	nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	node := nodeInfo.Node()
	if node == nil {
		return schedulerapi.HostPriority{}, fmt.Errorf("node not found")
	}

	if npvdup.isPodHasHostPathPV(pod) == false {
		return schedulerapi.HostPriority{
			Host:  node.Name,
			Score: 10,
		}, nil
	}
	_, quota := npvdup.getPVUsedAndQuotaSize(node)
	allocable := npvdup.getNodeDiskAllocableSize(node)

	var score int
	for path, size := range allocable {
		quotasize := quota[path]
		if size > quotasize {
			score += int((size - quotasize) / (1024 * 1024)) // MB
		}
	}
	return schedulerapi.HostPriority{
		Host:  node.Name,
		Score: score,
	}, nil
}

func (npvdup *NodePVDiskUsePriority) CalculateNodePVUseReduce(pod *v1.Pod, meta interface{}, nodeNameToInfo map[string]*schedulercache.NodeInfo, result schedulerapi.HostPriorityList) error {
	var maxCount int
	for i := range result {
		if result[i].Score > maxCount {
			maxCount = result[i].Score
		}
	}
	maxCountFloat := float64(maxCount)

	var fScore float64
	for i := range result {
		if maxCount > 0 {
			fScore = 10 * (float64(result[i].Score) / maxCountFloat)
		} else {
			fScore = 0
		}
		if glog.V(10) {
			// We explicitly don't do glog.V(10).Infof() to avoid computing all the parameters if this is
			// not logged. There is visible performance gain from it.
			glog.Infof("%v -> %v: NodePVDiskUsePriority, Score: (%d)", pod.Name, result[i].Host, int(fScore))
		}
		result[i].Score = int(fScore)
	}
	return nil
}

type NodeHostPathSpread struct {
	pvInfo  predicates.PersistentVolumeInfo
	pvcInfo predicates.PersistentVolumeClaimInfo
}

func NewNodeHostPathSpreadPriority(
	pvInfo predicates.PersistentVolumeInfo,
	pvcInfo predicates.PersistentVolumeClaimInfo) (algorithm.PriorityMapFunction, algorithm.PriorityReduceFunction) {
	nodeHostPathSpread := &NodeHostPathSpread{
		pvInfo:  pvInfo,
		pvcInfo: pvcInfo,
	}
	return nodeHostPathSpread.CalculateNodeHostPathSpreadMap, nodeHostPathSpread.CalculateNodeHostPathSpreadReduce
}

func (nhps *NodeHostPathSpread) getPodMountHostPathPVName(podVolume v1.Volume, podNamespace string) (ok bool, pvname, pvcname string, pv *v1.PersistentVolume, err error) {
	if pvcSource :=
		podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {

		pvc, err := nhps.pvcInfo.GetPersistentVolumeClaimInfo(podNamespace, pvcSource.ClaimName)
		if err != nil || pvc == nil {
			return false, "", pvcSource.ClaimName, nil, fmt.Errorf("failed to fetch PVC %s/%s from API server. err=%v", podNamespace, pvcSource.ClaimName, err)
		}

		pv, err := nhps.pvInfo.GetPersistentVolumeInfo(pvc.Spec.VolumeName)
		if err != nil || pv == nil {
			return false, "", pvcSource.ClaimName, nil, fmt.Errorf("failed to fetch PV %q from API server. err=%v", pvc.Spec.VolumeName, err)
		}

		if pv.Spec.HostPath == nil && isHostPathCSIPV(pv) == false {
			return false, "", pvcSource.ClaimName, nil, nil
		}

		return true, pv.Name, pvcSource.ClaimName, pv, nil
	} else {
		return false, "", "", nil, nil
	}
}
func (nhps *NodeHostPathSpread) CalculateNodeHostPathSpreadMap(pod *v1.Pod, meta interface{},
	nodeInfo *schedulercache.NodeInfo) (schedulerapi.HostPriority, error) {
	node := nodeInfo.Node()

	if node == nil {
		return schedulerapi.HostPriority{}, fmt.Errorf("node not found")
	}

	nodepods := nodeInfo.Pods()
	var count int
	for _, podVolume := range pod.Spec.Volumes {
		ok, _, pvcname, pv, err := nhps.getPodMountHostPathPVName(podVolume, pod.Namespace)
		if ok == false || pvcname == "" || pv == nil || err != nil {
			continue
		}
		isForOnePod := false
		if pv.Annotations != nil && pv.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
			isForOnePod = true
		}
		// next start count the pod mount hostpath number of this pv
		// if isForOnePod == true the pods share the same hostpath on the node
		countForPods := 0
		for _, pod := range nodepods {
			for _, v := range pod.Spec.Volumes {
				if pvcSource := v.VolumeSource.PersistentVolumeClaim; pvcSource != nil && pvcSource.ClaimName == pvcname {
					if isForOnePod {
						countForPods++
					} else {
						countForPods = 1
					}
				}
			}
		}
		count += countForPods
	}

	return schedulerapi.HostPriority{
		Host:  node.Name,
		Score: count,
	}, nil
}

func (nhps *NodeHostPathSpread) CalculateNodeHostPathSpreadReduce(pod *v1.Pod, meta interface{}, nodeNameToInfo map[string]*schedulercache.NodeInfo, result schedulerapi.HostPriorityList) error {
	var maxCount int
	for i := range result {
		if result[i].Score > maxCount {
			maxCount = result[i].Score
		}
	}
	maxCountFloat := float64(maxCount)

	var fScore float64
	for i := range result {
		if maxCount > 0 {
			fScore = 10 * ((maxCountFloat - float64(result[i].Score)) / maxCountFloat)
		} else {
			fScore = 10
		}
		if glog.V(10) {
			// We explicitly don't do glog.V(10).Infof() to avoid computing all the parameters if this is
			// not logged. There is visible performance gain from it.
			glog.Infof("%v -> %v: NodePVDiskUsePriority, Score: (%d)", pod.Name, result[i].Host, int(fScore))
		}
		result[i].Score = int(fScore)
	}
	return nil
}

func getParentDir(path string) string {
	if strings.HasSuffix(path, "/") == true {
		path = path[0 : len(path)-1]
	}
	i := strings.LastIndex(path, "/")
	return path[0:i]
}
func getStandardPathString(path string) string {
	if strings.HasSuffix(path, "/") == false {
		path = path + "/"
	}
	return strings.Replace(path, "//", "/", -1)
}
