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

package predicates

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	schedulertesting "k8s.io/kubernetes/pkg/scheduler/testing"
)

func newNode(nodename string, diskpaths []string, disksize, diskQuotaSize, diskUsedSize []int64) *v1.Node {
	var annotation map[string]string
	if diskpaths == nil || disksize == nil || len(diskpaths) == 0 || len(diskpaths) != len(disksize) ||
		len(diskpaths) != len(diskQuotaSize) || len(diskpaths) != len(diskUsedSize) {
		annotation = nil
	} else {
		annotation = make(map[string]string)
		info := make(xfs.NodeDiskQuotaInfoList, 0)
		states := xfs.QuotaStatus{DiskStatus: make([]xfs.DiskQuotaStatus, 0)}
		for i, _ := range diskpaths {
			info = append(info, xfs.NodeDiskQuotaInfo{
				MountPath: diskpaths[i],
				Allocable: disksize[i],
			})
			states.Capacity += disksize[i]
			states.CurUseSize += diskUsedSize[i]
			states.CurQuotaSize += diskQuotaSize[i]
			states.DiskStatus = append(states.DiskStatus, xfs.DiskQuotaStatus{
				Capacity:     disksize[i],
				CurUseSize:   diskUsedSize[i],
				CurQuotaSize: diskQuotaSize[i],
				MountPath:    diskpaths[i],
			})
		}

		sort.Sort(info)
		buf, err := json.Marshal(info)
		if err == nil {
			annotation[xfs.NodeDiskQuotaInfoAnn] = string(buf)
		}
		buf, err = json.Marshal(states)
		if err == nil {
			annotation[xfs.NodeDiskQuotaStatusAnn] = string(buf)
		}
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        nodename,
			Annotations: annotation,
		},
	}
}

func setPodNodeName(pod *v1.Pod, nodeName string) *v1.Pod {
	if pod != nil {
		pod.Spec.NodeName = nodeName
	}
	return pod
}
func setPodNsAndName(pod *v1.Pod, ns, name string) *v1.Pod {
	if pod != nil {
		pod.Name = name
		pod.Namespace = ns
	}
	return pod
}
func newPodWithPVC(pvcs ...string) *v1.Pod {
	ret := &v1.Pod{
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{},
		},
	}
	for _, pvc := range pvcs {
		ret.Spec.Volumes = append(ret.Spec.Volumes, v1.Volume{
			VolumeSource: v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc,
				},
			},
		})
	}
	return ret
}
func getPVMountHistoryStr(pathNum int, nodesname ...string) string {
	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	pathList := make(pvnodeaffinity.MountInfoList, 0, pathNum)
	for i := 0; i < pathNum; i++ {
		pathList = append(pathList, pvnodeaffinity.MountInfo{})
	}
	for _, name := range nodesname {
		mountList = append(mountList, pvnodeaffinity.HostPathPVMountInfo{NodeName: name, MountInfos: pathList})
	}
	buf, err := json.Marshal(mountList)
	if err == nil {
		return string(buf)
	}
	return ""
}
func getPVMountHistoryStr2(nodesname []string, quotaSize [][]int64, quotaPath [][]string, podInfo [][]string) string {
	if quotaSize != nil && len(quotaSize) > 0 && len(nodesname) != len(quotaSize) {
		fmt.Errorf("getPVMountHistoryStr len(nodesname):%d != len(quotaSize):%d", len(nodesname), len(quotaSize))
		return ""
	}
	if quotaPath != nil && quotaSize != nil && len(quotaPath) != len(quotaSize) {
		return ""
	}
	if podInfo != nil && len(podInfo) != len(quotaPath) {
		return ""
	}
	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	for i, name := range nodesname {
		if quotaSize == nil {
			mountList = append(mountList, pvnodeaffinity.HostPathPVMountInfo{NodeName: name})
		} else {
			info := pvnodeaffinity.HostPathPVMountInfo{NodeName: name, MountInfos: make(pvnodeaffinity.MountInfoList, 0, len(quotaSize[i]))}
			for j, size := range quotaSize[i] {
				var pi *pvnodeaffinity.PodInfo
				if podInfo != nil {
					pi = &pvnodeaffinity.PodInfo{
						Info: podInfo[i][j],
					}
				}
				info.MountInfos = append(info.MountInfos, pvnodeaffinity.MountInfo{
					VolumeQuotaSize: size,
					HostPath:        quotaPath[i][j],
					PodInfo:         pi,
				})
			}
			mountList = append(mountList, info)
		}
	}
	buf, err := json.Marshal(mountList)
	if err == nil {
		return string(buf)
	}
	return ""
}
func TestPodHostPVDiskPressureNoneAndTrue(t *testing.T) {
	pvInfo := FakePersistentVolumeInfo{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv1",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod: "true",
					xfs.PVHostPathMountPolicyAnn: "none",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr2([]string{"node1"},
						[][]int64{{50 * 1024 * 1024}},
						[][]string{{"/xfs/disk1/dir1"}}, [][]string{{"default:testpod1:uid1"}}),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/"},
				},
			},
		},
	}
	pvcInfo := FakePersistentVolumeClaimInfo{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc2", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv2"},
		},
	}
	tests := []struct {
		pod      *v1.Pod
		nodeInfo *schedulercache.NodeInfo
		node     *v1.Node
		fits     bool
		test     string
		wErr     error
		reasons  []algorithm.PredicateFailureReason
		pvInfo   FakePersistentVolumeInfo
		pvcInfo  FakePersistentVolumeClaimInfo
	}{
		{
			pod:      setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod1")),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "has enough disk",
			reasons:  []algorithm.PredicateFailureReason{},
			pvInfo:   FakePersistentVolumeInfo{pvInfo[0]},
			pvcInfo:  FakePersistentVolumeClaimInfo{pvcInfo[0]},
		},
		{
			pod:      setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod1")),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "has enough disk",
			reasons:  []algorithm.PredicateFailureReason{},
			pvInfo:   FakePersistentVolumeInfo{pvInfo[0]},
			pvcInfo:  FakePersistentVolumeClaimInfo{pvcInfo[0]},
		},
	}
	for _, test := range tests {
		pred := NewPodHostPVDiskPredicate(test.pvInfo, test.pvcInfo)
		test.nodeInfo.SetNode(test.node)
		fits, reasons, err := pred(test.pod, PredicateMetadata(test.pod, nil), test.nodeInfo)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !fits && !reflect.DeepEqual(reasons, test.reasons) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, reasons, test.reasons)
		}
		if fits != test.fits {
			t.Errorf("%s: expected %v, got %v", test.test, test.fits, fits)
		}
	}
}

func TestPodHostPVDiskPressureKeepAndFalse(t *testing.T) {
	pvInfo := FakePersistentVolumeInfo{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv1",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod: "false",
					xfs.PVHostPathMountPolicyAnn: "keep",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr2([]string{"node1"},
						[][]int64{{50 * 1024 * 1024}},
						[][]string{{"/xfs/disk1/dir1"}}, nil),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv2",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod: "false",
					xfs.PVHostPathMountPolicyAnn: "keep",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
	}
	pvcInfo := FakePersistentVolumeClaimInfo{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc2", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv2"},
		},
	}
	tests := []struct {
		pod      *v1.Pod
		nodeInfo *schedulercache.NodeInfo
		node     *v1.Node
		fits     bool
		test     string
		wErr     error
		reasons  []algorithm.PredicateFailureReason
		pvInfo   FakePersistentVolumeInfo
		pvcInfo  FakePersistentVolumeClaimInfo
	}{
		{
			pod:      setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod1")),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{50 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "can run directly",
			reasons:  []algorithm.PredicateFailureReason{},
			pvInfo:   FakePersistentVolumeInfo{pvInfo[0]},
			pvcInfo:  FakePersistentVolumeClaimInfo{pvcInfo[0]},
		},
		{
			pod:      setPodNsAndName(newPodWithPVC("hostpathpvc2"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(setPodNsAndName(newPodWithPVC("hostpathpvc2"), "default", "testpod1")),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{50 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "can run directly",
			reasons:  []algorithm.PredicateFailureReason{},
			pvInfo:   FakePersistentVolumeInfo{pvInfo[1]},
			pvcInfo:  FakePersistentVolumeClaimInfo{pvcInfo[1]},
		},
		{
			pod: setPodNsAndName(newPodWithPVC("hostpathpvc2", "hostpathpvc1"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(
				setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod1")),
			node: newNode("node1", []string{"/xfs/disk1", "/xfs/disk2"},
				[]int64{50 * 1024 * 1024, 50 * 1024 * 1024}, []int64{0, 0}, []int64{0, 0}),
			fits:    true,
			wErr:    nil,
			test:    "special test has enough disk",
			reasons: []algorithm.PredicateFailureReason{},
			pvInfo:  FakePersistentVolumeInfo{pvInfo[0], pvInfo[1]},
			pvcInfo: FakePersistentVolumeClaimInfo{pvcInfo[0], pvcInfo[1]},
		},
	}
	for _, test := range tests {
		pred := NewPodHostPVDiskPredicate(test.pvInfo, test.pvcInfo)
		test.nodeInfo.SetNode(test.node)
		fits, reasons, err := pred(test.pod, PredicateMetadata(test.pod, nil), test.nodeInfo)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !fits && !reflect.DeepEqual(reasons, test.reasons) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, reasons, test.reasons)
		}
		if fits != test.fits {
			t.Errorf("%s: expected %v, got %v", test.test, test.fits, fits)
		}
	}
}

func TestPodHostPVDiskPressureKeepAndTrue(t *testing.T) {
	pvInfo := FakePersistentVolumeInfo{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv1",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod: "true",
					xfs.PVHostPathMountPolicyAnn: "keep",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr2([]string{"node1"},
						[][]int64{{50 * 1024 * 1024, 50 * 1024 * 1024}},
						[][]string{{"/xfs/disk1/dir1", "/xfs/disk1/dir2"}}, [][]string{{"default:testpod1:uid1", "default:testpod2:uid2"}}),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv2",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod: "true",
					xfs.PVHostPathMountPolicyAnn: "keep",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr2([]string{"node1"},
						[][]int64{{50 * 1024 * 1024}},
						[][]string{{"/xfs/disk1/dir1"}}, [][]string{{"default:testpod1:uid1"}}),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv3",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod: "true",
					xfs.PVHostPathMountPolicyAnn: "keep",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr2([]string{"node1"},
						[][]int64{{50 * 1024 * 1024, 50 * 1024 * 1024}},
						[][]string{{"/xfs/disk1/dir1", "/xfs/disk2/dir2"}}, [][]string{{"default:testpod1:uid1", "default:testpod2:uid2"}}),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/"},
				},
			},
		},
	}
	pvcInfo := FakePersistentVolumeClaimInfo{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc1", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc2", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc3", Namespace: "default"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv3"},
		},
	}
	tests := []struct {
		pod      *v1.Pod
		nodeInfo *schedulercache.NodeInfo
		node     *v1.Node
		fits     bool
		test     string
		wErr     error
		reasons  []algorithm.PredicateFailureReason
		pvInfo   FakePersistentVolumeInfo
		pvcInfo  FakePersistentVolumeClaimInfo
	}{
		{
			pod:      setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod1")),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "can run directly",
			reasons:  []algorithm.PredicateFailureReason{},
			pvInfo:   FakePersistentVolumeInfo{pvInfo[0]},
			pvcInfo:  FakePersistentVolumeClaimInfo{pvcInfo[0]},
		},
		{
			pod: setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(
				setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod1"),
				setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod2")),
			node:    newNode("node1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:    false,
			wErr:    nil,
			test:    "disk is used",
			reasons: []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
			pvInfo:  FakePersistentVolumeInfo{pvInfo[1]},
			pvcInfo: FakePersistentVolumeClaimInfo{pvcInfo[1]},
		},
		{
			pod: setPodNsAndName(newPodWithPVC("hostpathpvc2"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(
				setPodNsAndName(newPodWithPVC("hostpathpvc2"), "default", "testpod1"),
				setPodNsAndName(newPodWithPVC("hostpathpvc2"), "default", "testpod2")),
			node:    newNode("node1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:    false,
			wErr:    nil,
			test:    "disk is used",
			reasons: []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
			pvInfo:  FakePersistentVolumeInfo{pvInfo[1]},
			pvcInfo: FakePersistentVolumeClaimInfo{pvcInfo[1]},
		},
		{
			pod: setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(
				setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod1"),
				setPodNsAndName(newPodWithPVC("hostpathpvc1"), "default", "testpod2")),
			node:    newNode("node1", []string{"/xfs/disk1"}, []int64{150 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:    true,
			wErr:    nil,
			test:    "has enough disk",
			reasons: []algorithm.PredicateFailureReason{},
			pvInfo:  FakePersistentVolumeInfo{pvInfo[0]},
			pvcInfo: FakePersistentVolumeClaimInfo{pvcInfo[0]},
		},
		{
			pod: setPodNsAndName(newPodWithPVC("hostpathpvc3"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(
				setPodNsAndName(newPodWithPVC("hostpathpvc3"), "default", "testpod1"),
				setPodNsAndName(newPodWithPVC("hostpathpvc3"), "default", "testpod2")),
			node: newNode("node1", []string{"/xfs/disk1", "/xfs/disk2"},
				[]int64{90 * 1024 * 1024, 90 * 1024 * 1024}, []int64{0, 0}, []int64{0, 0}),
			fits:    false,
			wErr:    nil,
			test:    "has no enough disk",
			reasons: []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
			pvInfo:  FakePersistentVolumeInfo{pvInfo[2]},
			pvcInfo: FakePersistentVolumeClaimInfo{pvcInfo[2]},
		},
		{
			pod: setPodNsAndName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(
				setPodNsAndName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "default", "testpod1"),
				setPodNsAndName(newPodWithPVC("hostpathpvc3", "hostpathpvc1"), "default", "testpod2"),
				setPodNsAndName(newPodWithPVC("hostpathpvc3", "hostpathpvc1"), "default", "testpod3")),
			node: newNode("node1", []string{"/xfs/disk1", "/xfs/disk2"},
				[]int64{300 * 1024 * 1024, 200 * 1024 * 1024}, []int64{0, 0}, []int64{0, 0}),
			fits:    true,
			wErr:    nil,
			test:    "special test has enough disk",
			reasons: []algorithm.PredicateFailureReason{},
			pvInfo:  FakePersistentVolumeInfo{pvInfo[0], pvInfo[2]},
			pvcInfo: FakePersistentVolumeClaimInfo{pvcInfo[0], pvcInfo[2]},
		},
		{
			pod: setPodNsAndName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "default", "schedulerpod"),
			nodeInfo: schedulercache.NewNodeInfo(
				setPodNsAndName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "default", "testpod1"),
				setPodNsAndName(newPodWithPVC("hostpathpvc3", "hostpathpvc1"), "default", "testpod2"),
				setPodNsAndName(newPodWithPVC("hostpathpvc3", "hostpathpvc1"), "default", "testpod3")),
			node: newNode("node1", []string{"/xfs/disk1", "/xfs/disk2"},
				[]int64{250 * 1024 * 1024, 150 * 1024 * 1024}, []int64{0, 0}, []int64{0, 0}),
			fits:    false,
			wErr:    nil,
			test:    "special test has no enough disk",
			reasons: []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
			pvInfo:  FakePersistentVolumeInfo{pvInfo[0], pvInfo[2]},
			pvcInfo: FakePersistentVolumeClaimInfo{pvcInfo[0], pvcInfo[2]},
		},
	}

	for _, test := range tests {
		pred := NewPodHostPVDiskPredicate(test.pvInfo, test.pvcInfo)
		test.nodeInfo.SetNode(test.node)
		fits, reasons, err := pred(test.pod, PredicateMetadata(test.pod, nil), test.nodeInfo)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !fits && !reflect.DeepEqual(reasons, test.reasons) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, reasons, test.reasons)
		}
		if fits != test.fits {
			t.Errorf("%s: expected %v, got %v", test.test, test.fits, fits)
		}
	}
}

func TestPodHostPVDiskPressure(t *testing.T) {
	tests := []struct {
		pod      *v1.Pod
		nodeInfo *schedulercache.NodeInfo
		node     *v1.Node
		fits     bool
		test     string
		wErr     error
		reasons  []algorithm.PredicateFailureReason
	}{
		{
			pod:      newPodWithPVC("hostpathpvc1"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "node is fit for pod",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc2"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     false,
			wErr:     nil,
			test:     "node has not enough disk fit for pod",
			reasons:  []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
		},
		{
			pod:      newPodWithPVC("hostpathpvc2"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node: newNode("node1", []string{"/xfs/disk1", "/xfs/disk2", "/xfs/disk3"},
				[]int64{5 * 1024 * 1024 * 1024, 5 * 1024 * 1024 * 1024, 5 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:    false,
			wErr:    nil,
			test:    "node has not one disk fit for pod",
			reasons: []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
		},
		{
			pod:      newPodWithPVC("hostpathpvc3"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node: newNode("node1", []string{"/xfs/disk1", "/xfs/disk2", "/xfs/disk3"},
				[]int64{5 * 1024 * 1024 * 1024, 5 * 1024 * 1024 * 1024, 5 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:    false,
			wErr:    nil,
			test:    "node has not one disk fit for pod",
			reasons: []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
		},
		{
			pod:      newPodWithPVC("hostpathpvc4", "hostpathpvc5"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node: newNode("node2", []string{"/xfs/disk1", "/xfs/disk2", "/xfs/disk3"},
				[]int64{5 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:    false,
			wErr:    nil,
			test:    "node has not one disk fit for pod",
			reasons: []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
		},
		{
			pod:      newPodWithPVC("hostpathpvc4", "hostpathpvc5"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{5 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "node can fit for pod one pv has be mounted",
			reasons:  []algorithm.PredicateFailureReason{ErrPodHostPathPVDiskPressure},
		},
	}
	pvInfo := FakePersistentVolumeInfo{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpv1"},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(100*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpv2"},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(10*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "hostpathpv3",
				Annotations: map[string]string{xfs.PVHostPathCapacityAnn: fmt.Sprintf("%d", 10*1024*1024*1024)},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(100*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv4",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod:   "true",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv5",
				Annotations: map[string]string{
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
	}

	pvcInfo := FakePersistentVolumeClaimInfo{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc1"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc2"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc3"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv3"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc4"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv4"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc5"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv5"},
		},
	}

	for _, test := range tests {
		pred := NewPodHostPVDiskPredicate(pvInfo, pvcInfo)
		test.nodeInfo.SetNode(test.node)
		fits, reasons, err := pred(test.pod, PredicateMetadata(test.pod, nil), test.nodeInfo)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !fits && !reflect.DeepEqual(reasons, test.reasons) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, reasons, test.reasons)
		}
		if fits != test.fits {
			t.Errorf("%s: expected %v, got %v", test.test, test.fits, fits)
		}
	}
}

func TestHostPathPVAffinityPredicate(t *testing.T) {
	tests := []struct {
		listPods []*v1.Pod
		pod      *v1.Pod
		nodeInfo *schedulercache.NodeInfo
		node     *v1.Node
		fits     bool
		test     string
		wErr     error
		reasons  []algorithm.PredicateFailureReason
	}{
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc1"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and true, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc1"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and true, node1 has run one pod, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc2"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc2"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc2"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc2"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     false,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod cannot run at node2",
			reasons:  []algorithm.PredicateFailureReason{ErrHostPathPVAffinityPredicate},
		},
		{
			pod:      newPodWithPVC("hostpathpvc3"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc3"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     false,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod cannot run at node2",
			reasons:  []algorithm.PredicateFailureReason{ErrHostPathPVAffinityPredicate},
		},
		{
			pod:      newPodWithPVC("hostpathpvc4"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc4"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false,  new pod cannot run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc5"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc5"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true,node1 has run one pod,  new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc6"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc6"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc7"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc7"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and false,node1 has run one pod,  new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc8"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc8"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc9"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "default policy, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc9"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     false,
			wErr:     nil,
			test:     "default policy, node1 has run one pod, new pod cannot run at node2",
			reasons:  []algorithm.PredicateFailureReason{ErrHostPathPVAffinityPredicate},
		},
		{
			pod:      newPodWithPVC("hostpathpvc10"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "default policy, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc10"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "default policy, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
	}
	pvInfo := FakePersistentVolumeInfo{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv1",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "true",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv2",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv3",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv4",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv5",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "true",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv6",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "true",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv7",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv8",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv9",
				Annotations: map[string]string{
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv10",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
	}

	pvcInfo := FakePersistentVolumeClaimInfo{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc1"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc2"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc3"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv3"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc4"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv4"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc5"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv5"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc6"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv6"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc7"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv7"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc8"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv8"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc9"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv9"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc10"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv10"},
		},
	}

	for _, test := range tests {
		pred := NewHostPathPVAffinityPredicate(pvInfo, pvcInfo, schedulertesting.FakePodLister(test.listPods))
		test.nodeInfo.SetNode(test.node)
		fits, reasons, err := pred(test.pod, PredicateMetadata(test.pod, nil), test.nodeInfo)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !fits && !reflect.DeepEqual(reasons, test.reasons) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, reasons, test.reasons)
		}
		if fits != test.fits {
			t.Errorf("%s: expected %v, got %v", test.test, test.fits, fits)
		}
	}
}

func TestHostPathPVAffinityPredicate2(t *testing.T) {
	tests := []struct {
		listPods []*v1.Pod
		pod      *v1.Pod
		nodeInfo *schedulercache.NodeInfo
		node     *v1.Node
		fits     bool
		test     string
		wErr     error
		reasons  []algorithm.PredicateFailureReason
	}{
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc1"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and true, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc1"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and true, node1 has run one pod, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			listPods: []*v1.Pod{},
			pod:      newPodWithPVC("hostpathpvc1"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and true, have been pod run at node1, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			listPods: []*v1.Pod{},
			pod:      newPodWithPVC("hostpathpvc1"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     false,
			wErr:     nil,
			test:     "keep and true, have been pod run at node1, new pod can not run at node2",
			reasons:  []algorithm.PredicateFailureReason{ErrHostPathPVAffinityPredicate2},
		},
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc2"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc2"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			listPods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc2"), "node1"),
			},
			pod:      newPodWithPVC("hostpathpvc2"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc3"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc3"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc4"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc4"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "keep and false,  new pod cannot run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc5"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc5"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true,node1 has run one pod,  new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc6"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc6"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc7"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and false, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc7"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and false,node1 has run one pod,  new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc8"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc8"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "none and true, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc9"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "default policy, node1 has run one pod, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc9"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "default policy, node1 has run one pod, new pod cannot run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc10"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node1", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "default policy, new pod can run at node1",
			reasons:  []algorithm.PredicateFailureReason{},
		},
		{
			pod:      newPodWithPVC("hostpathpvc10"),
			nodeInfo: schedulercache.NewNodeInfo(),
			node:     newNode("node2", []string{"/xfs/disk1"}, []int64{1 * 1024 * 1024 * 1024}, []int64{0}, []int64{0}),
			fits:     true,
			wErr:     nil,
			test:     "default policy, new pod can run at node2",
			reasons:  []algorithm.PredicateFailureReason{},
		},
	}
	pvInfo := FakePersistentVolumeInfo{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv1",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "true",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv2",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv3",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv4",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "keep",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv5",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "true",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv6",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "true",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv7",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv8",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn:   "none",
					xfs.PVHostPathQuotaForOnePod:   "false",
					xfs.PVCVolumeHostPathMountNode: "",
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv9",
				Annotations: map[string]string{
					xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr(1, "node1"),
				},
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv10",
			},
			Spec: v1.PersistentVolumeSpec{
				Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(5*1024*1024*1024, resource.BinarySI)},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
				},
			},
		},
	}

	pvcInfo := FakePersistentVolumeClaimInfo{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc1"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc2"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc3"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv3"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc4"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv4"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc5"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv5"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc6"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv6"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc7"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv7"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc8"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv8"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc9"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv9"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc10"},
			Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv10"},
		},
	}

	for _, test := range tests {
		pred := NewHostPathPVAffinityPredicate2(pvInfo, pvcInfo, schedulertesting.FakePodLister(test.listPods))
		test.nodeInfo.SetNode(test.node)
		fits, reasons, err := pred(test.pod, PredicateMetadata(test.pod, nil), test.nodeInfo)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !fits && !reflect.DeepEqual(reasons, test.reasons) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, reasons, test.reasons)
		}
		if fits != test.fits {
			t.Errorf("%s: expected %v, got %v", test.test, test.fits, fits)
		}
	}
}
