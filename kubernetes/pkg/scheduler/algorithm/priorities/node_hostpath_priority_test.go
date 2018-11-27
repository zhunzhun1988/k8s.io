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
	"reflect"
	"sort"
	"testing"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

type FakePersistentVolumeClaimInfo []v1.PersistentVolumeClaim

func (pvcs FakePersistentVolumeClaimInfo) GetPersistentVolumeClaimInfo(namespace string, pvcID string) (*v1.PersistentVolumeClaim, error) {
	for _, pvc := range pvcs {
		if pvc.Name == pvcID && pvc.Namespace == namespace {
			return &pvc, nil
		}
	}
	return nil, fmt.Errorf("Unable to find persistent volume claim: %s/%s", namespace, pvcID)
}

type FakePersistentVolumeInfo []v1.PersistentVolume

func (pvs FakePersistentVolumeInfo) GetPersistentVolumeInfo(pvID string) (*v1.PersistentVolume, error) {
	for _, pv := range pvs {
		if pv.Name == pvID {
			return &pv, nil
		}
	}
	return nil, fmt.Errorf("Unable to find persistent volume: %s", pvID)
}

func (pvs FakePersistentVolumeInfo) List() ([]*v1.PersistentVolume, error) {
	ret := make([]*v1.PersistentVolume, 0, len(pvs))
	for i, _ := range pvs {
		ret = append(ret, &pvs[i])
	}
	return ret, nil
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
func setPodNodeName(pod *v1.Pod, nodename string) *v1.Pod {
	pod.Spec.NodeName = nodename
	return pod
}
func getPVMountHistoryStr(nodesname []string, quotaSize [][]int64, quotaPath [][]string) string {
	if quotaSize != nil && len(quotaSize) > 0 && len(nodesname) != len(quotaSize) {
		fmt.Errorf("getPVMountHistoryStr len(nodesname):%d != len(quotaSize):%d", len(nodesname), len(quotaSize))
		return ""
	}
	if quotaPath != nil && quotaSize != nil && len(quotaPath) != len(quotaSize) {
		return ""
	}
	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	for i, name := range nodesname {
		if quotaSize == nil {
			mountList = append(mountList, pvnodeaffinity.HostPathPVMountInfo{NodeName: name})
		} else {
			info := pvnodeaffinity.HostPathPVMountInfo{NodeName: name, MountInfos: make(pvnodeaffinity.MountInfoList, 0, len(quotaSize[i]))}
			for j, size := range quotaSize[i] {
				info.MountInfos = append(info.MountInfos, pvnodeaffinity.MountInfo{
					VolumeQuotaSize: size,
					HostPath:        quotaPath[i][j],
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
func newNode(nodename string, diskpaths []string, disksize []int64) *v1.Node {
	var annotation map[string]string
	if diskpaths == nil || disksize == nil || len(diskpaths) == 0 || len(diskpaths) != len(disksize) {
		annotation = nil
	} else {
		annotation = make(map[string]string)
		info := make(xfs.NodeDiskQuotaInfoList, 0)

		for i, _ := range diskpaths {
			info = append(info, xfs.NodeDiskQuotaInfo{
				MountPath: diskpaths[i],
				Allocable: disksize[i],
			})
		}

		sort.Sort(info)
		buf, err := json.Marshal(info)
		if err == nil {
			annotation[xfs.NodeDiskQuotaInfoAnn] = string(buf)
		}
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        nodename,
			Annotations: annotation,
		},
	}
}

func TestPVNodeSpreadPriority(t *testing.T) {
	tests := []struct {
		pod          *v1.Pod
		pods         []*v1.Pod
		nodes        []*v1.Node
		expectedList schedulerapi.HostPriorityList
		test         string
	}{
		{
			pod:  newPodWithPVC(),
			pods: []*v1.Pod{},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "pod has no hostpath pvc",
		},
		{
			pod: newPodWithPVC("hostpathpvc1"),
			pods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc2"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3", "hostpathpvc1"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine3"),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 5}, {Host: "machine2", Score: 0}, {Host: "machine3", Score: 10}},
			test:         "machine1 has 2, machine2 has 1 and machine3 has none hostpath",
		},
		{
			pod: newPodWithPVC("hostpathpvc1", "hostpathpvc2"),
			pods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc2"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine3"),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 0}, {Host: "machine2", Score: 6}, {Host: "machine3", Score: 10}},
			test:         "machine1 has none , machine2 has 1 and machine3 has 1 hostpath",
		},
		{
			pod: newPodWithPVC("hostpathpvc3"),
			pods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc2"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine3"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine3"),
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc2"), "machine3"),
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "machine3"),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 0}, {Host: "machine3", Score: 0}},
			test:         "pv has not mount any hostpath",
		},
		{
			pod: newPodWithPVC("hostpathpvc1", "hostpathpvc2", "hostpathpvc3"),
			pods: []*v1.Pod{
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc2"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc2"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc1"), "machine1"),
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc2", "hostpathpvc3"), "machine2"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine3"),
				setPodNodeName(newPodWithPVC("hostpathpvc3"), "machine3"),
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc2"), "machine3"),
				setPodNodeName(newPodWithPVC("hostpathpvc1", "hostpathpvc3"), "machine3"),
			},
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 0}, {Host: "machine2", Score: 4}, {Host: "machine3", Score: 2}},
			test:         "machine1 has 2, machine2 has 2 and machine3 has 1 hostpath",
		},
	}
	pvInfo := FakePersistentVolumeInfo{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "hostpathpv1",
				Annotations: map[string]string{
					xfs.PVHostPathMountPolicyAnn: "none",
					xfs.PVHostPathQuotaForOnePod: "true",
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
					xfs.PVHostPathMountPolicyAnn: "none",
					xfs.PVHostPathQuotaForOnePod: "true",
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
					xfs.PVHostPathMountPolicyAnn: "none",
					xfs.PVHostPathQuotaForOnePod: "false",
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
	}

	for _, test := range tests {
		nodeHostPathSpread := &NodeHostPathSpread{
			pvInfo:  pvInfo,
			pvcInfo: pvcInfo,
		}
		nodeNameToInfo := schedulercache.CreateNodeNameToInfoMap(test.pods, test.nodes)
		ttp := priorityFunction(nodeHostPathSpread.CalculateNodeHostPathSpreadMap, nodeHostPathSpread.CalculateNodeHostPathSpreadReduce, nil)
		list, err := ttp(test.pod, nodeNameToInfo, test.nodes)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(test.expectedList, list) {
			t.Errorf("%s: \nexpected \n\t%#v, \ngot \n\t%#v\n", test.test, test.expectedList, list)
		}
	}
}

func TestNodePVDiskUsePriority(t *testing.T) {
	pv1 := v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "hostpathpv1",
			Annotations: map[string]string{
				xfs.PVHostPathMountPolicyAnn: "none",
				xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr([]string{"machine1"},
					[][]int64{{50 * 1024 * 1024}}, [][]string{{"/xfs/disk1/testdir"}}),
			},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
			},
		},
	}
	pv2 := v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "hostpathpv2",
			Annotations: map[string]string{
				xfs.PVHostPathMountPolicyAnn: "none",
				xfs.PVCVolumeHostPathMountNode: getPVMountHistoryStr([]string{"machine1", "machine2"},
					[][]int64{{50 * 1024 * 1024}, {50 * 1024 * 1024, 20 * 1024 * 1024}},
					[][]string{{"/xfs/disk1/testdir1"}, {"/xfs/disk1/testdir2", "/xfs/disk2/testdir3"}}),
			},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
			},
		},
	}
	pv3 := v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "hostpathpv3",
			Annotations: map[string]string{
				xfs.PVHostPathMountPolicyAnn:   "none",
				xfs.PVCVolumeHostPathMountNode: "", // has no node mounted
			},
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(50*1024*1024, resource.BinarySI)},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "/xfs/disk1"},
			},
		},
	}

	pvc1 := v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc1"},
		Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv1"},
	}

	pvc2 := v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc2"},
		Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv2"},
	}

	pvc3 := v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "hostpathpvc3"},
		Spec:       v1.PersistentVolumeClaimSpec{VolumeName: "hostpathpv3"},
	}

	tests := []struct {
		pod          *v1.Pod
		pods         []*v1.Pod
		nodes        []*v1.Node
		expectedList schedulerapi.HostPriorityList
		test         string
		pvInfo       FakePersistentVolumeInfo
		pvcInfo      FakePersistentVolumeClaimInfo
	}{
		{
			pod: newPodWithPVC(),
			nodes: []*v1.Node{
				newNode("machine1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
				newNode("machine2", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
				newNode("machine3", nil, nil),
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "pod use no hostpath pv",
			pvInfo:       FakePersistentVolumeInfo{},
			pvcInfo:      FakePersistentVolumeClaimInfo{},
		},
		{
			pod: newPodWithPVC("hostpathpvc1"),
			nodes: []*v1.Node{
				newNode("machine1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
				newNode("machine2", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
				newNode("machine3", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 5}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "hostpathpvc1 has be used 50MB",
			pvInfo:       FakePersistentVolumeInfo{pv1},
			pvcInfo:      FakePersistentVolumeClaimInfo{pvc1},
		},
		{
			pod: newPodWithPVC("hostpathpvc1"),
			nodes: []*v1.Node{
				newNode("machine1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
				newNode("machine2", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
				newNode("machine3", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 0}, {Host: "machine2", Score: 5}, {Host: "machine3", Score: 10}},
			test:         "hostpathpvc1 has be used 50MB",
			pvInfo:       FakePersistentVolumeInfo{pv1, pv2, pv3},
			pvcInfo:      FakePersistentVolumeClaimInfo{pvc1, pvc2, pvc3},
		},
		{
			pod: newPodWithPVC("hostpathpvc1"),
			nodes: []*v1.Node{
				newNode("machine1", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
				newNode("machine2", []string{"/xfs/disk1", "/xfs/disk2/"}, []int64{100 * 1024 * 1024, 100 * 1024 * 1024}),
				newNode("machine3", []string{"/xfs/disk1"}, []int64{100 * 1024 * 1024}),
			},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 0}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 7}},
			test:         "hostpathpvc1 has be used 50MB",
			pvInfo:       FakePersistentVolumeInfo{pv1, pv2, pv3},
			pvcInfo:      FakePersistentVolumeClaimInfo{pvc1, pvc2, pvc3},
		},
	}

	for _, test := range tests {
		nodePVDiskUsePriority := NodePVDiskUsePriority{
			pvInfo:  test.pvInfo,
			pvcInfo: test.pvcInfo,
		}
		nodeNameToInfo := schedulercache.CreateNodeNameToInfoMap(nil, test.nodes)
		ttp := priorityFunction(nodePVDiskUsePriority.CalculateNodePVUseMap, nodePVDiskUsePriority.CalculateNodePVUseReduce, nil)
		list, err := ttp(test.pod, nodeNameToInfo, test.nodes)
		if err != nil {
			t.Errorf("%s, unexpected error: %v", test.test, err)
		}

		if !reflect.DeepEqual(test.expectedList, list) {
			t.Errorf("%s,\nexpected:\n\t%+v,\ngot:\n\t%+v", test.test, test.expectedList, list)
		}
	}
}
