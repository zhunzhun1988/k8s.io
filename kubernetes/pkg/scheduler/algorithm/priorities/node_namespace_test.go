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
	"reflect"
	"testing"

	"k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/types"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	schedulertesting "k8s.io/kubernetes/pkg/scheduler/testing"
)

func newPodNamespace(namespace, name, nodename string, labels map[string]string, isMirrorPod, isCriticalPod bool, createBy string) *v1.Pod {
	annotations := make(map[string]string, 0)
	ownerReferences := []metav1.OwnerReference{}
	if isMirrorPod {
		annotations[types.ConfigMirrorAnnotationKey] = "true"
	} else if isCriticalPod {
		annotations[criticalPodAnnotation] = "true"
	} else if createBy != "" {
		ownerReferences = append(ownerReferences, metav1.OwnerReference{Kind: createBy})
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            name,
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: ownerReferences,
		},
		Spec: v1.PodSpec{
			NodeName: nodename,
		},
	}
}

func TestNodeNamespacePriority(t *testing.T) {
	labels1 := map[string]string{
		"foo": "bar",
		"baz": "blah",
	}
	tests := []struct {
		pod          *v1.Pod
		nodes        []*v1.Node
		nodePods     []*v1.Pod
		rcs          []*v1.ReplicationController
		rss          []*extensions.ReplicaSet
		services     []*v1.Service
		expectedList schedulerapi.HostPriorityList
		test         string
	}{
		{
			pod: newPodNamespace("testnamespace1", "testname", "", nil, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods:     []*v1.Pod{},
			services:     nil,
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "pod has not labels , all machines have not pods , rcs , rss and services",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", labels1, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace1", "testname1", "machine1", labels1, false, false, ""),
			},
			services:     nil,
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "nodepod has the same labels, have not rcs , rss and services",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", labels1, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace1", "testname1", "machine1", labels1, false, false, ""),
			},
			services:     []*v1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace1"}, Spec: v1.ServiceSpec{Selector: labels1}}},
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 0}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "nodepod has the same service, have not rcs , rss",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", labels1, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace1", "testname1", "machine1", labels1, false, false, ""),
			},
			services:     []*v1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace2"}, Spec: v1.ServiceSpec{Selector: labels1}}},
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "nodepod has the not the same service, have not rcs , rss",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", labels1, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace1", "testname1", "machine1", labels1, false, false, ""),
			},
			services:     nil,
			rcs:          []*v1.ReplicationController{{ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace1"}, Spec: v1.ReplicationControllerSpec{Selector: labels1}}},
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 0}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "nodepod has the same rc, have not service, rss",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", labels1, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace1", "testname1", "machine1", labels1, false, false, ""),
			},
			services:     nil,
			rcs:          []*v1.ReplicationController{{ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace2"}, Spec: v1.ReplicationControllerSpec{Selector: labels1}}},
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "nodepod has the not the same rc, have not service , rss",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", labels1, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace1", "testname1", "machine1", labels1, false, false, ""),
			},
			services:     nil,
			rcs:          nil,
			rss:          []*extensions.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace1"}, Spec: extensions.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: labels1}}}},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 0}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "nodepod has the same rs, have not service, rcs",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", labels1, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace1", "testname1", "machine1", labels1, false, false, ""),
			},
			services:     nil,
			rcs:          nil,
			rss:          []*extensions.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace2"}, Spec: extensions.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: labels1}}}},
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "nodepod has the not the same rs, have not service , rcs",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", nil, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("testnamespace2", "testname2", "machine1", nil, false, false, "Job"),
				newPodNamespace("testnamespace2", "testname3", "machine1", nil, true, false, ""),
				newPodNamespace("testnamespace2", "testname4", "machine1", nil, false, true, ""),
				newPodNamespace("testnamespace2", "testname5", "machine1", nil, false, false, "DaemonSet"),
			},
			services:     nil,
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "skip pod which are CriticalPod,MirrorPod, JobPod, DaemonSetPod and has not rcs, rss, svcs",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", nil, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("kube-system", "testname1", "machine1", nil, false, false, ""),
				newPodNamespace("kube-system", "testname3", "machine2", nil, false, false, ""),
				newPodNamespace("kube-system", "testname4", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname5", "machine1", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname6", "machine2", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname7", "machine2", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname8", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname9", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname10", "machine3", nil, false, false, ""),
			},
			services:     nil,
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 10}, {Host: "machine2", Score: 10}, {Host: "machine3", Score: 10}},
			test:         "all machines have run all the same namespace pods and has not rcs, rss, svcs",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", nil, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("kube-system", "testname1", "machine1", nil, false, false, ""),
				newPodNamespace("kube-system", "testname2", "machine2", nil, false, false, ""),
				newPodNamespace("kube-system", "testname3", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname4", "machine1", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname5", "machine2", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname6", "machine2", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname7", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname8", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname9", "machine3", nil, false, false, ""),
			},
			services:     nil,
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 5}, {Host: "machine2", Score: 3}, {Host: "machine3", Score: 2}},
			test:         "all machines have run all not the same namespace pods and has not rcs, rss, svcs",
		},
		{
			pod: newPodNamespace("testnamespace1", "testname", "", nil, false, false, ""),
			nodes: []*v1.Node{
				{ObjectMeta: metav1.ObjectMeta{Name: "machine1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "machine3"}},
			},
			nodePods: []*v1.Pod{
				newPodNamespace("kube-system", "testname1", "machine1", nil, false, false, ""),
				newPodNamespace("kube-system", "testname3", "machine2", nil, false, false, ""),
				newPodNamespace("kube-system", "testname4", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname5", "machine1", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname6", "machine1", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname7", "machine2", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname8", "machine2", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname9", "machine2", nil, false, false, ""),
				newPodNamespace("testnamespace1", "testname10", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname11", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname12", "machine3", nil, false, false, ""),
				newPodNamespace("testnamespace2", "testname13", "machine3", nil, false, false, ""),
			},
			services:     nil,
			rcs:          nil,
			rss:          nil,
			expectedList: []schedulerapi.HostPriority{{Host: "machine1", Score: 6}, {Host: "machine2", Score: 5}, {Host: "machine3", Score: 4}},
			test:         "all machines have run different namespace pods and has not rcs, rss, svcs",
		},
	}
	for _, test := range tests {
		nodeNameToInfo := schedulercache.CreateNodeNameToInfoMap(test.nodePods, nil)
		nameSpacePriority := NameSpacePriority{
			serviceLister:    schedulertesting.FakeServiceLister(test.services),
			controllerLister: schedulertesting.FakeControllerLister(test.rcs),
			replicaSetLister: schedulertesting.FakeReplicaSetLister(test.rss),
		}
		list, err := nameSpacePriority.CalculateNodeNameSpacePriority(test.pod, nodeNameToInfo, test.nodes)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(test.expectedList, list) {
			t.Errorf("%s: expected %#v, got %#v", test.test, test.expectedList, list)
		}
	}
}
