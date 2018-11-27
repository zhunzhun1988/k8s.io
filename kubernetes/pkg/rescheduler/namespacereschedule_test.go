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
	"fmt"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ca_simulator "k8s.io/kubernetes/cmd/kube-rescheduler/simulator"
	rs_utils "k8s.io/kubernetes/pkg/rescheduler/utils"
)

type FakeClient struct {
	ExistingNodes      []*v1.Node
	ExistingRC         []*v1.ReplicationController
	ExistingRS         []*extensions.ReplicaSet
	ExistingPods       []*v1.Pod
	ExistingNamespaces []*v1.Namespace

	// Output
	DeletedPods []*v1.Pod
	CreatePods  []*v1.Pod

	lock sync.Mutex
}

func (fakeClient *FakeClient) GetPods(namespace, nodeName string) ([]*v1.Pod, error) {
	ret := make([]*v1.Pod, 0, len(fakeClient.ExistingPods))
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()

	for _, pod := range fakeClient.ExistingPods {
		if (pod.Namespace == namespace || v1.NamespaceAll == namespace) && pod.Spec.NodeName == nodeName {
			ret = append(ret, pod)
		}
	}
	return ret, nil
}
func (fakeClient *FakeClient) GetPod(namespace, name string) (*v1.Pod, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()

	for _, pod := range fakeClient.ExistingPods {
		if (pod.Namespace == namespace || v1.NamespaceAll == namespace) && pod.Name == name {
			return pod, nil
		}
	}
	return nil, fmt.Errorf("Not Found")
}

func (fakeClient *FakeClient) GetUnschedulablePod(namespace string) ([]*v1.Pod, error) {
	ret := make([]*v1.Pod, 0, len(fakeClient.ExistingPods))
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()

	for _, pod := range fakeClient.ExistingPods {
		if (pod.Namespace == namespace || v1.NamespaceAll == namespace) && pod.Spec.NodeName == "" &&
			pod.Status.Phase != v1.PodSucceeded && pod.Status.Phase != v1.PodFailed {
			ret = append(ret, pod)
		}
	}
	return ret, nil
}
func (fakeClient *FakeClient) DeletePod(namespace, name string, grace int64) error {
	ret := make([]*v1.Pod, 0, len(fakeClient.ExistingPods))
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	find := false

	for _, pod := range fakeClient.ExistingPods {
		if pod.Namespace != namespace || pod.Name != name {
			ret = append(ret, pod)
		} else {
			find = true
			fakeClient.DeletedPods = append(fakeClient.DeletedPods, pod)
		}
	}
	if find {
		fakeClient.ExistingPods = ret
		return nil
	} else {
		return fmt.Errorf("pod %s:%s not find", namespace, name)
	}
}
func (fakeClient *FakeClient) CreatePod(podCreate *v1.Pod) (*v1.Pod, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()

	for _, pod := range fakeClient.ExistingPods {
		if podCreate.Namespace == pod.Namespace && podCreate.Name == pod.Name {
			return nil, fmt.Errorf("pod %s:%s is exist", podCreate.Namespace, podCreate.Name)
		}
	}
	fakeClient.ExistingPods = append(fakeClient.ExistingPods, podCreate)
	fakeClient.CreatePods = append(fakeClient.CreatePods, podCreate)
	return podCreate, nil
}

func (fakeClient *FakeClient) GetReplications(namespace string) ([]*v1.ReplicationController, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	ret := make([]*v1.ReplicationController, 0, len(fakeClient.ExistingRC))
	for _, rc := range fakeClient.ExistingRC {
		if rc.Namespace == namespace || v1.NamespaceAll == namespace {
			ret = append(ret, rc)
		}
	}
	return ret, nil
}
func (fakeClient *FakeClient) GetReplication(namespace, name string) (*v1.ReplicationController, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	for _, rc := range fakeClient.ExistingRC {
		if (rc.Namespace == namespace || v1.NamespaceAll == namespace) && rc.Name == name {
			return rc, nil
		}
	}
	return nil, fmt.Errorf("replication %s:%s is not find", namespace, name)
}
func (fakeClient *FakeClient) UpdateReplication(updateRc *v1.ReplicationController) (*v1.ReplicationController, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	for i, rc := range fakeClient.ExistingRC {
		if rc.Namespace == updateRc.Namespace && rc.Name == updateRc.Name {
			fakeClient.ExistingRC[i] = updateRc
			return updateRc, nil
		}
	}
	return nil, fmt.Errorf("replication %s:%s is not find", updateRc.Namespace, updateRc.Name)
}
func (fakeClient *FakeClient) GetReplicaSets(namespace string) ([]*extensions.ReplicaSet, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	ret := make([]*extensions.ReplicaSet, 0, len(fakeClient.ExistingRS))
	for _, rs := range fakeClient.ExistingRS {
		if rs.Namespace == namespace || v1.NamespaceAll == namespace {
			ret = append(ret, rs)
		}
	}
	return ret, nil
}
func (fakeClient *FakeClient) GetReplicaSet(namespace, name string) (*extensions.ReplicaSet, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	for _, rs := range fakeClient.ExistingRS {
		if (rs.Namespace == namespace || v1.NamespaceAll == namespace) && rs.Name == name {
			return rs, nil
		}
	}
	return nil, fmt.Errorf("replicaSet %s:%s is not find", namespace, name)
}

func (fakeClient *FakeClient) UpdateReplicaSet(updateRs *extensions.ReplicaSet) (*extensions.ReplicaSet, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	for i, rs := range fakeClient.ExistingRS {
		if rs.Namespace == updateRs.Namespace && rs.Name == updateRs.Name {
			fakeClient.ExistingRS[i] = updateRs
			return updateRs, nil
		}
	}
	return nil, fmt.Errorf("replicaSet %s:%s is not find", updateRs.Namespace, updateRs.Name)
}

func (fakeClient *FakeClient) UpdateNode(updateNode *v1.Node) (*v1.Node, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	for i, node := range fakeClient.ExistingNodes {
		if node.Name == updateNode.Name {
			fakeClient.ExistingNodes[i] = updateNode
			return updateNode, nil
		}
	}
	return nil, fmt.Errorf("node %s is not find", updateNode.Name)
}
func (fakeClient *FakeClient) GetNode(name string) (*v1.Node, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	for _, node := range fakeClient.ExistingNodes {
		if node.Name == name {
			return node, nil
		}
	}
	return nil, fmt.Errorf("node %s is not find", name)
}
func (fakeClient *FakeClient) UpdatePod(updatePod *v1.Pod) (*v1.Pod, error) {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	for i, pod := range fakeClient.ExistingPods {
		if pod.Namespace == updatePod.Namespace && pod.Name == updatePod.Name {
			fakeClient.ExistingPods[i] = updatePod
			return updatePod, nil
		}
	}
	return nil, fmt.Errorf("pod %s is not find", updatePod.Name)
}
func (fakeClient *FakeClient) ListPVS() []*v1.PersistentVolume {
	return []*v1.PersistentVolume{}
}
func (fakeClient *FakeClient) ListNamespace() []*v1.Namespace {
	fakeClient.lock.Lock()
	defer fakeClient.lock.Unlock()
	ret := make([]*v1.Namespace, 0, len(fakeClient.ExistingNamespaces))
	for _, ns := range fakeClient.ExistingNamespaces {
		ret = append(ret, ns)
	}
	return ret
}

func (fakeClient *FakeClient) ListReadyNodes() []*v1.Node {
	return fakeClient.ExistingNodes
}

func newNode(nodename string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodename,
		},
	}
}
func newReplicationController(namespace, name string, selector map[string]string) *v1.ReplicationController {
	rc := &v1.ReplicationController{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.ReplicationControllerSpec{
			Selector: selector,
		},
	}
	return rc
}
func newReplicaSet(namespace, name string, selectorMap map[string]string) *extensions.ReplicaSet {
	rs := &extensions.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: extensions.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selectorMap},
		},
	}
	return rs
}
func newNamespace(namespace string, canrescheduler bool) *v1.Namespace {
	canRsStr := "false"
	if canrescheduler == true {
		canRsStr = "true"
	}
	return &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:        namespace,
			Annotations: map[string]string{NSCanRescheduler: canRsStr},
		},
	}
}
func newPod(nodename, namespace, name string, createby runtime.Object, labels map[string]string, status v1.PodPhase) *v1.Pod {
	var conditions []v1.PodCondition
	if status == v1.PodRunning {
		conditions = append(conditions, v1.PodCondition{Type: v1.PodReady, Status: v1.ConditionTrue})
	}

	annotation := make(map[string]string, 0)
	if createby != nil {
		rc, ok := createby.(*v1.ReplicationController)
		if ok == true {
			createdByRefJson := fmt.Sprintf("{\"kind\":\"SerializedReference\",\"apiVersion\":\"v1\",\"reference\":{\"kind\":\"%s\",\"namespace\":\"%s\",\"name\":\"%s\",\"uid\":\"\",\"apiVersion\":\"v1\",\"resourceVersion\":\"\"}}",
				"ReplicationController", rc.Namespace, rc.Name)
			annotation["kubernetes.io/created-by"] = createdByRefJson
		} else {
			rs, okrs := createby.(*extensions.ReplicaSet)
			if okrs {
				createdByRefJson := fmt.Sprintf("{\"kind\":\"SerializedReference\",\"apiVersion\":\"v1\",\"reference\":{\"kind\":\"%s\",\"namespace\":\"%s\",\"name\":\"%s\",\"uid\":\"\",\"apiVersion\":\"v1\",\"resourceVersion\":\"\"}}",
					"ReplicaSet", rs.Namespace, rs.Name)
				annotation["kubernetes.io/created-by"] = createdByRefJson
			}
		}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotation,
			Labels:      labels,
		},
		Spec: v1.PodSpec{
			NodeName: nodename,
		},
		Status: v1.PodStatus{Phase: status, Conditions: conditions},
	}
}

func TestNoPodReschedule(t *testing.T) {
	nodes := []*v1.Node{
		newNode("node0"),
		newNode("node1"),
		newNode("node2"),
	}
	rcs := []*v1.ReplicationController{
		newReplicationController("namespace1", "replication1", map[string]string{"name": "app1"}),
		newReplicationController("namespace1", "replication2", map[string]string{"name": "app2"}),
		newReplicationController("namespace2", "replication3", map[string]string{"name": "app3"}),
	}
	rsc := []*extensions.ReplicaSet{
		newReplicaSet("namespace1", "replicaset1", map[string]string{"name": "app11"}),
		newReplicaSet("namespace1", "replicaset2", map[string]string{"name": "app12"}),
	}
	nss := []*v1.Namespace{
		newNamespace("namespace1", true),
		newNamespace("namespace2", true),
		newNamespace("namespace3", true),
	}
	pods := []*v1.Pod{}
	fakeClient := &FakeClient{
		ExistingNodes:      nodes,
		ExistingRC:         rcs,
		ExistingRS:         rsc,
		ExistingPods:       pods,
		ExistingNamespaces: nss,
	}
	podsBeingProcessed := rs_utils.NewPodSet()
	predicateChecker := &ca_simulator.PredicateChecker{
	//predicates: make(map[string]algorithm.FitPredicate, 0),
	}
	rs := NewNSReschedule(fakeClient, predicateChecker, podsBeingProcessed, false, 0, 0)
	rs.CheckAndReschedule()

	if len(fakeClient.ExistingPods) != 0 {
		t.Errorf("Unexpected  ExistingPods len = %d", len(fakeClient.ExistingPods))
	}
	if len(fakeClient.DeletedPods) != 0 {
		t.Errorf("Unexpected  DeletedPods len = %d", len(fakeClient.DeletedPods))
	}
}
func TestNotneedReschedule(t *testing.T) {
	nodes := []*v1.Node{
		newNode("node0"),
		newNode("node1"),
		newNode("node2"),
		newNode("node3"),
	}
	rcs := []*v1.ReplicationController{
		newReplicationController("namespace1", "replication1", map[string]string{"name": "app1"}),
		newReplicationController("namespace1", "replication2", map[string]string{"name": "app2"}),
		newReplicationController("namespace2", "replication3", map[string]string{"name": "app3"}),
	}
	rsc := []*extensions.ReplicaSet{
		newReplicaSet("namespace1", "replicaset1", map[string]string{"name": "app11"}),
		newReplicaSet("namespace1", "replicaset2", map[string]string{"name": "app12"}),
	}
	pods := []*v1.Pod{
		newPod("node0", "namespace1", "pod1", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node1", "namespace1", "pod2", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node2", "namespace2", "pod3", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
	}
	nss := []*v1.Namespace{
		newNamespace("namespace1", true),
		newNamespace("namespace2", true),
	}
	fakeClient := &FakeClient{
		ExistingNodes:      nodes,
		ExistingRC:         rcs,
		ExistingRS:         rsc,
		ExistingPods:       pods,
		ExistingNamespaces: nss,
	}
	podsBeingProcessed := rs_utils.NewPodSet()
	predicateChecker := &ca_simulator.PredicateChecker{
	//predicates: make(map[string]algorithm.FitPredicate, 0),
	}
	rs := NewNSReschedule(fakeClient, predicateChecker, podsBeingProcessed, false, 0, 0)
	rs.CheckAndReschedule()

	if len(fakeClient.ExistingPods) != 3 {
		t.Errorf("Unexpected  ExistingPods len = %d", len(fakeClient.ExistingPods))
	}
	if len(fakeClient.DeletedPods) != 0 {
		t.Errorf("Unexpected  DeletedPods len = %d", len(fakeClient.DeletedPods))
	}
	if len(fakeClient.CreatePods) != 0 {
		t.Errorf("Unexpected  DeletedPods len = %d", len(fakeClient.CreatePods))
	}
}

func TestCannotReschedule(t *testing.T) {
	nodes := []*v1.Node{
		newNode("node0"),
		newNode("node1"),
		newNode("node2"),
	}
	rcs := []*v1.ReplicationController{
		newReplicationController("namespace1", "replication1", map[string]string{"name": "app1"}),
		newReplicationController("namespace1", "replication2", map[string]string{"name": "app2"}),
		newReplicationController("namespace2", "replication3", map[string]string{"name": "app3"}),
	}
	rsc := []*extensions.ReplicaSet{
		newReplicaSet("namespace1", "replicaset1", map[string]string{"name": "app11"}),
		newReplicaSet("namespace1", "replicaset2", map[string]string{"name": "app12"}),
	}
	pods := []*v1.Pod{
		newPod("node0", "namespace1", "pod1", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node1", "namespace1", "pod2", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node2", "namespace1", "pod3", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node2", "namespace2", "pod4", rcs[2], map[string]string{"name": "app2"}, v1.PodRunning),
	}
	nss := []*v1.Namespace{
		newNamespace("namespace1", true),
		newNamespace("namespace2", true),
	}
	fakeClient := &FakeClient{
		ExistingNodes:      nodes,
		ExistingRC:         rcs,
		ExistingRS:         rsc,
		ExistingPods:       pods,
		ExistingNamespaces: nss,
	}
	podsBeingProcessed := rs_utils.NewPodSet()
	predicateChecker := &ca_simulator.PredicateChecker{
	//predicates: make(map[string]algorithm.FitPredicate, 0),
	}
	rs := NewNSReschedule(fakeClient, predicateChecker, podsBeingProcessed, false, 0, 0)
	rs.CheckAndReschedule()

	if len(fakeClient.ExistingPods) != 4 {
		t.Errorf("Unexpected  ExistingPods len = %d", len(fakeClient.ExistingPods))
	}
	if len(fakeClient.DeletedPods) != 0 {
		t.Errorf("Unexpected  DeletedPods len = %d", len(fakeClient.DeletedPods))
	}
	if len(fakeClient.CreatePods) != 0 {
		t.Errorf("Unexpected  DeletedPods len = %d", len(fakeClient.CreatePods))
	}
}

func TestOnePodReschedule(t *testing.T) {
	nodes := []*v1.Node{
		newNode("node0"),
		newNode("node1"),
		newNode("node2"),
	}
	rcs := []*v1.ReplicationController{
		newReplicationController("namespace1", "replication1", map[string]string{"name": "app1"}),
		newReplicationController("namespace1", "replication2", map[string]string{"name": "app2"}),
		newReplicationController("namespace2", "replication3", map[string]string{"name": "app3"}),
	}
	rsc := []*extensions.ReplicaSet{
		newReplicaSet("namespace1", "replicaset1", map[string]string{"name": "app11"}),
		newReplicaSet("namespace1", "replicaset2", map[string]string{"name": "app12"}),
	}
	pods := []*v1.Pod{
		newPod("node0", "namespace1", "pod1", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node1", "namespace1", "pod2", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node1", "namespace2", "pod3", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
	}
	nss := []*v1.Namespace{
		newNamespace("namespace1", true),
		newNamespace("namespace2", true),
	}
	fakeClient := &FakeClient{
		ExistingNodes:      nodes,
		ExistingRC:         rcs,
		ExistingRS:         rsc,
		ExistingPods:       pods,
		ExistingNamespaces: nss,
	}
	podsBeingProcessed := rs_utils.NewPodSet()
	predicateChecker := &ca_simulator.PredicateChecker{
	//predicates: make(map[string]algorithm.FitPredicate, 0),
	}
	rs := NewNSReschedule(fakeClient, predicateChecker, podsBeingProcessed, false, 0, 0)
	rs.CheckAndReschedule()

	if len(fakeClient.ExistingPods) != 3 {
		t.Errorf("Unexpected  ExistingPods len = %d", len(fakeClient.ExistingPods))
	}
	if len(fakeClient.CreatePods) != 1 {
		t.Errorf("Unexpected  one pod create")
	} else if fakeClient.CreatePods[0].Spec.NodeName != "node2" {
		t.Errorf("Unexpected one pod create at node2")
	}
	if len(fakeClient.DeletedPods) != 1 {
		t.Errorf("Unexpected  DeletedPods len = %d", len(fakeClient.DeletedPods))
	} else if fakeClient.DeletedPods[0].Spec.NodeName != "node1" {
		t.Errorf("Unexpected  delete on node node1")
	}
}

func TestNamespaceCannotReschedule(t *testing.T) {
	nodes := []*v1.Node{
		newNode("node0"),
		newNode("node1"),
		newNode("node2"),
	}
	rcs := []*v1.ReplicationController{
		newReplicationController("namespace1", "replication1", map[string]string{"name": "app1"}),
		newReplicationController("namespace1", "replication2", map[string]string{"name": "app2"}),
		newReplicationController("namespace2", "replication3", map[string]string{"name": "app3"}),
	}
	rsc := []*extensions.ReplicaSet{
		newReplicaSet("namespace1", "replicaset1", map[string]string{"name": "app11"}),
		newReplicaSet("namespace1", "replicaset2", map[string]string{"name": "app12"}),
	}
	pods := []*v1.Pod{
		newPod("node0", "namespace1", "pod1", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node1", "namespace1", "pod2", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
		newPod("node1", "namespace2", "pod3", rcs[0], map[string]string{"name": "app1"}, v1.PodRunning),
	}
	nss := []*v1.Namespace{
		newNamespace("namespace1", false),
		newNamespace("namespace2", false),
	}
	fakeClient := &FakeClient{
		ExistingNodes:      nodes,
		ExistingRC:         rcs,
		ExistingRS:         rsc,
		ExistingPods:       pods,
		ExistingNamespaces: nss,
	}
	podsBeingProcessed := rs_utils.NewPodSet()
	predicateChecker := &ca_simulator.PredicateChecker{
	//predicates: make(map[string]algorithm.FitPredicate, 0),
	}
	rs := NewNSReschedule(fakeClient, predicateChecker, podsBeingProcessed, false, 0, 0)
	rs.CheckAndReschedule()

	if len(fakeClient.ExistingPods) != 3 {
		t.Errorf("Unexpected  ExistingPods len = %d", len(fakeClient.ExistingPods))
	}
	if len(fakeClient.CreatePods) != 0 {
		t.Errorf("Unexpected pod created")
	}
	if len(fakeClient.DeletedPods) != 0 {
		t.Errorf("Unexpected  DeletedPods len = %d", len(fakeClient.DeletedPods))
	}
}
