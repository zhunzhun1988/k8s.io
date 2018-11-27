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

package utils

import (
	"k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
)

type ApiClientInterface interface {
	GetPods(namespace, nodeName string) ([]*v1.Pod, error)
	GetPod(namespace, name string) (*v1.Pod, error)
	GetUnschedulablePod(namespace string) ([]*v1.Pod, error)
	DeletePod(namespace, name string, grace int64) error
	CreatePod(podCreate *v1.Pod) (*v1.Pod, error)
	GetReplications(namespace string) ([]*v1.ReplicationController, error)
	GetReplication(namespace, name string) (*v1.ReplicationController, error)
	UpdateReplication(rc *v1.ReplicationController) (*v1.ReplicationController, error)
	GetReplicaSets(namespace string) ([]*extensions.ReplicaSet, error)
	GetReplicaSet(namespace, name string) (*extensions.ReplicaSet, error)
	UpdateReplicaSet(rs *extensions.ReplicaSet) (*extensions.ReplicaSet, error)
	UpdateNode(node *v1.Node) (*v1.Node, error)
	UpdatePod(pod *v1.Pod) (*v1.Pod, error)
	GetNode(name string) (*v1.Node, error)
	ListReadyNodes() []*v1.Node
	ListNamespace() []*v1.Namespace
	ListPVS() []*v1.PersistentVolume
}

type ApiClient struct {
	client *clientset.Clientset
	cache  *ReschedulerCache
}

func NewApiClient(client *clientset.Clientset, cache *ReschedulerCache) *ApiClient {
	return &ApiClient{
		client: client,
		cache:  cache,
	}
}
func (apiClient *ApiClient) ListReadyNodes() []*v1.Node {
	nodes, err := apiClient.cache.NodeLister.List(labels.Everything())
	if err == nil {
		ret := make([]*v1.Node, 0, len(nodes))

		for _, node := range nodes {
			if node.Spec.Unschedulable {
				continue
			}
			for _, condition := range node.Status.Conditions {
				if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
					ret = append(ret, node)
					break
				}
			}
		}
		return ret
	} else {
		return []*v1.Node{}
	}
}

func (apiClient *ApiClient) GetPod(namespace, name string) (*v1.Pod, error) {
	return apiClient.client.CoreV1().Pods(namespace).Get(name, metav1.GetOptions{})
}

func (apiClient *ApiClient) GetPods(namespace, nodeName string) ([]*v1.Pod, error) {
	podsOnNode, err := apiClient.cache.PodLister.Pods(namespace).List(labels.Everything())
	if err != nil {
		returnNull := make([]*v1.Pod, 0)
		return returnNull, err
	}
	ret := make([]*v1.Pod, 0, len(podsOnNode))
	for _, p := range podsOnNode {
		if p.Spec.NodeName == nodeName /*&& p.Status.Phase == v1.PodRunning*/ {
			ret = append(ret, p)
		}
	}
	return ret, nil
}

func (apiClient *ApiClient) GetUnschedulablePod(namespace string) ([]*v1.Pod, error) {
	pods, err := apiClient.cache.PodLister.Pods(namespace).List(labels.Everything())
	if err != nil {
		returnNull := make([]*v1.Pod, 0)
		return returnNull, err
	}
	ret := make([]*v1.Pod, 0, len(pods))
	for _, p := range pods {
		if p.Spec.NodeName == "" && p.Status.Phase != v1.PodSucceeded && p.Status.Phase != v1.PodFailed {
			ret = append(ret, p)
		}
	}
	return ret, nil
}

func (apiClient *ApiClient) ListNamespace() []*v1.Namespace {
	allNS, err := apiClient.cache.NamespaceLister.List(labels.Everything())

	if err != nil {
		return []*v1.Namespace{}
	}
	return allNS
}

func (apiClient *ApiClient) DeletePod(namespace, name string, grace int64) error {
	delErr := apiClient.client.CoreV1().Pods(namespace).Delete(name, metav1.NewDeleteOptions(grace))
	return delErr
}
func (apiClient *ApiClient) CreatePod(podCreate *v1.Pod) (*v1.Pod, error) {
	pod, errCreate := apiClient.client.CoreV1().Pods(podCreate.Namespace).Create(podCreate)
	return pod, errCreate
}

func (apiClient *ApiClient) GetReplications(namespace string) ([]*v1.ReplicationController, error) {
	rcs := make([]*v1.ReplicationController, 0)
	rcList, err := apiClient.cache.ControllerLister.ReplicationControllers(namespace).List(labels.Everything())
	if err != nil {
		return rcs, err
	}
	return rcList, nil
}
func (apiClient *ApiClient) GetReplication(namespace, name string) (*v1.ReplicationController, error) {
	rc, errGet := apiClient.client.CoreV1().ReplicationControllers(namespace).Get(name, metav1.GetOptions{})
	return rc, errGet
}
func (apiClient *ApiClient) UpdateReplication(rc *v1.ReplicationController) (*v1.ReplicationController, error) {
	rcUpdate, err := apiClient.client.CoreV1().ReplicationControllers(rc.Namespace).Update(rc)
	return rcUpdate, err
}
func (apiClient *ApiClient) GetReplicaSets(namespace string) ([]*extensions.ReplicaSet, error) {
	rss := make([]*extensions.ReplicaSet, 0)
	rsList, err := apiClient.cache.ReplicaSetLister.ReplicaSets(namespace).List(labels.Everything())
	if err != nil {
		return rss, err
	}
	return rsList, nil
}
func (apiClient *ApiClient) GetReplicaSet(namespace, name string) (*extensions.ReplicaSet, error) {
	rs, errGet := apiClient.client.Extensions().ReplicaSets(namespace).Get(name, metav1.GetOptions{})
	return rs, errGet
}

func (apiClient *ApiClient) UpdateReplicaSet(rs *extensions.ReplicaSet) (*extensions.ReplicaSet, error) {
	rsUpdate, err := apiClient.client.Extensions().ReplicaSets(rs.Namespace).Update(rs)
	return rsUpdate, err
}
func (apiClient *ApiClient) UpdateNode(node *v1.Node) (*v1.Node, error) {
	updateNode, err := apiClient.client.CoreV1().Nodes().Update(node)
	return updateNode, err
}
func (apiClient *ApiClient) GetNode(name string) (*v1.Node, error) {
	node, errGetNode := apiClient.client.CoreV1().Nodes().Get(name, metav1.GetOptions{})
	return node, errGetNode
}

func (apiClient *ApiClient) UpdatePod(pod *v1.Pod) (*v1.Pod, error) {
	return apiClient.client.CoreV1().Pods(pod.Namespace).Update(pod)
}

func (apiClient *ApiClient) ListPVS() []*v1.PersistentVolume {
	pvs, err := apiClient.client.CoreV1().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return []*v1.PersistentVolume{}
	}
	ret := make([]*v1.PersistentVolume, 0, len(pvs.Items))
	for i, _ := range pvs.Items {
		ret = append(ret, &pvs.Items[i])
	}
	return ret
}
