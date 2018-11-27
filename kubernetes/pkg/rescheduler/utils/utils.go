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
	"fmt"
	"sync"

	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/types"
)

const (
	CreatedByAnnotation   = "kubernetes.io/created-by"
	CriticalPodAnnotation = "CriticalAddonsOnly"
)

func PodId(pod *v1.Pod) string {
	return fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)
}

// Thread safe implementation of set of Pods.
type PodSet struct {
	set   map[string]struct{}
	mutex sync.Mutex
}

// NewPodSet creates new instance of PodSet.
func NewPodSet() *PodSet {
	return &PodSet{
		set:   make(map[string]struct{}),
		mutex: sync.Mutex{},
	}
}

// Add the pod to the set.
func (s *PodSet) Add(pod *v1.Pod) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.set[PodId(pod)] = struct{}{}
}

// Remove the pod from set.
func (s *PodSet) Remove(pod *v1.Pod) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.set, PodId(pod))
}

// Has checks whether the pod is in the set.
func (s *PodSet) Has(pod *v1.Pod) bool {
	return s.HasId(PodId(pod))
}

// HasId checks whether the pod is in the set.
func (s *PodSet) HasId(pod string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, found := s.set[pod]
	return found
}

// Add the key to the set.
func (s *PodSet) AddKey(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.set[key] = struct{}{}
}

// Remove the pod from set.
func (s *PodSet) RemoveByKey(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.set, key)
}

type ObjectSet struct {
	set    map[string]struct{}
	idFunc func(interface{}) string
	mutex  sync.Mutex "k8s.io/kubernetes/pkg/controller"
}

func NewObjectSet(idFunc func(interface{}) string) *ObjectSet {
	return &ObjectSet{
		set:    make(map[string]struct{}),
		idFunc: idFunc,
		mutex:  sync.Mutex{},
	}
}

func (s *ObjectSet) Add(o interface{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.set[s.idFunc(o)] = struct{}{}
}

func (s *ObjectSet) Remove(o interface{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.set, s.idFunc(o))
}

func (s *ObjectSet) Has(o interface{}) bool {
	return s.HasId(s.idFunc(o))
}

func (s *ObjectSet) HasId(str string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, found := s.set[str]
	return found
}

// CreatorRefKind returns the kind of the creator of the pod.
func CreatorRefKind(pod *v1.Pod) (string, error) {
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

// IsMirrorPod checks whether the pod is a mirror pod.
func IsMirrorPod(pod *v1.Pod) bool {
	_, found := pod.ObjectMeta.Annotations[types.ConfigMirrorAnnotationKey]
	return found
}

func IsCriticalPod(pod *v1.Pod) bool {
	_, found := pod.ObjectMeta.Annotations[CriticalPodAnnotation]
	return found
}

func AddTolerations(updateFunc func(pod *v1.Pod) (*v1.Pod, error), pod *v1.Pod) error {
	if pod.Spec.Tolerations == nil {
		pod.Spec.Tolerations = make([]v1.Toleration, 0)
	}
	for i, _ := range pod.Spec.Tolerations {
		if pod.Spec.Tolerations[i].Key == CriticalPodAnnotation {
			return nil
		}
	}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, v1.Toleration{
		Key:    CriticalPodAnnotation,
		Value:  PodId(pod),
		Effect: v1.TaintEffectNoSchedule,
	})
	if _, err := updateFunc(pod); err != nil {
		return err
	}
	return nil
}

func AddTaint(updateFunc func(node *v1.Node) (*v1.Node, error), node *v1.Node, value string) error {
	node.Spec.Taints = append(node.Spec.Taints, v1.Taint{
		Key:    CriticalPodAnnotation,
		Value:  value,
		Effect: v1.TaintEffectNoSchedule,
	})

	if _, err := updateFunc(node); err != nil {
		return err
	}
	return nil
}
