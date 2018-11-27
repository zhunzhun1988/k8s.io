/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package simulator

import (
	"fmt"

	kube_api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	// We need to import provider to intialize default scheduler.
	"k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/kubernetes/pkg/scheduler/algorithmprovider"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"k8s.io/kubernetes/pkg/scheduler/factory"
)

// PredicateChecker checks whether all required predicates are matched for given Pod and Node
type PredicateChecker struct {
	predicates map[string]algorithm.FitPredicate
}

// NewPredicateChecker builds PredicateChecker.
func NewPredicateChecker(kubeClient *clientset.Clientset, providerName string, stop chan struct{}) (*PredicateChecker, error) {
	provider, err := factory.GetAlgorithmProvider(providerName)
	if err != nil {
		return nil, err
	}
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)

	schedulerConfigFactory := factory.NewConfigFactory(
		"",
		kubeClient,
		informerFactory.Core().V1().Nodes(),
		factory.NewPodInformer(kubeClient, 0),
		informerFactory.Core().V1().PersistentVolumes(),
		informerFactory.Core().V1().PersistentVolumeClaims(),
		informerFactory.Core().V1().ReplicationControllers(),
		informerFactory.Extensions().V1beta1().ReplicaSets(),
		informerFactory.Apps().V1beta1().StatefulSets(),
		informerFactory.Core().V1().Services(),
		informerFactory.Policy().V1beta1().PodDisruptionBudgets(),
		informerFactory.Storage().V1().StorageClasses(),
		kube_api.DefaultHardPodAffinitySymmetricWeight,
		false,
		true,
	)

	informerFactory.Start(stop)
	predicates, err := schedulerConfigFactory.GetPredicates(provider.FitPredicateKeys)
	if err != nil {
		return nil, err
	}
	//schedulerConfigFactory.Run()
	return &PredicateChecker{
		predicates: predicates,
	}, nil
}

// NewTestPredicateChecker builds test version of PredicateChecker.
func NewTestPredicateChecker() *PredicateChecker {
	return &PredicateChecker{
		predicates: map[string]algorithm.FitPredicate{
			"default": predicates.GeneralPredicates,
		},
	}
}

// FitsAny checks if the given pod can be place on any of the given nodes.
func (p *PredicateChecker) FitsAny(pod *v1.Pod, nodeInfos map[string]*schedulercache.NodeInfo) (string, error) {
	for name, nodeInfo := range nodeInfos {
		if err := p.CheckPredicates(pod, nodeInfo); err == nil {
			return name, nil
		}
	}
	return "", fmt.Errorf("cannot put pod %s on any node", pod.Name)
}

// CheckPredicates checks if the given pod can be placed on the given node.
func (p *PredicateChecker) CheckPredicates(pod *v1.Pod, nodeInfo *schedulercache.NodeInfo) error {
	for _, predicate := range p.predicates {
		match, reasons, err := predicate(pod, nil, nodeInfo)
		nodename := "unknown"
		if nodeInfo.Node() != nil {
			nodename = nodeInfo.Node().Name
		}

		if err != nil {
			return fmt.Errorf("cannot put %s on %s due to %v: reason=%v", pod.Name, nodename, err, reasons)
		}
		if !match {
			return fmt.Errorf("cannot put %s on %s", pod.Name, nodename)
		}
	}
	return nil
}
