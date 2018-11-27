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

	"k8s.io/client-go/tools/cache"
	//"k8s.io/kubernetes/pkg/api"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	corelisters "k8s.io/client-go/listers/core/v1"
	extensionslisters "k8s.io/client-go/listers/extensions/v1beta1"
	"k8s.io/kubernetes/pkg/controller"
)

type ReschedulerCache struct {
	//kubeClient *clientset.Clientset
	// a means to list all nodes
	NodeLister corelisters.NodeLister
	// a means to list all known scheduled pods.
	PodLister corelisters.PodLister
	// a means to list all replicasets
	ReplicaSetLister extensionslisters.ReplicaSetLister
	// a means to list all controllers
	ControllerLister corelisters.ReplicationControllerLister
	// a means to list all namespaces
	NamespaceLister corelisters.NamespaceLister

	startCacheFunc func()
	stopCacheFunc  func()

	nodeInformerSynced cache.InformerSynced
	podInformerSynced  cache.InformerSynced
	rcInformerSynced   cache.InformerSynced
	rsInformerSynced   cache.InformerSynced
	nsInformerSynced   cache.InformerSynced
}

func NewReschedulerCache(clientBuilder controller.ControllerClientBuilder) *ReschedulerCache {
	stopEverything := make(chan struct{})
	versionedClient := clientBuilder.ClientOrDie("shared-informers")
	sharedInformers := informers.NewSharedInformerFactory(versionedClient, 0)
	nodeInformer := sharedInformers.Core().V1().Nodes()
	podInformer := sharedInformers.Core().V1().Pods()
	rcInformer := sharedInformers.Core().V1().ReplicationControllers()
	rsInformer := sharedInformers.Extensions().V1beta1().ReplicaSets()
	nsInformer := sharedInformers.Core().V1().Namespaces()
	c := &ReschedulerCache{
		//kubeClient:         kubeClient,
		NodeLister:         nodeInformer.Lister(),
		PodLister:          podInformer.Lister(),
		ReplicaSetLister:   rsInformer.Lister(),
		ControllerLister:   rcInformer.Lister(),
		NamespaceLister:    nsInformer.Lister(),
		nodeInformerSynced: nodeInformer.Informer().HasSynced,
		podInformerSynced:  podInformer.Informer().HasSynced,
		rcInformerSynced:   rcInformer.Informer().HasSynced,
		rsInformerSynced:   rsInformer.Informer().HasSynced,
		nsInformerSynced:   nsInformer.Informer().HasSynced,
	}
	c.startCacheFunc = func() {
		sharedInformers.Start(stopEverything)
	}
	c.stopCacheFunc = func() {
		close(stopEverything)
	}
	return c
}

func (rsc *ReschedulerCache) Start() {
	rsc.startCacheFunc()
}
func (rsc *ReschedulerCache) Stop() {
	rsc.stopCacheFunc()
}

func (rsc *ReschedulerCache) Wait() error {
	if !cache.WaitForCacheSync(wait.NeverStop,
		rsc.nodeInformerSynced,
		rsc.podInformerSynced,
		rsc.rcInformerSynced,
		rsc.rsInformerSynced,
		rsc.nsInformerSynced,
	) {
		return fmt.Errorf("timed out waiting for caches to sync")
	}
	return nil
}

/*func (rsc *ReschedulerCache) createAssignedNonTerminatedPodLW() *cache.ListWatch {
	selector := fields.ParseSelectorOrDie("spec.nodeName!=" + "" + ",status.phase!=" + string(api.PodSucceeded) + ",status.phase!=" + string(api.PodFailed))
	return cache.NewListWatchFromClient(rsc.kubeClient.Core().RESTClient(), "pods", api.NamespaceAll, selector)
}*/
