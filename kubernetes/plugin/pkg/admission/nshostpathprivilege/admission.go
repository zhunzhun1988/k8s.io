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

package nshostpathprivilege

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	informers "k8s.io/kubernetes/pkg/client/informers/informers_generated/internalversion"
	corelisters "k8s.io/kubernetes/pkg/client/listers/core/internalversion"
	kubeapiserveradmission "k8s.io/kubernetes/pkg/kubeapiserver/admission"
)

// PluginName is the name of this admission plugin
const (
	PluginName                 = "NsHostPathAndPrivilege"
	NamespaceAllowHostPathAnn  = "io.enndata.namespace/alpha-allowhostpath"
	NamespaceAllowPrivilegeAnn = "io.enndata.namespace/alpha-allowprivilege"
)

func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		hostPathAndPrivilegeAdmission := NewNsHostPathAndPrivilege()
		return hostPathAndPrivilegeAdmission, nil
	})
}

var _ = admission.Interface(&nsHostPathAndPrivilege{})

type nsHostPathAndPrivilege struct {
	*admission.Handler

	client internalclientset.Interface

	namespacesLister corelisters.NamespaceLister
}

var _ = kubeapiserveradmission.WantsInternalKubeClientSet(&nsHostPathAndPrivilege{})
var _ = kubeapiserveradmission.WantsInternalKubeInformerFactory(&nsHostPathAndPrivilege{})

// NewNsHostPathAndPrivilege returns an admission.Interface implementation which limits admission of Pod CREATE
// if
func NewNsHostPathAndPrivilege() *nsHostPathAndPrivilege {
	return &nsHostPathAndPrivilege{
		Handler: admission.NewHandler(admission.Create),
	}
}

func (a *nsHostPathAndPrivilege) ValidateInitialization() error {
	if a.client == nil {
		return fmt.Errorf("missing client")
	}
	if a.namespacesLister == nil {
		return fmt.Errorf("missing namespacesLister lister")
	}
	return nil
}

func (a *nsHostPathAndPrivilege) SetInternalKubeClientSet(cl internalclientset.Interface) {
	a.client = cl
}

func (a *nsHostPathAndPrivilege) SetInternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespacesInformer := f.Core().InternalVersion().Namespaces()
	a.namespacesLister = namespacesInformer.Lister()
}

// Validate ensures an authorizer is set.
func (a *nsHostPathAndPrivilege) Validate() error {
	if a.client == nil {
		return fmt.Errorf("missing client")
	}
	if a.namespacesLister == nil {
		return fmt.Errorf("missing namespacesLister")
	}
	return nil
}

func (s *nsHostPathAndPrivilege) Admit(a admission.Attributes) (err error) {
	if a.GetResource().GroupResource() != api.Resource("pods") {
		return nil
	}
	obj := a.GetObject()
	if obj == nil {
		return nil
	}
	pod, ok := obj.(*api.Pod)
	if !ok {
		return nil
	}
	ns, errGet := s.getNamespace(a.GetNamespace())

	useHostPath := isPodUseHostPath(pod)
	if useHostPath {
		if ns == nil {
			return admission.NewForbidden(a, fmt.Errorf("pod use hostpath get %s: %v", a.GetNamespace(), errGet))
		}
		if isNamespaceAllowHostPath(ns) == false {
			return admission.NewForbidden(a, fmt.Errorf("namespace %s: not support hostpath", a.GetNamespace()))
		}
	}

	usePrivilege := isPodPrivilge(pod)
	if usePrivilege {
		if ns == nil {
			return admission.NewForbidden(a, fmt.Errorf("pod use privilege get %s: %v", a.GetNamespace(), errGet))
		}
		if isNamespaceAllowPrivilege(ns) == false {
			return admission.NewForbidden(a, fmt.Errorf("namespace %s: not support privilege", a.GetNamespace()))
		}
	}
	return nil
}

func isPodUseHostPath(pod *api.Pod) bool {
	if pod == nil {
		return false
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.HostPath != nil {
			return true
		}
	}
	return false
}

func isPodPrivilge(pod *api.Pod) bool {
	if pod == nil {
		return false
	}
	for _, c := range pod.Spec.Containers {
		if c.SecurityContext != nil && c.SecurityContext.Privileged != nil && *c.SecurityContext.Privileged == true {
			return true
		}
	}
	for _, c := range pod.Spec.InitContainers {
		if c.SecurityContext != nil && c.SecurityContext.Privileged != nil && *c.SecurityContext.Privileged == true {
			return true
		}
	}
	return false
}
func isNamespaceAllowHostPath(ns *api.Namespace) bool {
	if ns == nil || ns.Annotations == nil || ns.Annotations[NamespaceAllowHostPathAnn] != "true" {
		return false
	}
	return true
}
func isNamespaceAllowPrivilege(ns *api.Namespace) bool {
	if ns == nil || ns.Annotations == nil || ns.Annotations[NamespaceAllowPrivilegeAnn] != "true" {
		return false
	}
	return true
}

func (s *nsHostPathAndPrivilege) getNamespace(name string) (*api.Namespace, error) {
	ns, err := s.namespacesLister.Get(name)
	if err == nil {
		return ns, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	// Could not find in cache, attempt to look up directly
	numAttempts := 3
	retryInterval := time.Duration(rand.Int63n(100)+int64(100)) * time.Millisecond
	for i := 0; i < numAttempts; i++ {
		if i != 0 {
			time.Sleep(retryInterval)
		}
		ns, err := s.client.Core().Namespaces().Get(name, metav1.GetOptions{})
		if err == nil {
			return ns, nil
		}
		if !errors.IsNotFound(err) {
			return nil, err
		}
	}

	return nil, nil
}
