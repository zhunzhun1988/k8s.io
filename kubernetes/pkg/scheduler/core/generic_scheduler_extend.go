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

package core

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
)

type PredicatePriorityGeter interface {
	GetPredicates(predicateKeys sets.String) (map[string]algorithm.FitPredicate, error)
	GetPriorityFunctionConfigs(priorityKeys sets.String) ([]algorithm.PriorityConfig, error)
}

func (g *genericScheduler) GetPredicates() map[string]algorithm.FitPredicate {
	g.muPredicate.RLock()
	defer g.muPredicate.RUnlock()
	ret := make(map[string]algorithm.FitPredicate)
	for name, pre := range g.predicates {
		ret[name] = pre
	}
	return ret
}

func (g *genericScheduler) HasPredicate(predicatName string) bool {
	g.muPredicate.RLock()
	defer g.muPredicate.RUnlock()
	_, exist := g.predicates[predicatName]
	return exist
}

func (g *genericScheduler) AddPredicate(predicatName string) (bool, error) {
	if g.predicatePriorityGeter == nil {
		return false, fmt.Errorf("g.predicatePriorityGeter==nil")
	}

	set := sets.NewString(predicatName)
	ret, err := g.predicatePriorityGeter.GetPredicates(set)
	if err != nil {
		return false, err
	}
	predicate, ok := ret[predicatName]

	if ok == false || predicate == nil {
		return false, fmt.Errorf("unknow error")
	}

	g.muPredicate.Lock()
	defer g.muPredicate.Unlock()
	g.predicates[predicatName] = predicate
	return true, nil
}

func (g *genericScheduler) RemovePredicate(predicatName string) (bool, error) {
	g.muPredicate.Lock()
	defer g.muPredicate.Unlock()
	delete(g.predicates, predicatName)
	return true, nil
}

func (g *genericScheduler) GetPrioritys() []algorithm.PriorityConfig {
	g.muPriority.RLock()
	defer g.muPriority.RUnlock()
	ret := make([]algorithm.PriorityConfig, 0, len(g.prioritizers))
	for _, priority := range g.prioritizers {
		ret = append(ret, priority)
	}
	return ret
}

func (g *genericScheduler) HasPriority(priorityName string) bool {
	g.muPriority.RLock()
	defer g.muPriority.RUnlock()
	for _, priority := range g.prioritizers {
		if priority.Name == priorityName {
			return true
		}
	}
	return false
}

func (g *genericScheduler) AddPriority(priorityName string) (bool, error) {
	if g.predicatePriorityGeter == nil {
		return false, fmt.Errorf("g.predicatePriorityGeter==nil")
	}
	set := sets.NewString(priorityName)
	prioritys, err := g.predicatePriorityGeter.GetPriorityFunctionConfigs(set)
	if err != nil {
		return false, err
	}
	if len(prioritys) != 1 {
		return false, fmt.Errorf("unknow error")
	}

	g.muPriority.Lock()
	defer g.muPriority.Unlock()
	for i, _ := range g.prioritizers {
		if g.prioritizers[i].Name == priorityName {
			return true, nil
		}
	}
	g.prioritizers = append(g.prioritizers, prioritys...)
	return true, nil
}

func (g *genericScheduler) RemovePriority(priorityName string) (bool, error) {
	g.muPriority.Lock()
	defer g.muPriority.Unlock()
	for i, _ := range g.prioritizers {
		if g.prioritizers[i].Name == priorityName {
			g.prioritizers = append(g.prioritizers[0:i], g.prioritizers[i+1:]...)
			return true, nil
		}
	}
	return false, fmt.Errorf("%s is not exist", priorityName)
}
