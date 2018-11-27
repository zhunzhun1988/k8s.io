package predicates

import (
	"fmt"
	"reflect"
	"testing"

	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

func TestNamespaceNodeSelector(t *testing.T) {

	tests := []struct {
		NsConfig NsConfig

		podNamespace string
		nodelabel    map[string]string
		fits         bool
		test         string
	}{
		{
			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{},
			},

			podNamespace: "namespace1",
			nodelabel:    map[string]string{Nsnodeselector_systemlabel: "issystemnode"},
			fits:         false,
			test:         "pod can not be schedulered when pod namespace specify systemnode and NsConfig is not defined",
		},
		{
			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{
					NotMatch: LabelValues{
						Nsnodeselector_systemlabel: map[string]struct{}{"*": struct{}{}}},
				},
			},

			podNamespace: "namespace1",
			nodelabel:    map[string]string{Nsnodeselector_systemlabel: "issystemnode"},
			fits:         false,
			test: "pod can not be schedulered when pod namespace specify systemnode and NsConfig is defined as default" +
				"same with nil NsConfig",
		},
		{
			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{},
			},

			podNamespace: "namespace1",
			nodelabel:    map[string]string{},
			fits:         true,
			test:         "pod can be schedulered when node label is not systemnode",
		},
		{
			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{},
			},
			podNamespace: "namespace2",
			nodelabel:    map[string]string{},
			fits:         true,
			test:         "pod can be schedulered when pod Namespace did not defined in NsConfig and node is not system node",
		},
		{
			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{},
			},
			podNamespace: "namespace2",
			nodelabel:    map[string]string{Nsnodeselector_systemlabel: "issystemnode"},
			fits:         false,
			test:         "pod can not be schedulered when pod Namespace did not defined in NsConfig and node is system node",
		},
		{
			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{
					Match: LabelValues{
						Nsnodeselector_systemlabel: map[string]struct{}{"*": struct{}{}},
					},
					NotMatch: LabelValues{},
				},
			},
			podNamespace: "namespace1",
			nodelabel:    map[string]string{Nsnodeselector_systemlabel: "issystemnode"},
			fits:         true,
			test: "pod can be schedulered to systemnode when NsConfig Match policy is on and " +
				"default policy(NotMatch) cannot be added to NsConfig",
		},
		{

			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{
					Match: LabelValues{
						"kubernetes.io/hostname": map[string]struct{}{"*": struct{}{}}},
					NotMatch: LabelValues{
						"kubernetes.io/hostname": map[string]struct{}{"*": struct{}{}}},
				},
			},
			podNamespace: "namespace1",
			nodelabel:    map[string]string{"kubernetes.io/hostname": ""},
			fits:         false,
			test:         "conflictive NsConfig policy",
		},
		{

			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{
					Match: LabelValues{
						Nsnodeselector_systemlabel: map[string]struct{}{"*": struct{}{}},
						"kubernetes.io/hostname":   map[string]struct{}{"*": struct{}{}},
					},
					NotMatch: LabelValues{},
				},
			},
			podNamespace: "namespace1",
			nodelabel:    map[string]string{"kubernetes.io/hostname": "a"},
			fits:         false,
			test:         "use two match LabelVaules in Nsconfig",
		},
		{

			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{
					Match: LabelValues{
						Nsnodeselector_systemlabel: map[string]struct{}{"*": struct{}{}},
						"kubernetes.io/hostname":   map[string]struct{}{"*": struct{}{}},
					},
					NotMatch: LabelValues{},
				},
			},
			podNamespace: "namespace1",
			nodelabel:    map[string]string{Nsnodeselector_systemlabel: "", "kubernetes.io/hostname": ""},
			fits:         true,
			test:         "use two match LabelVaules in Nsconfig,must satisfy both",
		},
		{

			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{
					NotMatch: LabelValues{
						"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}, "152": struct{}{}}},
				},
			},
			podNamespace: "namespace1",
			nodelabel:    map[string]string{"kubernetes.io/hostname": "151"},
			fits:         false,
			test:         "pod can not be schedulered to node when NsConfig NotMatch specify the same node",
		},
		{

			NsConfig: map[string]NsConfigItem{
				"namespace1": NsConfigItem{
					Match: LabelValues{
						"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}, "152": struct{}{}}},
					NotMatch: LabelValues{},
				},
			},
			podNamespace: "namespace1",
			nodelabel:    map[string]string{"kubernetes.io/hostname": "151"},
			fits:         true,
			test:         "pod can be schedulered to node when NsConfig Match specify the same node",
		},
	}

	expectedFailureReasons := []algorithm.PredicateFailureReason{ErrNsNodeSelectorNotMatch}
	for _, test := range tests {

		cm := &v1.ConfigMap{
			Data: map[string]string{},
		}

		SetNsNodeSelectorConfigMapNsConfig(cm, test.NsConfig)

		fakeCmGet := func() *v1.ConfigMap {
			return cm
		}

		NsNS := &NsNodeSelector{
			cmGeter: fakeCmGet,
		}

		fmt.Println("nsnodeselector.go test begin")
		node := v1.Node{ObjectMeta: meta_v1.ObjectMeta{Labels: test.nodelabel}}
		nodeInfo := schedulercache.NewNodeInfo()
		nodeInfo.SetNode(&node)

		pod := &v1.Pod{ObjectMeta: meta_v1.ObjectMeta{Namespace: test.podNamespace}}
		fits, reasons, err := NsNS.CheckNamespaceNodeSelector(pod, PredicateMetadata(pod, nil), nodeInfo)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !fits && !reflect.DeepEqual(reasons, expectedFailureReasons) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, reasons, expectedFailureReasons)
		}
		if fits != test.fits {
			t.Errorf("%s: expected: %v got %v", test.test, test.fits, fits)
		}
	}

}
