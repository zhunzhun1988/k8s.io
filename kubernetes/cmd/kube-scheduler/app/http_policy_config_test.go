package app

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	"k8s.io/kubernetes/pkg/scheduler/core"
	"k8s.io/kubernetes/pkg/scheduler/factory"

	clientset "k8s.io/client-go/kubernetes"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"

	"k8s.io/client-go/informers"
	restclient "k8s.io/client-go/rest"
	utiltesting "k8s.io/client-go/util/testing"
)

func jsonConfig(nsConfig map[string]predicates.NsConfigItem) string {
	buf, _ := json.MarshalIndent(nsConfig, " ", "  ")
	return string(buf)
}

func TestRefresh(t *testing.T) {
	tests := []struct {
		FakeConfigMapNodePodHandler clientset.Interface
		willbedeletepods            []string
		remainpods                  []string
		addr                        string
		geturl                      string
		RMsg                        ReturnMsg
		test                        string
	}{
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match: predicates.LabelValues{
										"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{},
								},
							})},
					},
				},
				ExistingPods: []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod0",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node0",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod1",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node1",
						},
					},
				},
				ExistingNodes: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
							Labels: map[string]string{
								"kubernetes.io/hostname": "152",
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
							Labels: map[string]string{
								"kubernetes.io/hostname": "151",
							},
						},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.Namespace{}),
			},
			willbedeletepods: []string{"pod0"},
			remainpods:       []string{"pod1"},
			addr:             ":11761",
			geturl:           "/nsnodeselector/refresh/namespace0",
			RMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "[namespace0:pod0]:node0",
			},
			test: "According to match rule,pod0 is running in wrong node,will be deleted,while pod1 remains",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:    predicates.LabelValues{},
									NotMatch: predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
								},
							})},
					},
				},
				ExistingPods: []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod0",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node0",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod1",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node1",
						},
					},
				},
				ExistingNodes: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
							Labels: map[string]string{
								"kubernetes.io/hostname": "152",
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
							Labels: map[string]string{
								"kubernetes.io/hostname": "151",
							},
						},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.Namespace{}),
			},
			willbedeletepods: []string{"pod1"},
			remainpods:       []string{"pod0"},
			addr:             ":11762",
			geturl:           "/nsnodeselector/refresh/namespace0",
			RMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "[namespace0:pod1]:node1",
			},
			test: "According to notmatch rule, pod1 is running in wrong node,will be deleted while pod0 remains",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									MustMatch: predicates.LabelValues{
										"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{},
								},
							})},
					},
				},
				ExistingPods: []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod0",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node0",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod1",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node1",
						},
					},
				},
				ExistingNodes: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
							Labels: map[string]string{
								"kubernetes.io/hostname": "151",
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
							Labels: map[string]string{
								"kubernetes.io/hostname": "152",
							},
						},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.Namespace{}),
			},

			willbedeletepods: []string{"pod1"},
			remainpods:       []string{"pod0"},
			addr:             ":11763",
			geturl:           "/nsnodeselector/refresh/namespace0",
			RMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "[namespace0:pod1]:node1",
			},
			test: "According to Mustmatch rule,pod1 is running in wrong node, will be deleted,while pod0 remains",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									MustNotMatch: predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
								},
							})},
					},
				},
				ExistingPods: []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod0",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node0",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod1",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node1",
						},
					},
				},
				ExistingNodes: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
							Labels: map[string]string{
								"kubernetes.io/hostname": "152",
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node1",
							Labels: map[string]string{
								"kubernetes.io/hostname": "151",
							},
						},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.Namespace{}),
			},
			willbedeletepods: []string{"pod1"},
			remainpods:       []string{"pod0"},
			addr:             ":11764",
			geturl:           "/nsnodeselector/refresh/namespace0",
			RMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "[namespace0:pod1]:node1",
			},
			test: "According to MustNotmatch rule, pod1 is running in wrong node,will be deleted",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{},
					},
				},
				ExistingPods: []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod0",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node0",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod1",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node1",
						},
					},
				},
				ExistingNodes: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
							Labels: map[string]string{
								"enndata.cn/systemnode": "",
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "node1",
							Labels: map[string]string{},
						},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.Namespace{}),
			},
			willbedeletepods: []string{"pod0"},
			remainpods:       []string{"pod1"},
			addr:             ":11765",
			geturl:           "/nsnodeselector/refresh/namespace0",
			RMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "[namespace0:pod0]:node0",
			},
			test: "no data in configmap,pod can not running in system node",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				ExistingPods: []*v1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod0",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node0",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod1",
							Namespace: "namespace0",
							Labels:    map[string]string{},
						},
						Spec: v1.PodSpec{
							NodeName: "node1",
						},
					},
				},
				ExistingNodes: []*v1.Node{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "node0",
							Labels: map[string]string{
								"enndata.cn/systemnode": "",
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:   "node1",
							Labels: map[string]string{},
						},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.Namespace{}),
			},
			willbedeletepods: []string{"pod0"},
			remainpods:       []string{"pod1"},
			addr:             ":11766",
			geturl:           "/nsnodeselector/refresh/namespace0",
			RMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "[namespace0:pod0]:node0",
			},
			test: "no configmap,pod can not running in system node",
		},
	}
	for _, test := range tests {

		//set configmap nil,client nil
		predicates.SetNsNodeSelectorConfigMapNil()
		SetClientnil()

		c := &SchedulerConfig{
			client: test.FakeConfigMapNodePodHandler,
		}

		//run server
		runserver("", c, test.addr, "", "")

		ret, err := http.Get("http://localhost" + test.addr + test.geturl)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		defer ret.Body.Close()

		body, err := ioutil.ReadAll(ret.Body)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}

		setMsg := ReturnMsg{}

		err = json.Unmarshal(body, &setMsg)

		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !reflect.DeepEqual(setMsg, test.RMsg) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, setMsg, test.RMsg)
		}

		//test and verify delete pods and remain pods
		for _, depod := range test.willbedeletepods {
			getpod, err := test.FakeConfigMapNodePodHandler.CoreV1().Pods("namespace0").Get(depod, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s: unexpected error: %v", test.test, err)
			}
			if getpod != nil {
				t.Errorf("%s: unexpected failure reasons: %s pod is not deleted", test.test, depod)
			}
		}

		for _, repod := range test.remainpods {
			getpod, err := test.FakeConfigMapNodePodHandler.CoreV1().Pods("namespace0").Get(repod, metav1.GetOptions{})
			if err != nil {
				t.Errorf("%s: unexpected error: %v", test.test, err)
			}
			if getpod == nil {
				t.Errorf("%s: unexpected failure reasons: %s pod is deleted", test.test, repod)
			}
		}
	}
}

func TestNsNodeSelectorAddUpdateOrDelete(t *testing.T) {
	tests := []struct {
		FakeConfigMapNodePodHandler clientset.Interface
		addr                        string
		url                         string
		expectMsg                   ReturnMsg
		expectNsConfig              map[string]NsNodeSelectorConfigRet
		test                        string
	}{
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match: predicates.LabelValues{},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11721",
			url:  "/nsnodeselector/add?namespace=namespace0&type=match&key=newkey&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "newkey", Value: "152"},
					},
					NotMatch: []KeyValue{
						{Key: "enndata.cn/systemnode", Value: "*"},
					},
				},
			},
			test: "add nsConfig match keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:    predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},

			addr: ":11722",
			url:  "/nsnodeselector/delete?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match:    []KeyValue{},
					NotMatch: []KeyValue{},
				},
			},
			test: "delete nsConfig match keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:    predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{"newkey": map[string]struct{}{"151": struct{}{}}},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11723",
			url:  "/nsnodeselector/update?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "152"},
					},
					NotMatch: []KeyValue{
						{Key: "newkey", Value: "151"},
					},
				},
			},
			test: "update nsConfig match keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:    predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11724",
			url:  "/nsnodeselector/add?namespace=namespace0&type=notmatch&key=newkey&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{
						{Key: "newkey", Value: "152"},
					},
				},
			},
			test: "add nsConfig notmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:    predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{"newkey": map[string]struct{}{"*": struct{}{}}},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11725",
			url:  "/nsnodeselector/delete?namespace=namespace0&type=notmatch&key=newkey&value=*",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{},
				},
			},
			test: "delete nsConfig notmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:    predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{"newkey": map[string]struct{}{"151": struct{}{}}},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11726",
			url:  "/nsnodeselector/update?namespace=namespace0&type=notmatch&key=newkey&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{
						{Key: "newkey", Value: "152"},
					},
				},
			},
			test: "update nsConfig notmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									MustMatch: predicates.LabelValues{},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11727",
			url:  "/nsnodeselector/add?namespace=namespace0&type=mustmatch&key=newkey&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					MustMatch: []KeyValue{
						{Key: "newkey", Value: "152"},
					},
					NotMatch: []KeyValue{
						{Key: "enndata.cn/systemnode", Value: "*"},
					},
				},
			},
			test: "add nsConfig Mustmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									MustMatch: predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch:  predicates.LabelValues{},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},

			addr: ":11728",
			url:  "/nsnodeselector/delete?namespace=namespace0&type=mustmatch&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					MustMatch: []KeyValue{},
					NotMatch:  []KeyValue{},
				},
			},
			test: "delete nsConfig Mustmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									MustMatch: predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch:  predicates.LabelValues{"newkey": map[string]struct{}{"151": struct{}{}}},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11729",
			url:  "/nsnodeselector/update?namespace=namespace0&type=mustmatch&key=kubernetes.io/hostname&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					MustMatch: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "152"},
					},
					NotMatch: []KeyValue{
						{Key: "newkey", Value: "151"},
					},
				},
			},
			test: "update nsConfig mustmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:        predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									MustNotMatch: predicates.LabelValues{},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11730",
			url:  "/nsnodeselector/add?namespace=namespace0&type=mustnotmatch&key=newkey&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{
						{Key: "enndata.cn/systemnode", Value: "*"},
					},
					MustNotMatch: []KeyValue{
						{Key: "newkey", Value: "152"},
					},
				},
			},
			test: "add nsConfig mustnotmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:        predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch:     predicates.LabelValues{},
									MustNotMatch: predicates.LabelValues{"newkey": map[string]struct{}{"*": struct{}{}}},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11731",
			url:  "/nsnodeselector/delete?namespace=namespace0&type=mustnotmatch&key=newkey&value=*",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch:     []KeyValue{},
					MustNotMatch: []KeyValue{},
				},
			},
			test: "delete nsConfig mustnotmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"namespace0": predicates.NsConfigItem{
									Match:        predicates.LabelValues{"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch:     predicates.LabelValues{},
									MustNotMatch: predicates.LabelValues{"newkey": map[string]struct{}{"151": struct{}{}}},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11732",
			url:  "/nsnodeselector/update?namespace=namespace0&type=mustnotmatch&key=newkey&value=152",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{},
					MustNotMatch: []KeyValue{
						{Key: "newkey", Value: "152"},
					},
				},
			},
			test: "update nsConfig mustnotmatch keyvalue",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11733",
			url:  "/nsnodeselector/add?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{
						{Key: "enndata.cn/systemnode", Value: "*"},
					},
				},
			},
			test: "add nsConfig match keyvalue while nsconfig is not defined",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11734",
			url:  "/nsnodeselector/delete?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "key [kubernetes.io/hostname] not exist",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{},
			test:           "delete nsConfig match keyvalue while nsconfig is not defined",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11735",
			url:  "/nsnodeselector/update?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "key [kubernetes.io/hostname] not exist",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{},
			test:           "update nsConfig match keyvalue while nsconfig is not defined",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing:  []*v1.ConfigMap{},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11736",
			url:  "/nsnodeselector/add?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{
				"namespace0": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{
						{Key: "enndata.cn/systemnode", Value: "*"},
					},
				},
			},
			test: "add nsConfig match keyvalue while configmap is not defined",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing:  []*v1.ConfigMap{},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11737",
			url:  "/nsnodeselector/delete?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "key [kubernetes.io/hostname] not exist",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{},
			test:           "delete nsConfig match keyvalue while configmap is not defined",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing:  []*v1.ConfigMap{},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11738",
			url:  "/nsnodeselector/update?namespace=namespace0&type=match&key=kubernetes.io/hostname&value=151",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "key [kubernetes.io/hostname] not exist",
				Data: "",
			},
			expectNsConfig: map[string]NsNodeSelectorConfigRet{},
			test:           "update nsConfig match keyvalue while configmap is not defined",
		},
	}

	for _, test := range tests {

		predicates.SetNsNodeSelectorConfigMapNil()

		c := &SchedulerConfig{
			client: test.FakeConfigMapNodePodHandler,
		}

		runserver("", c, test.addr, "", ",")

		ret, err := http.Get("http://localhost" + test.addr + test.url)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		defer ret.Body.Close()

		body, err := ioutil.ReadAll(ret.Body)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}

		setMsg := ReturnMsg{}

		err = json.Unmarshal(body, &setMsg)

		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !reflect.DeepEqual(setMsg, test.expectMsg) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, setMsg, test.expectMsg)
		}

		ret2, err := http.Get("http://localhost" + test.addr + "/nsnodeselector/")
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		defer ret2.Body.Close()

		body, err = ioutil.ReadAll(ret2.Body)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		msg := make(map[string]NsNodeSelectorConfigRet)

		err = json.Unmarshal(body, &msg)

		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}

		if !reflect.DeepEqual(msg, test.expectNsConfig) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, msg, test.expectNsConfig)
		}
	}

}

func TestGetNsNodeSelector(t *testing.T) {
	tests := []struct {
		FakeConfigMapNodePodHandler clientset.Interface
		addr                        string
		expect                      map[string]NsNodeSelectorConfigRet
		test                        string
	}{
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"kube-system1": predicates.NsConfigItem{
									Match: predicates.LabelValues{
										"kubernetes.io/hostname": map[string]struct{}{"151": struct{}{}}},
									NotMatch: predicates.LabelValues{},
								},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11712",
			expect: map[string]NsNodeSelectorConfigRet{
				"kube-system1": NsNodeSelectorConfigRet{
					Match: []KeyValue{
						{Key: "kubernetes.io/hostname", Value: "151"},
					},
					NotMatch: []KeyValue{},
				},
			},
			test: "not nil configmap",
		},
		{
			FakeConfigMapNodePodHandler: &FakeConfigMapNodePodHandler{
				Existing: []*v1.ConfigMap{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "nsnodeselector",
							Namespace: "kube-system",
						},
						Data: map[string]string{
							"nsnodeselector.json": jsonConfig(map[string]predicates.NsConfigItem{
								"kube-system": predicates.NsConfigItem{},
							})},
					},
				},
				Clientset: clientsetfake.NewSimpleClientset(&v1.ConfigMapList{}),
			},
			addr: ":11711",
			expect: map[string]NsNodeSelectorConfigRet{
				"kube-system": NsNodeSelectorConfigRet{
					NotMatch: []KeyValue{
						{Key: "enndata.cn/systemnode", Value: "*"},
					},
				},
			},
			test: "nil configmap",
		},
	}

	for _, test := range tests {
		predicates.SetNsNodeSelectorConfigMapNil()

		c := &SchedulerConfig{
			client: test.FakeConfigMapNodePodHandler,
		}
		runserver("", c, test.addr, "", ",")

		ret, err := http.Get("http://localhost" + test.addr + "/nsnodeselector/")
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		defer ret.Body.Close()

		body, err := ioutil.ReadAll(ret.Body)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}

		retConfig := make(map[string]NsNodeSelectorConfigRet)

		err = json.Unmarshal(body, &retConfig)

		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !reflect.DeepEqual(retConfig, test.expect) {
			t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, retConfig, test.expect)
		}

	}

}

func fakePredicate(pod *v1.Pod, meta algorithm.PredicateMetadata, nodeInfo *schedulercache.NodeInfo) (bool, []algorithm.PredicateFailureReason, error) {
	return true, nil, nil
}

func fakePriorityFunc(pod *v1.Pod, nodeNameToInfo map[string]*schedulercache.NodeInfo, nodes []*v1.Node) (schedulerapi.HostPriorityList, error) {

	var ret schedulerapi.HostPriorityList
	ret = []schedulerapi.HostPriority{
		{
			Host:  "prio1",
			Score: 1,
		},
		{
			Host:  "prio2",
			Score: 1,
		},
	}

	return ret, nil

}

func TestSetPolicy(t *testing.T) {
	//create factory
	handler := utiltesting.FakeHandler{
		StatusCode:   500,
		ResponseBody: "",
		T:            t,
	}
	server := httptest.NewServer(&handler)
	defer server.Close()
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: server.URL, ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}}})
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	factory := factory.NewConfigFactory(
		v1.DefaultSchedulerName,
		client,
		informerFactory.Core().V1().Nodes(),
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().PersistentVolumes(),
		informerFactory.Core().V1().PersistentVolumeClaims(),
		informerFactory.Core().V1().ReplicationControllers(),
		informerFactory.Extensions().V1beta1().ReplicaSets(),
		informerFactory.Apps().V1beta1().StatefulSets(),
		informerFactory.Core().V1().Services(),
		informerFactory.Policy().V1beta1().PodDisruptionBudgets(),
		informerFactory.Storage().V1().StorageClasses(),
		v1.DefaultHardPodAffinitySymmetricWeight,
		true,
		false,
	)

	var predicatePriorityGeter core.PredicatePriorityGeter
	predicatePriorityGeter = factory

	tests := []struct {
		basicAuthFile      string
		predicates         map[string]algorithm.FitPredicate
		prioritizers       []algorithm.PriorityConfig
		addr               string
		certFile           string
		keyFile            string
		getUrl             string
		expectMsg          ReturnMsg
		expectPredicates   []string
		expectPrioritizers []string
		test               string
	}{
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "CheckNodeMemoryPressure": fakePredicate, "GeneralPredicates": fakePredicate},
			prioritizers: []algorithm.PriorityConfig{
				{

					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{

					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},

			addr:     ":12353",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/set/predicate/HostName/on",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectPredicates:   []string{"CheckNodeDiskPressure", "CheckNodeMemoryPressure", "GeneralPredicates", "HostName"},
			expectPrioritizers: []string{"BalancedResourceAllocation", "EqualPriority"},
			test:               "turn on a predicate not in use",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "CheckNodeMemoryPressure": fakePredicate},
			prioritizers: []algorithm.PriorityConfig{
				{

					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{

					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},

			addr:     ":12361",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/set/predicate/FakeHostName/on",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "Invalid predicate name \"FakeHostName\" specified - no corresponding function found",
				Data: "",
			},
			expectPredicates:   []string{"CheckNodeDiskPressure", "CheckNodeMemoryPressure"},
			expectPrioritizers: []string{"BalancedResourceAllocation", "EqualPriority"},
			test:               "turn on a wrong name predicate",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "CheckNodeMemoryPressure": fakePredicate, "GeneralPredicates": fakePredicate},
			prioritizers:  []algorithm.PriorityConfig{},

			addr:     ":12354",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/set/predicate/CheckNodeDiskPressure/on",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectPredicates:   []string{"CheckNodeDiskPressure", "CheckNodeMemoryPressure", "GeneralPredicates"},
			expectPrioritizers: []string{},
			test:               "turn on a predicate already in use",
		},

		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "GeneralPredicates": fakePredicate},
			prioritizers:  []algorithm.PriorityConfig{},
			addr:          ":12365",
			certFile:      "",
			keyFile:       "",
			getUrl:        "/policy/set/predicate/CheckNodeDiskPressure/off",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectPredicates:   []string{"GeneralPredicates"},
			expectPrioritizers: []string{},
			test:               "turn off a predicate in use with GeneralPredicates",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate},
			prioritizers:  []algorithm.PriorityConfig{},
			addr:          ":12364",
			certFile:      "",
			keyFile:       "",
			getUrl:        "/policy/set/predicate/CheckNodeDiskPressure/off",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "predicate policy [GeneralPredicates] should not be deleted",
				Data: "",
			},
			expectPredicates:   []string{"CheckNodeDiskPressure"},
			expectPrioritizers: []string{},
			test:               "turn off a predicate in use without GeneralPredicates",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "CheckNodeMemoryPressure": fakePredicate, "GeneralPredicates": fakePredicate},
			prioritizers:  []algorithm.PriorityConfig{},
			addr:          ":12357",
			certFile:      "",
			keyFile:       "",
			getUrl:        "/policy/set/predicate/GeneralPredicates/off",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "predicate policy [GeneralPredicates] should not be deleted",
				Data: "",
			},
			expectPredicates:   []string{"CheckNodeDiskPressure", "CheckNodeMemoryPressure", "GeneralPredicates"},
			expectPrioritizers: []string{},
			test:               "turn off a musthave predicate",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{},
			prioritizers: []algorithm.PriorityConfig{
				{

					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{

					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},

			addr:     ":12355",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/set/priority/InterPodAffinityPriority/on",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectPredicates:   []string{},
			expectPrioritizers: []string{"BalancedResourceAllocation", "EqualPriority", "InterPodAffinityPriority"},
			test:               "turn on a priority not in use",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{},
			prioritizers: []algorithm.PriorityConfig{
				{

					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{

					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},

			addr:     ":12362",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/set/priority/BalancedResourceAllocation/on",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectPredicates:   []string{},
			expectPrioritizers: []string{"BalancedResourceAllocation", "EqualPriority"},
			test:               "turn on a priority in use",
		},

		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{},
			prioritizers: []algorithm.PriorityConfig{
				{

					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{

					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},

			addr:     ":12358",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/set/priority/BalancedResourceAllocation/off",
			expectMsg: ReturnMsg{
				Code: 0,
				Msg:  "OK",
				Data: "",
			},
			expectPredicates:   []string{},
			expectPrioritizers: []string{"EqualPriority"},
			test:               "turn off a priority in use",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{},
			prioritizers: []algorithm.PriorityConfig{
				{

					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{

					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},

			addr:     ":12359",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/set/priority/NodePVDiskUsePriority/off",
			expectMsg: ReturnMsg{
				Code: 1,
				Msg:  "NodePVDiskUsePriority is not exist",
				Data: "",
			},
			expectPredicates:   []string{},
			expectPrioritizers: []string{"BalancedResourceAllocation", "EqualPriority"},
			test:               "turn off a priority not in use",
		},
	}
	for _, test := range tests {
		queue := core.NewSchedulingQueue()
		c := &SchedulerConfig{
			configer: core.NewGenericScheduler(nil, nil, queue, test.predicates, algorithm.EmptyPredicateMetadataProducer, test.prioritizers, algorithm.EmptyPriorityMetadataProducer, nil, nil, nil, false, false, predicatePriorityGeter),
			client:   client,
		}
		runserver(test.basicAuthFile, c, test.addr, test.certFile, test.keyFile)

		ret, err := http.Get("http://localhost" + test.addr + test.getUrl)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		defer ret.Body.Close()

		body, err := ioutil.ReadAll(ret.Body)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}

		retMsg := ReturnMsg{}

		err = json.Unmarshal(body, &retMsg)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", test.test, err)
		}
		if !reflect.DeepEqual(retMsg, test.expectMsg) {
			t.Errorf("%s: unexpected failure reasons in Msg: %v, want: %v", test.test, retMsg, test.expectMsg)
		}

		//get new predicate
		retPredicate := []string{}
		modifiedPredicate, _ := c.getOnPredicates()
		for p, _ := range modifiedPredicate {
			retPredicate = append(retPredicate, p)
		}
		sort.Strings(retPredicate)
		if !reflect.DeepEqual(test.expectPredicates, retPredicate) {
			t.Errorf("%s:unexpected failure reasons in Predicate: %v,want :%v", test.test, retPredicate, test.expectPredicates)
		}

		//get new priority
		retPriority := []string{}
		modifiedPriority, _ := c.getOnPrioritys()
		for p, _ := range modifiedPriority {
			retPriority = append(retPriority, p)
		}

		sort.Strings(retPriority)
		if !reflect.DeepEqual(test.expectPrioritizers, retPriority) {
			t.Errorf("%s:unexpected failure reasons in Priority: %v,want :%v", test.test, retPriority, test.expectPrioritizers)
		}

	}

}

// TODO: fixed the test issue later
/*func TestGetPolicyHttpServer(t *testing.T) {
	//create factory
	var configData []byte
	var policy schedulerapi.Policy

	handler := utiltesting.FakeHandler{
		StatusCode:   500,
		ResponseBody: "",
		T:            t,
	}
	server := httptest.NewServer(&handler)
	defer server.Close()
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: server.URL, ContentConfig: restclient.ContentConfig{GroupVersion: &legacyscheme.Registry.GroupOrDie(v1.GroupName).GroupVersion}})
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	factory := factory.NewConfigFactory(
		v1.DefaultSchedulerName,
		client,
		informerFactory.Core().V1().Nodes(),
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().PersistentVolumes(),
		informerFactory.Core().V1().PersistentVolumeClaims(),
		informerFactory.Core().V1().ReplicationControllers(),
		informerFactory.Extensions().V1beta1().ReplicaSets(),
		informerFactory.Apps().V1beta1().StatefulSets(),
		informerFactory.Core().V1().Services(),
		informerFactory.Policy().V1beta1().PodDisruptionBudgets(),
		informerFactory.Storage().V1().StorageClasses(),
		v1.DefaultHardPodAffinitySymmetricWeight,
		true,
	)

	configData = []byte(`{}`)
	if err := runtime.DecodeInto(latestschedulerapi.Codec, configData, &policy); err != nil {
		t.Errorf("Invalid configuration: %v", err)
	}

	var predicatePriorityGeter core.PredicatePriorityGeter
	predicatePriorityGeter = factory

	tests := []struct {
		basicAuthFile string
		predicates    map[string]algorithm.FitPredicate
		prioritizers  []algorithm.PriorityConfig
		addr          string
		certFile      string
		keyFile       string
		getUrl        string

		expectItem ReturnItem
		test       string
	}{
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{},
			prioritizers:  []algorithm.PriorityConfig{},

			addr:     ":12345",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy",
			expectItem: ReturnItem{
				Prioritys: []Item{{Name: "BalancedResourceAllocation", IsOn: false}, {Name: "EqualPriority", IsOn: false}, {Name: "ImageLocalityPriority", IsOn: false},
					{Name: "InterPodAffinityPriority", IsOn: false}, {Name: "LeastRequestedPriority", IsOn: false}, {Name: "MostRequestedPriority", IsOn: false}, {Name: "NodeAffinityPriority", IsOn: false},
					{Name: "NodeNameSpacePriority", IsOn: false}, {Name: "NodePVDiskUsePriority", IsOn: false}, {Name: "NodePreferAvoidPodsPriority", IsOn: false}, {Name: "PVNodeSpreadPriority", IsOn: false},
					{Name: "SelectorSpreadPriority", IsOn: false}, {Name: "ServiceSpreadingPriority", IsOn: false}, {Name: "TaintTolerationPriority", IsOn: false}},
				Pedicates: []Item{{Name: "CheckNodeDiskPressure", IsOn: false}, {Name: "CheckVolumeBinding", IsOn: false}, {Name: "CheckNodeMemoryPressure", IsOn: false}, {Name: "GeneralPredicates", IsOn: false},
					{Name: "HostName", IsOn: false}, {Name: "HostPathPVAffinityPredicate", IsOn: false}, {Name: "HostPathPVAffinityPredicate2", IsOn: false},
					{Name: "MatchInterPodAffinity", IsOn: false}, {Name: "MatchNodeSelector", IsOn: false}, {Name: "MaxAzureDiskVolumeCount", IsOn: false},
					{Name: "MaxEBSVolumeCount", IsOn: false}, {Name: "MaxGCEPDVolumeCount", IsOn: false}, {Name: "NamespacesNodeSelector", IsOn: false}, {Name: "NoDiskConflict", IsOn: false},
					{Name: "NoVolumeZoneConflict", IsOn: false}, {Name: "NodeNameSpacePredicate", IsOn: false}, {Name: "PodFitsHostPorts", IsOn: false}, {Name: "PodFitsPorts", IsOn: false},
					{Name: "PodFitsResources", IsOn: false}, {Name: "PodHostPathPVDiskPressure", IsOn: false}, {Name: "PodToleratesNodeTaints", IsOn: false}, {Name: "CheckNodeCondition", IsOn: false}},
			},
			test: "no predicates and priorities can be used,all policy state is false",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "CheckNodeMemoryPressure": fakePredicate},
			prioritizers: []algorithm.PriorityConfig{
				{

					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{

					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},

			addr:     ":12346",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy",
			expectItem: ReturnItem{
				Prioritys: []Item{{Name: "BalancedResourceAllocation", IsOn: true}, {Name: "EqualPriority", IsOn: true}, {Name: "ImageLocalityPriority", IsOn: false},
					{Name: "InterPodAffinityPriority", IsOn: false}, {Name: "LeastRequestedPriority", IsOn: false}, {Name: "MostRequestedPriority", IsOn: false}, {Name: "NodeAffinityPriority", IsOn: false},
					{Name: "NodeNameSpacePriority", IsOn: false}, {Name: "NodePVDiskUsePriority", IsOn: false}, {Name: "NodePreferAvoidPodsPriority", IsOn: false}, {Name: "PVNodeSpreadPriority", IsOn: false},
					{Name: "SelectorSpreadPriority", IsOn: false}, {Name: "ServiceSpreadingPriority", IsOn: false}, {Name: "TaintTolerationPriority", IsOn: false}},
				Pedicates: []Item{{Name: "CheckNodeDiskPressure", IsOn: true}, {Name: "CheckNodeMemoryPressure", IsOn: true}, {Name: "GeneralPredicates", IsOn: false},
					{Name: "HostName", IsOn: false}, {Name: "HostPathPVAffinityPredicate", IsOn: false}, {Name: "HostPathPVAffinityPredicate2", IsOn: false},
					{Name: "MatchInterPodAffinity", IsOn: false}, {Name: "MatchNodeSelector", IsOn: false}, {Name: "MaxAzureDiskVolumeCount", IsOn: false},
					{Name: "MaxEBSVolumeCount", IsOn: false}, {Name: "MaxGCEPDVolumeCount", IsOn: false}, {Name: "NamespacesNodeSelector", IsOn: false}, {Name: "NoDiskConflict", IsOn: false},
					{Name: "NoVolumeZoneConflict", IsOn: false}, {Name: "NodeNameSpacePredicate", IsOn: false}, {Name: "PodFitsHostPorts", IsOn: false}, {Name: "PodFitsPorts", IsOn: false},
					{Name: "PodFitsResources", IsOn: false}, {Name: "PodHostPathPVDiskPressure", IsOn: false}, {Name: "PodToleratesNodeTaints", IsOn: false}},
			},
			test: "two predicates and two priorities can be used in init,state is true",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "CheckNodeMemoryPressure": fakePredicate},
			prioritizers: []algorithm.PriorityConfig{
				{
					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{
					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},
			addr:     ":12347",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/get/predicate/",
			expectItem: ReturnItem{
				Prioritys: nil,
				Pedicates: []Item{{Name: "CheckNodeDiskPressure", IsOn: true}, {Name: "CheckNodeMemoryPressure", IsOn: true}, {Name: "GeneralPredicates", IsOn: false},
					{Name: "HostName", IsOn: false}, {Name: "HostPathPVAffinityPredicate", IsOn: false}, {Name: "HostPathPVAffinityPredicate2", IsOn: false},
					{Name: "MatchInterPodAffinity", IsOn: false}, {Name: "MatchNodeSelector", IsOn: false}, {Name: "MaxAzureDiskVolumeCount", IsOn: false},
					{Name: "MaxEBSVolumeCount", IsOn: false}, {Name: "MaxGCEPDVolumeCount", IsOn: false}, {Name: "NamespacesNodeSelector", IsOn: false}, {Name: "NoDiskConflict", IsOn: false},
					{Name: "NoVolumeZoneConflict", IsOn: false}, {Name: "NodeNameSpacePredicate", IsOn: false}, {Name: "PodFitsHostPorts", IsOn: false}, {Name: "PodFitsPorts", IsOn: false},
					{Name: "PodFitsResources", IsOn: false}, {Name: "PodHostPathPVDiskPressure", IsOn: false}, {Name: "PodToleratesNodeTaints", IsOn: false}},
			},
			test: "only get predicate",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{},
			prioritizers:  []algorithm.PriorityConfig{},
			addr:          ":12349",
			certFile:      "",
			keyFile:       "",
			getUrl:        "/policy/get/priority/BalancedResourceAllocation",
			expectItem: ReturnItem{
				Prioritys: []Item{{Name: "BalancedResourceAllocation", IsOn: false}},
				Pedicates: nil,
			},
			test: "get a spec priority",
		},
		{
			basicAuthFile: "",
			predicates:    map[string]algorithm.FitPredicate{"CheckNodeDiskPressure": fakePredicate, "CheckNodeMemoryPressure": fakePredicate},
			prioritizers: []algorithm.PriorityConfig{
				{
					Function: fakePriorityFunc,
					Name:     "BalancedResourceAllocation",
				},
				{
					Function: fakePriorityFunc,
					Name:     "EqualPriority",
				},
			},
			addr:     ":12350",
			certFile: "",
			keyFile:  "",
			getUrl:   "/policy/get/predicate/CheckNodeDiskPressure",
			expectItem: ReturnItem{
				Prioritys: nil,
				Pedicates: []Item{{Name: "CheckNodeDiskPressure", IsOn: true}},
			},
			test: "get a spec predicate",
		},
	}


		for _, test := range tests {
			queue := core.NewSchedulingQueue()
			c := &SchedulerConfig{
				configer: core.NewGenericScheduler(nil, nil, queue, test.predicates, algorithm.EmptyPredicateMetadataProducer, test.prioritizers, algorithm.EmptyMetadataProducer, nil, nil, predicatePriorityGeter),
				client:   client,
			}

			runserver(test.basicAuthFile, c, test.addr, test.certFile, test.keyFile)

			ret, err := http.Get("http://localhost" + test.addr + test.getUrl)
			if err != nil {
				t.Errorf("%s: unexpected error: %v", test.test, err)
			}
			defer ret.Body.Close()

			body, err := ioutil.ReadAll(ret.Body)
			if err != nil {
				t.Errorf("%s: unexpected error: %v", test.test, err)
			}

			retItem := ReturnItem{
				Prioritys: []Item{},
				Pedicates: []Item{},
			}

			err = json.Unmarshal(body, &retItem)
			fmt.Println("json")
			if err != nil {
				t.Errorf("%s: unexpected error: %v", test.test, err)
			}
			if !reflect.DeepEqual(retItem, test.expectItem) {
				t.Errorf("%s: unexpected failure reasons: %v, want: %v", test.test, retItem, test.expectItem)
			}
		}

}*/

func runserver(basicAuthFile string, c *SchedulerConfig, addr, certFile, keyFile string) {
	StartPolicyHttpServer(basicAuthFile, c, addr, certFile, keyFile)
	time.Sleep(100 * time.Millisecond)
}
