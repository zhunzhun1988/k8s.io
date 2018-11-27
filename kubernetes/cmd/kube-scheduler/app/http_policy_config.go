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

package app

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/plugin/pkg/authenticator/password/passwordfile"
	"k8s.io/apiserver/plugin/pkg/authenticator/request/basicauth"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	"k8s.io/kubernetes/pkg/scheduler/factory"
)

const (
	schedulerpolicy_configmap_ns   = "kube-system"
	schedulerpolicy_configmap_name = "schedulerpolicy"
	schedulerpolicy_itemname       = "schedulerpolicy.json"
	schedulerpolicy_describename   = "readme"
	schedulerpolicy_describe       = "it's created by scheduler to save policys, please not delete it"
)

type SchedulerConfig struct {
	configer                          algorithm.SchdulerConfig
	client                            clientset.Interface
	policyConfigMap                   *v1.ConfigMap
	nsNodeSelectorConfigMap           *v1.ConfigMap
	configMapTimeOut                  time.Duration
	policyConfigMapUpdateTime         time.Time
	nsNodeSelectorConfigMapUpdateTime time.Time
}

type Item struct {
	Name string `json:"name"`
	IsOn bool   `json:"isOn"`
}

type ItemList []Item

func (il ItemList) Len() int { return len(il) }
func (il ItemList) Less(i, j int) bool {
	return il[i].Name < il[j].Name
}
func (il ItemList) Swap(i, j int) {
	il[i], il[j] = il[j], il[i]
}

type PolicySaveItem struct {
	Prioritys  []string `json:"prioritys"`
	Predicates []string `json:"pedicates"`
}

type ReturnItem struct {
	Prioritys ItemList `json:"prioritys"`
	Pedicates ItemList `json:"pedicates"`
}

type ReturnMsg struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data string `json:"data"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type NsNodeSelectorConfigRet struct {
	Match        []KeyValue `json:"match"`
	MustMatch    []KeyValue `json:"mustMatch"`
	NotMatch     []KeyValue `json:"notMatch"`
	MustNotMatch []KeyValue `json:"mustNotMatch"`
}

func DeletePod(client clientset.Interface, namespace, name string) error {
	var GracePeriodSeconds int64 = 0
	return client.CoreV1().Pods(namespace).Delete(name, &meta_v1.DeleteOptions{GracePeriodSeconds: &GracePeriodSeconds})
}

func GetPods(client clientset.Interface, namespace string) ([]*v1.Pod, error) {
	pods, err := client.CoreV1().Pods(namespace).List(meta_v1.ListOptions{})
	if err != nil {
		return []*v1.Pod{}, err
	}
	ret := make([]*v1.Pod, 0, len(pods.Items))
	for i, _ := range pods.Items {
		ret = append(ret, &pods.Items[i])
	}
	return ret, nil
}

func GetSchedulerPolicyConfigMap(client clientset.Interface) *v1.ConfigMap {
	cm, err := client.CoreV1().ConfigMaps(schedulerpolicy_configmap_ns).Get(schedulerpolicy_configmap_name, meta_v1.GetOptions{})
	if err != nil {
		glog.Errorf("GetSchedulerPolicyConfigMap %s:%s , err:%v", schedulerpolicy_configmap_ns, schedulerpolicy_configmap_name, err)
		return nil
	}
	return cm
}

func GetNamespaces(client clientset.Interface) ([]*v1.Namespace, error) {
	nsList, err := client.CoreV1().Namespaces().List(meta_v1.ListOptions{})
	if err != nil {
		return []*v1.Namespace{}, err
	}
	ret := make([]*v1.Namespace, 0, len(nsList.Items))
	for i := range nsList.Items {
		ret = append(ret, &nsList.Items[i])
	}
	return ret, nil
}

func GetNodes(client clientset.Interface) ([]*v1.Node, error) {
	nodeList, err := client.CoreV1().Nodes().List(meta_v1.ListOptions{})
	if err != nil {
		return []*v1.Node{}, err
	}
	ret := make([]*v1.Node, 0, len(nodeList.Items))
	for i := range nodeList.Items {
		ret = append(ret, &nodeList.Items[i])
	}
	return ret, nil
}

func GetSavePolicys(sc *SchedulerConfig) *PolicySaveItem {
	cm := sc.getPolicyConfigMap(false)
	if cm == nil {
		return nil
	}
	if cm.Data[schedulerpolicy_itemname] == "" {
		return nil
	}
	buf := cm.Data[schedulerpolicy_itemname]
	ret := &PolicySaveItem{}
	if err := json.Unmarshal([]byte(buf), ret); err != nil {
		return nil
	}
	return ret
}

func CreateOrUpdateSchedulerPolicyConfigMap(client clientset.Interface, updateCm *v1.ConfigMap) (*v1.ConfigMap, error) {
	curCM, err := client.CoreV1().ConfigMaps(updateCm.Namespace).Get(updateCm.Name, meta_v1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return client.CoreV1().ConfigMaps(updateCm.Namespace).Create(updateCm)
		}
		glog.Errorf("CreateOrUpdateSchedulerPolicyConfigMap %s:%s , err:%v", schedulerpolicy_configmap_ns, schedulerpolicy_configmap_name, err)
		return nil, err
	} else {
		curCM.Data = updateCm.Data
		return client.CoreV1().ConfigMaps(curCM.Namespace).Update(curCM)
	}
}

func filterEmpty(strs []string) []string {
	ret := make([]string, 0, len(strs))
	for _, str := range strs {
		if str != "" {
			ret = append(ret, str)
		}
	}
	return ret
}
func slicToMap(slic []string) map[string]bool {
	ret := make(map[string]bool)
	if slic == nil {
		return ret
	}
	for _, str := range slic {
		ret[str] = true
	}
	return ret
}
func mapToSlic(m map[string]bool) []string {
	if m == nil {
		return []string{}
	}
	ret := make([]string, 0, len(m))
	for key := range m {
		ret = append(ret, key)
	}
	return ret
}

func (sc *SchedulerConfig) getPolicyConfigMap(notcache bool) *v1.ConfigMap {
	if sc.policyConfigMap == nil || notcache == true || time.Since(sc.policyConfigMapUpdateTime) > sc.configMapTimeOut {
		cm := GetSchedulerPolicyConfigMap(sc.client)
		sc.policyConfigMap = cm
		sc.policyConfigMapUpdateTime = time.Now()
	}
	return sc.policyConfigMap
}

func (sc *SchedulerConfig) getNsNodeSelectorConfigMap(notcache bool) *v1.ConfigMap {
	if sc.nsNodeSelectorConfigMap == nil || notcache == true || time.Since(sc.nsNodeSelectorConfigMapUpdateTime) > sc.configMapTimeOut {
		cm := predicates.GetNsNodeSelectorConfigMap(sc.client)
		sc.nsNodeSelectorConfigMap = cm
		sc.nsNodeSelectorConfigMapUpdateTime = time.Now()
	}
	return sc.nsNodeSelectorConfigMap
}

func (sc *SchedulerConfig) getOnPredicates() (map[string]bool, error) {
	ret := make(map[string]bool)
	predicates := sc.configer.GetPredicates()
	for name, _ := range predicates {
		ret[name] = true
	}
	return ret, nil
}
func (sc *SchedulerConfig) getOnPrioritys() (map[string]bool, error) {
	ret := make(map[string]bool)
	prioritys := sc.configer.GetPrioritys()
	for _, priority := range prioritys {
		ret[priority.Name] = true
	}
	return ret, nil
}

func (sc *SchedulerConfig) PolicyGet(request *restful.Request, response *restful.Response) {
	policyType := request.PathParameter("type")
	policyName := request.PathParameter("name")

	registeredFitPredicates := factory.ListRegisteredFitPredicates()
	registeredFitPrioritys := factory.ListRegisteredPriorityFunctions()

	retItem := ReturnItem{
		Prioritys: make([]Item, 0, len(registeredFitPrioritys)),
		Pedicates: make([]Item, 0, len(registeredFitPredicates)),
	}
	onFitPredicates, errPreGet := sc.getOnPredicates()
	onFitPrioritys, errPriGet := sc.getOnPrioritys()

	var err1, err2 error
	if policyType != "" && policyType != "predicate" {
		retItem.Pedicates = nil
	} else if errPreGet != nil {
		err1 = errPreGet
	} else {
		for _, name := range registeredFitPredicates {
			var on bool
			if _, exist := onFitPredicates[name]; exist {
				on = true
			}
			if (policyName != "" && policyName == name) || policyName == "" {
				retItem.Pedicates = append(retItem.Pedicates, Item{Name: name, IsOn: on})
			}
		}
	}
	if policyType != "" && policyType != "priority" {
		retItem.Prioritys = nil
	} else if errPriGet != nil {
		err2 = errPriGet
	} else {
		for _, name := range registeredFitPrioritys {
			var on bool
			if _, exist := onFitPrioritys[name]; exist {
				on = true
			}
			if (policyName != "" && policyName == name) || policyName == "" {
				retItem.Prioritys = append(retItem.Prioritys, Item{Name: name, IsOn: on})
			}
		}
	}
	var errMsg string
	if err1 != nil || err2 != nil {
		if err1 != nil {
			errMsg += err1.Error()
		}
		if err2 != nil {
			errMsg += err2.Error()
		}
		response.Write([]byte(errMsg))
	} else {
		sort.Sort(retItem.Pedicates)
		sort.Sort(retItem.Prioritys)
		response.WriteAsJson(retItem)
	}
}
func (sc *SchedulerConfig) isValidPolicy(predicates, priority []string) error {
	musthavePredicate := []string{"GeneralPredicates"}
	if predicates != nil {
		for _, pre := range musthavePredicate {
			find := false
			for _, curPre := range predicates {
				if curPre == pre {
					find = true
					break
				}
			}
			if find == false {
				return fmt.Errorf("predicate policy [%s] should not be deleted", pre)
			}
		}
	}
	return nil
}

func (sc *SchedulerConfig) savePolicy() {
	pre, err1 := sc.getOnPredicates()
	pri, err2 := sc.getOnPrioritys()

	saveItem := PolicySaveItem{
		Predicates: mapToSlic(pre),
		Prioritys:  mapToSlic(pri),
	}
	if err := sc.isValidPolicy(saveItem.Predicates, saveItem.Prioritys); err != nil {
		glog.Errorf("%v is not valid policy:%v", saveItem, err)
		return
	}
	buf, err3 := json.MarshalIndent(&saveItem, " ", "  ")
	if err1 != nil || err2 != nil || err3 != nil {
		glog.Errorf("save err %v,%v,%v", err1, err2, err3)
		return
	}
	cm := &v1.ConfigMap{
		ObjectMeta: meta_v1.ObjectMeta{
			Namespace: schedulerpolicy_configmap_ns,
			Name:      schedulerpolicy_configmap_name,
		},
		Data: map[string]string{
			schedulerpolicy_itemname:     string(buf),
			schedulerpolicy_describename: schedulerpolicy_describe,
		},
	}
	if updateCm, errUpdate := CreateOrUpdateSchedulerPolicyConfigMap(sc.client, cm); errUpdate != nil {
		glog.Errorf("CreateOrUpdateSchedulerPolicyConfigMap err:%v", errUpdate)
	} else {
		sc.policyConfigMap = updateCm
	}
}
func (sc *SchedulerConfig) PolicySet(request *restful.Request, response *restful.Response) {
	policyType := request.PathParameter("type")
	policyName := request.PathParameter("name")
	isOn := request.PathParameter("ison")

	ret := ReturnMsg{}
	var err error = nil
	var ok bool = true
	switch policyType {
	case "predicate":
		if isOn != "off" {
			ok, err = sc.configer.AddPredicate(policyName)
		} else {
			pres, _ := sc.getOnPredicates()
			delete(pres, policyName)
			if errValid := sc.isValidPolicy(mapToSlic(pres), nil); errValid != nil {
				ok = false
				err = errValid
			} else {
				ok, err = sc.configer.RemovePredicate(policyName)
			}
		}

	case "priority":
		if isOn != "off" {
			ok, err = sc.configer.AddPriority(policyName)
		} else {
			pris, _ := sc.getOnPrioritys()
			delete(pris, policyName)
			if errValid := sc.isValidPolicy(nil, mapToSlic(pris)); errValid != nil {
				ok = false
				err = errValid
			} else {
				ok, err = sc.configer.RemovePriority(policyName)
			}
		}
	default:
		ok = false
		err = fmt.Errorf("set policy type error[predicate/priority]")
	}

	if ok == false || err != nil {
		ret.Code = 1
		ret.Msg = err.Error()
	} else {
		ret.Code = 0
		ret.Msg = "OK"
		sc.savePolicy()
	}
	response.WriteAsJson(ret)
}

func (sc *SchedulerConfig) NsNodeSelectorGet(request *restful.Request, response *restful.Response) {
	cm := sc.getNsNodeSelectorConfigMap(false)
	if cm == nil {
		response.Write([]byte("undefine NodeSelector configmap"))
		return
	}
	namespaces, _ := GetNamespaces(sc.client)
	nsconfig := predicates.GetNsConfigByConfigMap(cm, namespaces)
	tmp := make(map[string]NsNodeSelectorConfigRet)
	for ns, config := range nsconfig {
		nsNodeSelectorConfigRet := NsNodeSelectorConfigRet{}
		if config.Match != nil {
			nsNodeSelectorConfigRet.Match = make([]KeyValue, 0, len(config.Match))
			for key, valueMap := range config.Match {
				if key != "" {
					nsNodeSelectorConfigRet.Match = append(nsNodeSelectorConfigRet.Match, KeyValue{
						Key:   key,
						Value: strings.Join(predicates.ListMapString(valueMap), ","),
					})
				}
			}
		}
		if config.MustMatch != nil {
			nsNodeSelectorConfigRet.MustMatch = make([]KeyValue, 0, len(config.MustMatch))
			for key, valueMap := range config.MustMatch {
				if key != "" {
					nsNodeSelectorConfigRet.MustMatch = append(nsNodeSelectorConfigRet.MustMatch, KeyValue{
						Key:   key,
						Value: strings.Join(predicates.ListMapString(valueMap), ","),
					})
				}
			}
		}
		if config.NotMatch != nil {
			nsNodeSelectorConfigRet.NotMatch = make([]KeyValue, 0, len(config.NotMatch))
			for key, valueMap := range config.NotMatch {
				if key != "" {
					nsNodeSelectorConfigRet.NotMatch = append(nsNodeSelectorConfigRet.NotMatch, KeyValue{
						Key:   key,
						Value: strings.Join(predicates.ListMapString(valueMap), ","),
					})
				}
			}
		}
		if config.MustNotMatch != nil {
			nsNodeSelectorConfigRet.MustNotMatch = make([]KeyValue, 0, len(config.MustNotMatch))
			for key, valueMap := range config.MustNotMatch {
				if key != "" {
					nsNodeSelectorConfigRet.MustNotMatch = append(nsNodeSelectorConfigRet.MustNotMatch, KeyValue{
						Key:   key,
						Value: strings.Join(predicates.ListMapString(valueMap), ","),
					})
				}
			}
		}
		tmp[ns] = nsNodeSelectorConfigRet
	}
	response.WriteAsJson(tmp)
}

func (sc *SchedulerConfig) CheckNamespaceSchedulerNodes(request *restful.Request, response *restful.Response) {
	nodes, errNode := GetNodes(sc.client)
	if errNode != nil {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  fmt.Sprintf("get nodes error:%v", errNode.Error()),
			Data: ""})
		return
	}
	namespace := request.PathParameter("namespace")
	nsConfig := predicates.GetNsConfigByConfigMap(sc.getNsNodeSelectorConfigMap(false), nil)
	selector, err := predicates.GetNsLabelSelector(nsConfig, namespace)
	if err != nil {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  err.Error(),
			Data: ""})
		return
	}
	okNodes := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if selector.Matches(labels.Set(node.Labels)) {
			okNodes = append(okNodes, node.Name)
		}
	}
	response.WriteAsJson(ReturnMsg{Code: 0,
		Msg:  "OK",
		Data: fmt.Sprintf("[%s]", strings.Join(okNodes, ","))})
}

func (sc *SchedulerConfig) NsNodeSelectorNodeLabels(request *restful.Request, response *restful.Response) {
	nodes, errNode := GetNodes(sc.client)
	if errNode != nil {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  fmt.Sprintf("get nodes error:%v", errNode.Error()),
			Data: ""})
		return
	}
	m := make(predicates.LabelValues)
	m[predicates.Nsnodeselector_systemlabel] = map[string]struct{}{"*": struct{}{}}
	for _, node := range nodes {
		for k, v := range node.Labels {
			//set.Insert(predicates.MakeSelectorByKeyValue(k, v))
			if vm, find := m[k]; find == true {
				vm[v] = struct{}{}
			} else {
				m[k] = map[string]struct{}{v: struct{}{}}
			}
		}
	}

	lbs := make([]KeyValue, 0, len(m))
	for k, vm := range m {
		if k != "" {
			lbs = append(lbs, KeyValue{Key: k, Value: strings.Join(predicates.ListMapString(vm), ",")})
		}
	}
	buf, _ := json.Marshal(lbs)
	response.WriteAsJson(ReturnMsg{Code: 1,
		Msg:  "OK",
		Data: string(buf)})
}

func (sc *SchedulerConfig) NsNodeSelectorRefresh(request *restful.Request, response *restful.Response) {
	nodes, errNode := GetNodes(sc.client)
	if errNode != nil {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  fmt.Sprintf("get nodes error:%v", errNode.Error()),
			Data: ""})
		return
	}
	namespace := request.PathParameter("namespace")
	nsConfig := predicates.GetNsConfigByConfigMap(sc.getNsNodeSelectorConfigMap(true), nil)
	selector, err := predicates.GetNsLabelSelector(nsConfig, namespace)
	if err != nil {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  fmt.Sprintf("GetNsLabelSelector err:%v", err.Error()),
			Data: ""})
		return
	}
	okNodes := make(map[string]struct{})
	for _, node := range nodes {
		if selector.Matches(labels.Set(node.Labels)) {
			okNodes[node.Name] = struct{}{}
		}
	}

	pods, errPods := GetPods(sc.client, namespace)
	if errPods != nil {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  fmt.Sprintf("get pods err:%v", errPods.Error()),
			Data: ""})
		return
	}
	errMsg := make([]string, 0, len(pods))
	okMsg := make([]string, 0, len(pods))
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			if _, ok := okNodes[pod.Spec.NodeName]; ok == false {
				errDelete := DeletePod(sc.client, pod.Namespace, pod.Name)
				if errDelete != nil {
					errMsg = append(errMsg, fmt.Sprintf("delete pod[%s:%s] err:%v", pod.Namespace, pod.Name, errDelete))
				} else {
					okMsg = append(okMsg, fmt.Sprintf("[%s:%s]:%s", pod.Namespace, pod.Name, pod.Spec.NodeName))
				}
			}
		}
	}
	if len(errMsg) == 0 {
		response.WriteAsJson(ReturnMsg{Code: 0,
			Msg:  "OK",
			Data: strings.Join(okMsg, ",")})
	} else {
		response.WriteAsJson(ReturnMsg{Code: 0,
			Msg:  strings.Join(errMsg, "|"),
			Data: strings.Join(okMsg, ",")})
	}
}

func (sc *SchedulerConfig) NsNodeSelectorAddUpdateOrDelete(request *restful.Request, response *restful.Response) {
	request.Request.ParseForm()
	addUpdateOrDelete := request.PathParameter("addupdateordelete")
	if addUpdateOrDelete != "delete" && addUpdateOrDelete != "add" && addUpdateOrDelete != "update" {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  fmt.Sprintf("unknow operation %s", addUpdateOrDelete),
			Data: ""})
		return
	}
	namespace := request.Request.FormValue("namespace")
	matchType := request.Request.FormValue("type")
	matchKey := request.Request.FormValue("key")
	matchValue := request.Request.FormValue("value")

	cm := sc.getNsNodeSelectorConfigMap(true)

	namespaces, _ := GetNamespaces(sc.client)
	nsConfig := predicates.GetNsConfigByConfigMap(cm, namespaces)
	if nsConfig == nil {
		nsConfig = make(predicates.NsConfig)
	}
	nsConfigItem := nsConfig[namespace]

	notExist := ReturnMsg{Code: 1,
		Msg:  fmt.Sprintf("key [%s] not exist", matchKey),
		Data: ""}
	isExist := ReturnMsg{Code: 1,
		Msg:  fmt.Sprintf("key [%s] is existed", matchKey),
		Data: ""}

	tmpFun := func(lbvs predicates.LabelValues) bool {
		if addUpdateOrDelete == "add" {
			if _, exist := lbvs[matchKey]; exist == false {
				lbvs.InsertValue(matchKey, matchValue)
			} else {
				response.WriteAsJson(isExist)
				return false
			}
		} else if addUpdateOrDelete == "delete" {
			if _, exist := lbvs[matchKey]; exist == true {
				delete(lbvs, matchKey)
			} else {
				response.WriteAsJson(notExist)
				return false
			}
		} else {
			if _, exist := lbvs[matchKey]; exist == true {
				delete(lbvs, matchKey)
				lbvs.InsertValue(matchKey, matchValue)
			} else {
				response.WriteAsJson(notExist)
				return false
			}
		}
		return true
	}
	switch matchType {
	case "match":
		if nsConfigItem.Match == nil {
			nsConfigItem.Match = make(predicates.LabelValues)
		}
		if tmpFun(nsConfigItem.Match) == false {
			return
		}
	case "mustmatch":
		if nsConfigItem.MustMatch == nil {
			nsConfigItem.MustMatch = make(predicates.LabelValues)
		}
		if tmpFun(nsConfigItem.MustMatch) == false {
			return
		}
	case "notmatch":
		if nsConfigItem.NotMatch == nil {
			nsConfigItem.NotMatch = make(predicates.LabelValues)
		}
		if tmpFun(nsConfigItem.NotMatch) == false {
			return
		}
	case "mustnotmatch":
		if nsConfigItem.MustNotMatch == nil {
			nsConfigItem.MustNotMatch = make(predicates.LabelValues)
		}
		if tmpFun(nsConfigItem.MustNotMatch) == false {
			return
		}
	default:
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  fmt.Sprintf("unknow matchtype %s", matchType),
			Data: ""})
		return
	}
	nsConfig[namespace] = nsConfigItem
	predicates.SetNsNodeSelectorConfigMapNsConfig(cm, nsConfig)
	if updateCm, err := CreateOrUpdateSchedulerPolicyConfigMap(sc.client, cm); err != nil {
		response.WriteAsJson(ReturnMsg{Code: 1,
			Msg:  err.Error(),
			Data: ""})
	} else {
		response.WriteAsJson(ReturnMsg{Code: 0,
			Msg:  "OK",
			Data: ""})
		sc.nsNodeSelectorConfigMap = updateCm
	}
}

func (sc *SchedulerConfig) RestorePolicys() {
	saveItem := GetSavePolicys(sc)
	if saveItem != nil {
		savePre := slicToMap(saveItem.Predicates)
		savePri := slicToMap(saveItem.Prioritys)

		onPre, err1 := sc.getOnPredicates()
		onPri, err2 := sc.getOnPrioritys()

		for pre := range savePre {
			if _, ok := onPre[pre]; ok == false {
				sc.configer.AddPredicate(pre)
			}
		}
		for pre := range onPre {
			if _, ok := savePre[pre]; ok == false {
				sc.configer.RemovePredicate(pre)
			}
		}
		for pri := range savePri {
			if _, ok := onPri[pri]; ok == false {
				sc.configer.AddPriority(pri)
			}
		}
		for pri := range onPri {
			if _, ok := savePri[pri]; ok == false {
				sc.configer.RemovePriority(pri)
			}
		}

		if err1 != nil || err2 != nil {
			glog.Infof("getOnPredicates:%v getOnPrioritys:%v, not RestorePolicys", err1, err2)
		} else {

		}
	} else {
		glog.Infof("GetSavePolicys fail not RestorePolicys")
	}
}

func StartPolicyHttpServer(basicAuthFile string, c *SchedulerConfig, addr, certFile, keyFile string) {
	var wsContainer *restful.Container = restful.NewContainer()
	mux := http.NewServeMux()
	handler := http.Handler(mux)
	if basicAuthFile != "" {
		auth, err := newAuthenticatorFromBasicAuthFile(basicAuthFile)
		if err != nil {
			glog.Errorf("Unable to StartPolicyHttpServer: %v", err)
			return
		}
		handler = WithAuthentication(mux, auth)
	}
	wsContainer.Router(restful.CurlyRouter{})
	wsContainer.ServeMux = mux
	ws1 := new(restful.WebService)
	ws1.Path("/policy").Consumes("*/*").Produces(restful.MIME_JSON)
	ws1.Route(ws1.GET("/").To(c.PolicyGet).
		Doc("show all policy").
		Writes(ReturnItem{}))
	ws1.Route(ws1.GET("/get").To(c.PolicyGet).
		Doc("show all policy").
		Writes(ReturnItem{}))
	ws1.Route(ws1.GET("/get/{type}").To(c.PolicyGet).
		Doc("show the policy of type").
		Writes(ReturnItem{}))
	ws1.Route(ws1.GET("/get/{type}/{name}").To(c.PolicyGet).
		Doc("show the given policy of type").
		Writes(ReturnItem{}))
	ws1.Route(ws1.GET("/set/{type}/{name}/{ison}").To(c.PolicySet).
		Doc("enable/disable policy").
		Writes(ReturnMsg{}))

	ws2 := new(restful.WebService)
	ws2.Path("/nsnodeselector").Consumes("*/*").Produces(restful.MIME_JSON)
	ws2.Route(ws2.GET("/").To(c.NsNodeSelectorGet).
		Doc("show all nsnodeselector").
		Writes(map[string]NsNodeSelectorConfigRet{}))
	ws2.Route(ws2.GET("/check/{namespace}").To(c.CheckNamespaceSchedulerNodes).
		Doc("check which node namespace pod can schedule to").
		Writes(ReturnMsg{}))
	ws2.Route(ws2.GET("/{addupdateordelete}/").To(c.NsNodeSelectorAddUpdateOrDelete).
		Doc("add update, or delete namespace nodeselector match/notmach/mustmatch/mustnotmatch").
		Writes(ReturnMsg{}))
	ws2.Route(ws2.GET("/nodelabels/").To(c.NsNodeSelectorNodeLabels).
		Doc("get all node labels").
		Writes(ReturnMsg{}))
	ws2.Route(ws2.GET("/refresh/{namespace}").To(c.NsNodeSelectorRefresh).
		Doc("get all node labels").
		Writes(ReturnMsg{}))

	wsContainer.Add(ws1)
	wsContainer.Add(ws2)

	serverPolicy := &http.Server{
		Addr:    addr,
		Handler: handler,
	}
	go func() {
		if certFile != "" && keyFile != "" { // for https
			glog.Fatal(serverPolicy.ListenAndServeTLS(certFile, keyFile))
		} else { //for http
			glog.Fatal(serverPolicy.ListenAndServe())
		}
	}()
}

// newAuthenticatorFromBasicAuthFile returns an authenticator.Request or an error
func newAuthenticatorFromBasicAuthFile(basicAuthFile string) (authenticator.Request, error) {
	basicAuthenticator, err := passwordfile.NewCSV(basicAuthFile)
	if err != nil {
		return nil, err
	}

	return basicauth.New(basicAuthenticator), nil
}

func WithAuthentication(handler http.Handler, auth authenticator.Request) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, ok, err := auth.AuthenticateRequest(req)
		if ok == false || err != nil {
			if err != nil {
				glog.Errorf("Unable to authenticate the request due to an error: %v", err)
			}
			unauthorizedBasicAuth(w, req)
			return
		}
		handler.ServeHTTP(w, req)
	})
}

func unauthorizedBasicAuth(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("WWW-Authenticate", `Basic realm="kubernetes-master"`)
	http.Error(w, "Unauthorized", http.StatusUnauthorized)
}
