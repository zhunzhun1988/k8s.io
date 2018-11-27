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

package debug

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"

	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/rescheduler/utils"
)

const (
	INFOTYPE_NODEINFO                      string = "1"
	INFOTYPE_MESSAGE                       string = "2"
	INFOTYPE_RESCHEDULE_OK                 string = "3"
	INFOTYPE_RESCHEDULE_FAIL               string = "4"
	INFOTYPE_RESCHEDULE_STARTONERESCHEDULE string = "5"
	INFOTYPE_RESCHEDULE_STOPONERESCHEDULE  string = "6"
)

type PodInfos struct {
	Name      string
	Namespace string
}

type NodeInfos struct {
	NodeName string
	PodInfos []PodInfos
}

type Infos map[string]*NodeInfos

type RescheduleDebug struct {
	apiClient       utils.ApiClientInterface
	info            Infos
	nodeInfoJson    string
	mutex           sync.Mutex
	conns           []net.Conn
	stop            chan struct{}
	rescheduleCount int
	debugPort       int
	canRSNS         map[string]bool
}

func NewRescheduleDebug(apiClient utils.ApiClientInterface, debugPort int) *RescheduleDebug {
	return &RescheduleDebug{
		apiClient:       apiClient,
		mutex:           sync.Mutex{},
		conns:           make([]net.Conn, 0),
		info:            make(map[string]*NodeInfos),
		canRSNS:         make(map[string]bool),
		rescheduleCount: 0,
		debugPort:       debugPort,
	}
}

func (rsd *RescheduleDebug) Run() {
	go func() {
		lis, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(rsd.debugPort))
		defer lis.Close()
		if err != nil {
			rsd.Log(fmt.Sprintf("Error Listen %v ", err.Error()))
			return
		}

		for {
			conn, err := lis.Accept()
			if err != nil {
				rsd.Log(fmt.Sprintf("Error accepting client: ", err.Error()))
			}

			go func(c net.Conn) {
				rsd.mutex.Lock()
				defer rsd.mutex.Unlock()
				rsd.conns = append(rsd.conns, c)
				rsd.Log(fmt.Sprintf("Connect to RescheduleDebugServer success, clinet size = %d", len(rsd.conns)))
				rsd.sendNodeInfo(c)
			}(conn)
		}
	}()
}

func (rsd *RescheduleDebug) SetCanReschedulerNS(canrsns map[string]bool) {
	rsd.canRSNS = make(map[string]bool, len(canrsns))
	for ns, canrs := range canrsns {
		if canrs {
			rsd.canRSNS[ns] = true
		}
	}
}
func (rsd *RescheduleDebug) BeginOneReschedule() {
	rsd.rescheduleCount++
	rsd.sendMsg(INFOTYPE_RESCHEDULE_STARTONERESCHEDULE, fmt.Sprintf("%d", rsd.rescheduleCount))
	rsd.SyncNodeInfo()

}
func (rsd *RescheduleDebug) SyncNodeInfo() {
	jsonStr, isChange := rsd.getNodesInfo()
	if isChange {
		rsd.nodeInfoJson = jsonStr
		go func() {
			rsd.mutex.Lock()
			defer rsd.mutex.Unlock()
			for _, conn := range rsd.conns {
				rsd.sendNodeInfo(conn)
			}
		}()
	}
}
func (rsd *RescheduleDebug) EndOneReschedule() {
	rsd.sendMsg(INFOTYPE_RESCHEDULE_STARTONERESCHEDULE, fmt.Sprintf("%d", rsd.rescheduleCount))
}
func getNodePods(apiClient utils.ApiClientInterface, nodeName string, canrsns map[string]bool) ([]*v1.Pod, error) {
	podsOnNode, err := apiClient.GetPods(v1.NamespaceAll, nodeName)
	if err != nil {
		returnNull := make([]*v1.Pod, 0)
		return returnNull, err
	}
	ret := make([]*v1.Pod, 0, len(podsOnNode))
	for _, p := range podsOnNode {
		if canrsns[p.Namespace] == true {
			ret = append(ret, p)
		}
	}
	return ret, nil
}
func (rsd *RescheduleDebug) getNodesInfo() (jsonStr string, isChanged bool) {
	nodes := rsd.apiClient.ListReadyNodes()

	var changed bool = false
	if len(nodes) != len(rsd.info) {
		rsd.info = make(map[string]*NodeInfos)
		changed = true
	}
	for _, node := range nodes {
		//podsOnNode, err := rsd.client.Pods(v1.NamespaceAll).List(
		//	v1.ListOptions{FieldSelector: fields.SelectorFromSet(fields.Set{"spec.nodeName": node.Name})})
		podsOnNode, err := getNodePods(rsd.apiClient, node.Name, rsd.canRSNS)
		if err != nil {
			return "", true
		}
		ni, find := rsd.info[node.Name]
		if !find || len(ni.PodInfos) != len(podsOnNode) {
			ni = &NodeInfos{
				NodeName: node.Name,
				PodInfos: make([]PodInfos, len(podsOnNode)),
			}
			rsd.info[node.Name] = ni
			changed = true
		}

		for j, pod := range podsOnNode {
			if ni.PodInfos[j].Namespace != pod.Namespace || ni.PodInfos[j].Name != pod.Name {
				ni.PodInfos[j].Namespace = pod.Namespace
				ni.PodInfos[j].Name = pod.Name
				changed = true
			}
		}
	}
	ret, _ := json.Marshal(rsd.info)
	return string(ret), changed
}
func (rsd *RescheduleDebug) sendNodeInfo(conn net.Conn) {
	rsd.sendMsg(INFOTYPE_NODEINFO, rsd.nodeInfoJson)
}
func (rsd *RescheduleDebug) PodRescheduled(podNamespace, fromPodName, toPodName, fromNode, toNode, messge string, isOk bool) {
	if isOk {
		rsd.sendMsg(INFOTYPE_RESCHEDULE_OK, podNamespace+":"+fromPodName+":"+toPodName+":"+fromNode+":"+toNode+":"+messge)
	} else {
		//rsd.sendMsg(INFOTYPE_RESCHEDULE_FAIL, podNamespace+":"+fromPodName+":"+toPodName+":"+fromNode+":"+toNode+":"+messge)
	}
}
func (rsd *RescheduleDebug) Log(msg string) {
	fmt.Printf("RescheduleDebug: %s\n", msg)
	rsd.sendMsg(INFOTYPE_MESSAGE, msg)
}
func (rsd *RescheduleDebug) sendMsg(typemsg, msg string) {
	go func(typemsg, msg string) {
		rsd.mutex.Lock()
		defer rsd.mutex.Unlock()
		tmpConn := make([]net.Conn, 0, len(rsd.conns))
		for _, conn := range rsd.conns {
			_, err := conn.Write([]byte("#" + typemsg + "->" + msg + "#"))
			if err != nil {
				fmt.Printf("sendMsg write error %v\n", err)
			} else {
				tmpConn = append(tmpConn, conn)
			}
		}
		rsd.conns = tmpConn
	}(typemsg, msg)
}
