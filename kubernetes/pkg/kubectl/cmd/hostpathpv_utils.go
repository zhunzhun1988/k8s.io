/*Copyright 2017 The Kubernetes Authors.

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

package cmd

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"time"

	"strings"

	//"k8s.io/api/core/v1"
	//nodeutilv1 "k8s.io/kubernetes/pkg/api/v1/node"
	"k8s.io/kubernetes/pkg/apis/core"
	//"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
)

func convertIntToString(size int64) string {
	var ret string
	if size < 1024 {
		ret = fmt.Sprintf("%d B", size)
	} else if size < 1024*1024 {
		ret = fmt.Sprintf("%.2fKB", float64(size)/1024.0)
	} else if size < 1024*1024*1024 {
		ret = fmt.Sprintf("%.2fMB", float64(size)/(1024.0*1024.0))
	} else if size < 1024*1024*1024*1024 {
		ret = fmt.Sprintf("%.2fGB", float64(size)/(1024.0*1024.0*1024.0))
	} else if size < 1024*1024*1024*1024*1024 {
		ret = fmt.Sprintf("%.2fTB", float64(size)/(1024.0*1024.0*1024.0*1024.0))
	} else {
		ret = fmt.Sprintf("%.2fPB", float64(size)/(1024.0*1024.0*1024.0*1024.0*1024.0))
	}
	if len(ret) < 9 {
		ret = getNumSpace(9-len(ret), " ") + ret
	}
	return ret
}

func getPVCapacity(pv *core.PersistentVolume) int64 {
	storage, exists := pv.Spec.Capacity[core.ResourceStorage]
	if exists == false {
		return 0
	}
	capacity := storage.Value()
	if pv.Annotations != nil &&
		pv.Annotations[xfs.PVHostPathCapacityAnn] != "" {
		capacityAnn, errParse := strconv.ParseInt(pv.Annotations[xfs.PVHostPathCapacityAnn], 10, 64)
		if errParse == nil {
			capacity = capacityAnn
		}
	}
	return capacity
}

func getNodeCondition(status *core.NodeStatus, conditionType core.NodeConditionType) (int, *core.NodeCondition) {
	if status == nil {
		return -1, nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	return -1, nil
}

func getNodeReadyStatus(node *core.Node) string {
	_, currentReadyCondition := getNodeCondition(&node.Status, core.NodeReady)
	if currentReadyCondition.Status == core.ConditionTrue {
		return "Ready"
	} else {
		return "unReady"
	}
}

func getPVTypeStr(pv *core.PersistentVolume) string {
	t := getPVType(pv)
	switch t {
	case KeepFalse:
		return "KeepFalse"
	case KeepTrue:
		return "KeepTrue"
	case NoneTrue:
		return "NoneTrue"
	case NoneFalse:
		return "NoneFalse"
	case CSIKeepFalse:
		return "CSIKeepFalse"
	case CSIKeepTrue:
		return "CSIKeepTrue"
	case CSINoneTrue:
		return "CSINoneTrue"
	case CSINoneFalse:
		return "CSINoneFalse"
	}
	return "Unknow"
}

func getHostpathPVType(pv *core.PersistentVolume) int {
	if pv.Annotations == nil {
		return KeepFalse
	} else if pv.Annotations[xfs.PVHostPathMountPolicyAnn] != xfs.PVHostPathNone &&
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
		return KeepTrue
	} else if pv.Annotations[xfs.PVHostPathMountPolicyAnn] != xfs.PVHostPathNone &&
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] != "true" {
		return KeepFalse
	} else if pv.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathNone &&
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] != "true" {
		return NoneFalse
	} else {
		return NoneTrue
	}
}

func getCSIHostpathPVType(pv *core.PersistentVolume) int {
	if pv.Annotations == nil {
		return CSIKeepFalse
	} else if pv.Annotations[xfs.PVHostPathMountPolicyAnn] != xfs.PVHostPathNone &&
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
		return CSIKeepTrue
	} else if pv.Annotations[xfs.PVHostPathMountPolicyAnn] != xfs.PVHostPathNone &&
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] != "true" {
		return CSIKeepFalse
	} else if pv.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathNone &&
		pv.Annotations[xfs.PVHostPathQuotaForOnePod] != "true" {
		return CSINoneFalse
	} else {
		return CSINoneTrue
	}
}

func isHostPathCSIPV(pv *core.PersistentVolume) bool {
	if pv.Spec.CSI != nil && strings.Contains(strings.ToLower(pv.Spec.CSI.Driver), "hostpath") == true {
		return true
	}
	return false
}

func getPVType(pv *core.PersistentVolume) int {
	if pv.Spec.HostPath != nil {
		return getHostpathPVType(pv)
	} else if isHostPathCSIPV(pv) == true {
		return getCSIHostpathPVType(pv)
	}
	return PVUnknow
}

func getPodInfo(str string) (ns, name, uid string) {
	if str != "" {
		strs := strings.Split(str, ":")
		if len(strs) == 3 {
			return strs[0], strs[1], strs[2]
		}
	}
	return
}

func maxint(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func getPercentStr(used, capacity int64) string {
	if capacity <= 0 {
		return "(00.00%)"
	}
	return fmt.Sprintf("(%.2f%%)", (float64(used)/float64(capacity))*100.0)
}

func getDelayStyle1(sum int, interval time.Duration, stop <-chan struct{}) <-chan string {
	strChan := make(chan string, 0)
	go func() {
		i := 1
	loopfor:
		for {
			select {
			case <-stop:
				strChan <- ""
				break loopfor
			default:
				strChan <- fmt.Sprintf(" (%d/%d)", i, sum)
				i++
			}
			time.Sleep(interval)
		}
	}()
	return strChan
}

func getFromReader(r io.Reader, laststrLen *int) <-chan string {
	strChan := make(chan string, 0)
	lineReader := bufio.NewReader(r)
	go func() {
		for {
			line, _, err := lineReader.ReadLine()
			if err != nil {
				//strChan <- ""
				//close(strChan)
			} else {
				if strings.Contains(string(line), "Has moved") {
					tmp := string("[ ") + string(line) + string(" ]")
					strChan <- tmp
					*laststrLen = len(tmp)
				}
			}
		}
	}()
	return strChan
}

type printFun func()

var printFunChan chan printFun

func init() {
	printFunChan = make(chan printFun, 10)
	go func() {
		for {
			select {
			case fun := <-printFunChan:
				fun()
			}
		}
	}()
}

func printTimeDelay(strChan <-chan string, stop <-chan struct{}) {
	lastStr := ""
	go func() {
	loopfor:
		for {
			select {
			case <-stop:
				printFunChan <- func() {
					fmt.Printf("%s", strings.Repeat("\b", len(lastStr)))
				}
				break loopfor
			case str := <-strChan:
				printFunChan <- func() {
					fmt.Printf("%s%s", strings.Repeat("\b", len(lastStr)), str)
					lastStr = str
				}
			}
		}
	}()
}

func stepPrintf(statueChan <-chan string, format string, a ...interface{}) {
	buf := fmt.Sprintf(format, a...)
	//fmt.Printf("%s", buf)
	printFunChan <- func() {
		fmt.Printf("%s", buf)
	}
	go func() {
		l := len(buf)
	forLoop:
		for {
			select {
			case statue := <-statueChan:
				num, err := strconv.Atoi(statue)
				if err == nil { // is num
					l += num
				} else {
					printFunChan <- func() {
						if 120-l > 0 {
							fmt.Printf("%s[ %s ]\n", strings.Repeat("-", 120-l), statue)
						} else {
							fmt.Printf("[ %s ]\n", statue)
						}
					}
					break forLoop
				}
			}
		}
	}()
}

func getHashStr(strs []string, l int) string {
	hash := md5.New()
	for _, str := range strs {
		hash.Write([]byte(str))
	}
	tmp := hex.EncodeToString(hash.Sum(nil))
	if l >= len(tmp) {
		return tmp
	}
	return tmp[:l]
}
