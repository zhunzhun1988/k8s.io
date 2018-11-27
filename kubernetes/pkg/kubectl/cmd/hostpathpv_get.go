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
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	//"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	//"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/kubectl/cmd/templates"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/util/i18n"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
)

var (
	hostpathpv_valid_resources = `Valid resource types include:

    * all
    * node
    * pv
    * pod
    `

	hostpathpv_get_long = templates.LongDesc(`
		Display one or many resources diskquota infomation.

		` + hostpathpv_valid_resources)

	hostpathpv_get_example = templates.Examples(`
		# List all pods quota info in ps output format.
		kubectl hostpathpv get pods

		# List specified pod quota info in ps output format.
		kubectl hostpathpv get pods podname

		# List all node quota info in ps output format.
		kubectl hostpathpv get nodes

		# List a node quota info with specified NAME in ps output format.
		kubectl hostpathpv get node nodename

		# List all pvs quota info in ps output format.
		kubectl hostpathpv get pvs

		# List a pv quota info with specified NAME in ps output format.
		kubectl hostpathpv get pv pvname

		# List all resources quota info.
		kubectl hostpathpv get all`)
)

const (
	PVUnknow = iota
	KeepTrue
	KeepFalse
	NoneTrue
	NoneFalse
	CSIKeepTrue
	CSIKeepFalse
	CSINoneTrue
	CSINoneFalse
)

type DiskInfo struct {
	path     string
	capacity int64
	disabled bool
	keep     int64
	none     int64
	share    int64
	used     int64
	dirNum   int
}

type NodeInfoItem struct {
	name           string
	status         string
	diskNum        int
	diskDisableNum int
	diskCapacity   int64
	diskUsed       int64
	allQuota       int64
	keepQuota      int64
	noneQuota      int64
	shareQuota     int64

	diskInfos []DiskInfo
}

func NewCmdHostPathPVGet(f cmdutil.Factory, out io.Writer, errOut io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get (TYPE/NAME ...) [flags]",
		Short:   i18n.T("Display node pod or pv quota information"),
		Long:    hostpathpv_get_long,
		Example: hostpathpv_get_example,
		Run: func(cmd *cobra.Command, args []string) {
			err := RunHostpathPVGet(f, out, errOut, cmd, args)
			cmdutil.CheckErr(err)
		},
	}
	cmd.Flags().Bool("filter", true, "Ignored no disk quota resource")
	cmd.Flags().Bool("all-namespaces", false, "If present, list the requested object(s) across all namespaces. Namespace in current context is ignored even if specified with --namespace.")
	//cmd.Flags().StringP("human-readable", "h", "", "print human readable sizes (e.g., 1K 234M 2G)")
	return cmd
}

func RunHostpathPVGet(f cmdutil.Factory, out, errOut io.Writer, cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, "You must specify the type of resource to get. ", hostpathpv_valid_resources)
		usageString := "Required resource not specified."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	clientset, err := f.ClientSet()
	if err != nil {
		return err
	}

	resource := args[0]
	filter := cmdutil.GetFlagBool(cmd, "filter")
	namespaces, _, _ := f.ToRawKubeConfigLoader().Namespace()
	allNamespaces := cmdutil.GetFlagBool(cmd, "all-namespaces")
	if allNamespaces == true {
		namespaces = metav1.NamespaceAll
	}
	switch {
	case resource == "pods" || resource == "pod":
		podName := ""
		if len(args) >= 2 {
			podName = args[1]
		}
		if strings.Contains(podName, "/") {
			strs := strings.Split(podName, "/")
			if len(strs) == 2 {
				podName = strs[1]
				namespaces = strs[0]
				if strings.Contains(podName, ":") {
					podName = podName[0:strings.Index(podName, ":")]
				}
			}
		}
		getPods(clientset, namespaces, podName, filter)
	case resource == "nodes" || resource == "node":
		nodeName := ""
		if len(args) >= 2 {
			nodeName = args[1]
		}
		getNodes(clientset, nodeName, filter)
	case resource == "pvs" || resource == "pv":
		pvName := ""
		if len(args) >= 2 {
			pvName = args[1]
		}
		getPVs(clientset, pvName)
	case resource == "all":
		getPods(clientset, namespaces, "", filter)
		getNodes(clientset, "", filter)
		getPVs(clientset, "")
	default:
		fmt.Fprint(errOut, "You must specify the type of resource to get. ", hostpathpv_valid_resources)
		usageString := "Required resource not suport."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	return nil
}

func getPods(clientset internalclientset.Interface, namespaces, podName string, filter bool) {
	pods, err := clientset.Core().Pods(namespaces).List(metav1.ListOptions{})
	if err != nil {
		return
	}
	if len(pods.Items) == 0 {
		return
	}
	pvs, err := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	var sumQuota, sumUsed, sumKeep, sumNone, sumShare int64

	displayer := NewDisplayer("  ", "", "Name", "Status", "HostPathPVNum", "Quota", "Used", "Keep", "None", "Share")

	for i, _ := range pods.Items {
		pod := &pods.Items[i]
		if podName != "" && podName != pod.Name {
			continue
		}
		used, keep, none, share, hostpathpvnum := getPodQuotaInfo(pod, pvs)
		if keep+none+share == 0 && filter == true {
			continue
		}
		displayer.AddLine(pod.Namespace+"/"+pod.Name, string(pod.Status.Phase), strconv.Itoa(hostpathpvnum), convertIntToString(keep+none+share),
			convertIntToString(used), convertIntToString(keep), convertIntToString(none), convertIntToString(share))

		sumQuota += keep + none + share
		sumUsed += used
		sumKeep += keep
		sumNone += none
		sumShare += share
	}
	displayer.Print(true)
	fmt.Printf("AllQuota: %s, AllUsed: %s, AllKeep: %s, AllNone: %s, AllShare: %s\n\n",
		convertIntToString(sumQuota), convertIntToString(sumUsed), convertIntToString(sumKeep),
		convertIntToString(sumNone), convertIntToString(sumShare))
}

func getNodes(clientset internalclientset.Interface, nodeName string, filter bool) {
	nodes, err := clientset.Core().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return
	}
	if len(nodes.Items) == 0 {
		return
	}
	pvs, err := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	displayer := NewDisplayer("   ", "", "Name", "Status", "DiskNum", "Capacity", "Quota", "Used", "Keep", "None", "Share")
	var allCapacity, allQuota, allUsed, allKeep, allNone, allShare int64
	for i, _ := range nodes.Items {
		node := &nodes.Items[i]
		if nodeName != "" && nodeName != node.Name {
			continue
		}
		quotaInfo := getNodeQuotaInfos(node, pvs)
		if quotaInfo.diskNum == 0 && filter == true {
			continue
		}
		displayer.AddLine(quotaInfo.name, quotaInfo.status, fmt.Sprintf("(%d/%d)", quotaInfo.diskNum-quotaInfo.diskDisableNum, quotaInfo.diskNum),
			convertIntToString(quotaInfo.diskCapacity),
			convertIntToString(quotaInfo.allQuota)+getPercentStr(quotaInfo.allQuota, quotaInfo.diskCapacity),
			convertIntToString(quotaInfo.diskUsed)+getPercentStr(quotaInfo.diskUsed, quotaInfo.diskCapacity),
			convertIntToString(quotaInfo.keepQuota)+getPercentStr(quotaInfo.keepQuota, quotaInfo.diskCapacity),
			convertIntToString(quotaInfo.noneQuota)+getPercentStr(quotaInfo.noneQuota, quotaInfo.diskCapacity),
			convertIntToString(quotaInfo.shareQuota)+getPercentStr(quotaInfo.shareQuota, quotaInfo.diskCapacity))
		allCapacity += quotaInfo.diskCapacity
		allQuota += quotaInfo.allQuota
		allUsed += quotaInfo.diskUsed
		allKeep += quotaInfo.keepQuota
		allNone += quotaInfo.noneQuota
		allShare += quotaInfo.shareQuota
	}
	displayer.Print(true)
	fmt.Printf("AllCapacity: %s, AllQuota: %s, AllUsed: %s, AllKeep: %s, AllNone: %s, AllShare: %s\n\n",
		convertIntToString(allCapacity), convertIntToString(allQuota)+getPercentStr(allQuota, allCapacity),
		convertIntToString(allUsed)+getPercentStr(allUsed, allCapacity),
		convertIntToString(allKeep)+getPercentStr(allKeep, allCapacity),
		convertIntToString(allNone)+getPercentStr(allNone, allCapacity),
		convertIntToString(allShare)+getPercentStr(allShare, allCapacity))

}

func getPVs(clientset internalclientset.Interface, pvName string) {
	pvs, err := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		return
	}
	displayer := NewDisplayer("   ", "", "Name", "Status", "NodeNum", "PathNum", "Capacity", "Type", "Quota", "Used")
	fmt.Printf("pvs=%d\n", len(pvs.Items))
	var sumQuota, sumUsed int64
	var sumPaths, sumPVs int
	for i, _ := range pvs.Items {
		pv := &pvs.Items[i]
		if (pv.Spec.HostPath == nil && isHostPathCSIPV(pv) == false) || (pvName != "" && pv.Name != pvName) {
			continue
		}
		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			displayer.AddLine(pv.Name, string(pv.Status.Phase), "0", "0", convertIntToString(getPVCapacity(pv)), getPVTypeStr(pv), convertIntToString(0), convertIntToString(0))
			continue
		}
		sumPVs++
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			continue
		}
		var allQuota, allUsed int64
		var pathNum int
		for _, item := range mountList {
			for _, mountInfo := range item.MountInfos {
				allQuota += mountInfo.VolumeQuotaSize
				allUsed += mountInfo.VolumeCurrentSize
				pathNum++
			}
		}
		sumPaths += pathNum
		sumQuota += allQuota
		sumUsed += allUsed
		displayer.AddLine(pv.Name, string(pv.Status.Phase), strconv.Itoa(len(mountList)), strconv.Itoa(pathNum),
			convertIntToString(getPVCapacity(pv)), getPVTypeStr(pv), convertIntToString(allQuota), convertIntToString(allUsed))
	}
	displayer.Print(true)
	fmt.Printf("NumPVs: %d, NumPaths: %d, AllUsed: %s, AllQuota: %s\n\n",
		sumPVs, sumPVs, convertIntToString(sumUsed), convertIntToString(sumQuota))
}

func getNodeQuotaInfos(node *core.Node, pvs *core.PersistentVolumeList) NodeInfoItem {
	ret := NodeInfoItem{
		name:   node.Name,
		status: getNodeReadyStatus(node),
	}
	if node.Annotations == nil || node.Annotations[xfs.NodeDiskQuotaInfoAnn] == "" {
		return ret
	}
	diskinfo := node.Annotations[xfs.NodeDiskQuotaInfoAnn]
	list := make(xfs.NodeDiskQuotaInfoList, 0)
	errUmarshal := json.Unmarshal([]byte(diskinfo), &list)
	if errUmarshal != nil {
		return ret
	}
	ret.diskNum = len(list)
	ret.diskInfos = make([]DiskInfo, 0, ret.diskNum)
	for _, disk := range list {
		ret.diskCapacity += disk.Allocable
		ret.diskInfos = append(ret.diskInfos, DiskInfo{
			path:     disk.MountPath,
			capacity: disk.Allocable,
			disabled: disk.Disabled,
		})
		if disk.Disabled {
			ret.diskDisableNum++
		}
	}

	for _, pv := range pvs.Items {
		if pv.Spec.HostPath == nil && isHostPathCSIPV(&pv) == false {
			continue
		}
		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			continue
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			continue
		}
		pvType := getPVType(&pv)

		for _, item := range mountList {
			if item.NodeName == node.Name {
				if pvType == KeepFalse || pvType == CSIKeepFalse || pvType == NoneFalse || pvType == CSINoneFalse {
					if len(item.MountInfos) > 0 {
						diskInfo := getDiskInfoByHostPath(item.MountInfos[0].HostPath, ret.diskInfos)
						ret.diskUsed += item.MountInfos[0].VolumeCurrentSize
						ret.shareQuota += item.MountInfos[0].VolumeQuotaSize
						if diskInfo != nil {
							diskInfo.used += item.MountInfos[0].VolumeCurrentSize
							diskInfo.share += item.MountInfos[0].VolumeQuotaSize
							diskInfo.dirNum++
						}
					}
				} else {
					for _, mountInfo := range item.MountInfos {
						diskInfo := getDiskInfoByHostPath(mountInfo.HostPath, ret.diskInfos)
						if pvType == KeepTrue || pvType == CSIKeepTrue {
							if diskInfo != nil {
								diskInfo.used += mountInfo.VolumeCurrentSize
								diskInfo.keep += mountInfo.VolumeQuotaSize
								diskInfo.dirNum++
							}
							ret.keepQuota += mountInfo.VolumeQuotaSize
							ret.diskUsed += mountInfo.VolumeCurrentSize
						} else if pvType == NoneTrue || pvType == CSINoneTrue {
							if diskInfo != nil {
								diskInfo.used += mountInfo.VolumeCurrentSize
								diskInfo.none += mountInfo.VolumeQuotaSize
								diskInfo.dirNum++
							}
							ret.noneQuota += mountInfo.VolumeQuotaSize
							ret.diskUsed += mountInfo.VolumeCurrentSize
						}
					}
				}
				break
			}
		}
	}
	ret.allQuota = ret.keepQuota + ret.noneQuota + ret.shareQuota
	return ret
}

func getDiskInfoByHostPath(hostPath string, diskinfos []DiskInfo) *DiskInfo {
	for i, _ := range diskinfos {
		info := &diskinfos[i]
		if strings.HasPrefix(path.Clean(hostPath), path.Clean(info.path)) {
			return info
		}
	}
	return nil
}
func getPodQuotaInfo(pod *core.Pod, pvs *core.PersistentVolumeList) (used, keep, none, share int64, pvcnum int) {
	podUsedClaim := make(map[string]bool)
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			podUsedClaim[pvcSource.ClaimName] = true
		}
	}
	for _, pv := range pvs.Items {
		if pv.Spec.ClaimRef == nil { // no bound pvc
			continue
		}
		if exist, _ := podUsedClaim[pv.Spec.ClaimRef.Name]; exist == false ||
			pv.Spec.ClaimRef.Namespace != pod.Namespace || (pv.Spec.HostPath == nil && isHostPathCSIPV(&pv) == false) {
			continue
		}
		pvcnum++
		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			continue
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			continue
		}
		pvType := getPVType(&pv)

		for _, item := range mountList {
			if item.NodeName == pod.Spec.NodeName {
				if pvType == KeepFalse || pvType == CSIKeepFalse || pvType == NoneFalse || pvType == CSINoneFalse {
					if len(item.MountInfos) > 0 {
						used += item.MountInfos[0].VolumeCurrentSize
						share += item.MountInfos[0].VolumeQuotaSize
					}
				} else {
					for _, mountInfo := range item.MountInfos {
						if mountInfo.PodInfo != nil {
							_, _, uid := getPodInfo(mountInfo.PodInfo.Info)
							if uid == string(pod.UID) {
								if pvType == KeepTrue || pvType == CSIKeepTrue {
									keep += mountInfo.VolumeQuotaSize
									used += mountInfo.VolumeCurrentSize
								} else if pvType == NoneTrue || pvType == CSINoneTrue {
									none += mountInfo.VolumeQuotaSize
									used += mountInfo.VolumeCurrentSize
								}
								break
							}

						}
					}
				}
				break
			}
		}
	}
	return
}
