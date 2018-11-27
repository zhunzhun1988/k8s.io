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
	//"k8s.io/kubernetes/pkg/kubectl"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/kubectl/cmd/templates"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/util/i18n"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
)

var (
	hostpathpv_describe_valid_resources = `Valid resource types include:

    * node
    * pv
    * pod
    `

	hostpathpv_describe_long = templates.LongDesc(`
		Show details of a specific resource diskquota info.

		` + hostpathpv_describe_valid_resources)

	hostpathpv_describe_example = templates.Examples(`
		# Describe pods quota info.
		kubectl hostpathpv describe pods

		# Describe specified pod quota info.
		kubectl hostpathpv describe pods podname

		# Describe all node quota info.
		kubectl hostpathpv describe nodes

		# Describe a node quota info with specified NAME.
		kubectl hostpathpv describe node nodename

		# Describe all pvs quota inf.
		kubectl hostpathpv describe pvs

		# Describe a pv quota info with specified NAME.
		kubectl hostpathpv describe pv pvname`)
)

type DiskQuotaDetail struct {
	pvname    string
	pvtype    string
	hostpath  string
	pod       string
	container string
	podpath   string
	quota     int64
	used      int64
}

func NewCmdHostPathPVDescribe(f cmdutil.Factory, out io.Writer, errOut io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "describe (TYPE/NAME ...) [flags]",
		Short:   i18n.T("Describe node pod or pv quota information"),
		Long:    hostpathpv_describe_long,
		Example: hostpathpv_describe_example,
		Run: func(cmd *cobra.Command, args []string) {
			err := RunHostpathPVDescribe(f, out, errOut, cmd, args)
			cmdutil.CheckErr(err)
		},
	}
	cmd.Flags().Bool("filter", true, "Ignored no disk quota resource")
	cmd.Flags().Bool("all-namespaces", false, "If present, list the requested object(s) across all namespaces. Namespace in current context is ignored even if specified with --namespace.")
	return cmd
}

func RunHostpathPVDescribe(f cmdutil.Factory, out, errOut io.Writer, cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, "You must specify the type of resource to get. ", hostpathpv_describe_valid_resources)
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
		describePods(clientset, namespaces, podName, filter)
	case resource == "nodes" || resource == "node":
		nodeName := ""
		if len(args) >= 2 {
			nodeName = args[1]
		}
		describeNodes(clientset, nodeName, filter)
	case resource == "pvs" || resource == "pv":
		pvName := ""
		if len(args) >= 2 {
			pvName = args[1]
		}
		describePVs(clientset, pvName, filter)
	default:
		fmt.Fprint(errOut, "You must specify the type of resource to describe. ", hostpathpv_describe_valid_resources)
		usageString := "Required resource not suport."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	return nil
}

func describePods(clientset internalclientset.Interface, namespaces, podName string, filter bool) {
	pods, err := clientset.Core().Pods(namespaces).List(metav1.ListOptions{})
	if err != nil {
		return
	}
	if len(pods.Items) == 0 {
		return
	}
	pvs, err := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})

	for i, _ := range pods.Items {
		pod := &pods.Items[i]
		if podName != "" && podName != pod.Name {
			continue
		}
		displayer := NewDisplayer("  ", "  ", "PV", "HostPath", "Container", "PodPath", "Type", "Quota", "Used")
		//displayer.AddCutOffLine("-")
		var needPrint bool
		for _, podVolume := range pod.Spec.Volumes {
			if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
				pvname, hostPath, pvtype, quota, used := getPodMountHostPathByPvc(pvcSource.ClaimName, pod, pvs)
				if hostPath != "" {
					needPrint = true
					containers, podPaths := getPodPathByVolumeName(podVolume.Name, pod)
					if len(containers) > 0 && len(containers) == len(podPaths) {
						for j, container := range containers {
							displayer.AddLine(pvname, hostPath, container, podPaths[j], pvtype,
								convertIntToString(quota),
								convertIntToString(used)+getPercentStr(used, quota))
						}
					}
					if len(containers) == 0 {
						displayer.AddLine(pvname, hostPath, "***", "***", pvtype,
							convertIntToString(quota),
							convertIntToString(used)+getPercentStr(used, quota))
					}
				}
			}
		}
		if filter && needPrint {
			fmt.Printf("Name:\t\t %s\n", pod.Name)
			fmt.Printf("Namespace:\t %s\n", pod.Namespace)
			fmt.Printf("Node:\t\t %s\n", pod.Spec.NodeName)
			fmt.Printf("HostPath Mount:\n")
			displayer.Print(true)
			fmt.Printf("\n\n")
		}
	}

}

func getPodMountHostPathByPvc(pvcSource string, pod *core.Pod, pvs *core.PersistentVolumeList) (pvname, hostpath, pvtype string, quota, used int64) {
	for _, pv := range pvs.Items {
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != pod.Namespace || pv.Spec.ClaimRef.Name != pvcSource { // no bound pvc
			continue
		}
		if pv.Spec.HostPath == nil && isHostPathCSIPV(&pv) == false {
			return pv.Name, "", "", 0, 0
		}

		if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
			return pv.Name, "Unknow", getPVTypeStr(&pv), 0, 0
		}
		mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

		mountList := pvnodeaffinity.HostPathPVMountInfoList{}
		errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
		if errUmarshal != nil {
			return pv.Name, "Unknow", getPVTypeStr(&pv), 0, 0
		}

		for _, item := range mountList {
			if item.NodeName == pod.Spec.NodeName {
				t := getPVType(&pv)
				if t == KeepFalse || t == NoneFalse {
					if len(item.MountInfos) > 0 {
						return pv.Name, item.MountInfos[0].HostPath, getPVTypeStr(&pv), item.MountInfos[0].VolumeQuotaSize, item.MountInfos[0].VolumeCurrentSize
					}
					return pv.Name, "Unknow", getPVTypeStr(&pv), 0, 0
				}
				for _, mountInfo := range item.MountInfos {
					if mountInfo.PodInfo != nil {
						_, _, uid := getPodInfo(mountInfo.PodInfo.Info)
						if uid == string(pod.UID) {
							return pv.Name, mountInfo.HostPath, getPVTypeStr(&pv), mountInfo.VolumeQuotaSize, mountInfo.VolumeCurrentSize
						}
					}
				}
				return pv.Name, "Unknow", getPVTypeStr(&pv), 0, 0
			}
		}
	}
	return "", "", "", 0, 0
}

func getPodPathByPVCName(pvcName string, pod *core.Pod) (containers, podPaths []string) {
	for _, podVolume := range pod.Spec.Volumes {
		if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
			if pvcSource.ClaimName == pvcName {
				return getPodPathByVolumeName(podVolume.Name, pod)
			}
		}
	}
	return
}
func getPodPathByVolumeName(vName string, pod *core.Pod) (containers, podPaths []string) {
	if pod == nil {
		return
	}
	for _, c := range pod.Spec.Containers {
		for _, volumeMount := range c.VolumeMounts {
			if volumeMount.Name == vName {
				containers = append(containers, c.Name)
				podPaths = append(podPaths, volumeMount.MountPath)
			}
		}
	}
	return
}

func getPodsWithPVCOfNode(pvcname, nodename, namespace string, pods *core.PodList) []*core.Pod {
	ret := make([]*core.Pod, 0, 10)
	for i, _ := range pods.Items {
		pod := &pods.Items[i]
		if pod.Namespace != namespace || pod.Spec.NodeName != nodename {
			continue
		}
		for _, podVolume := range pod.Spec.Volumes {
			if pvcSource := podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil && pvcSource.ClaimName == pvcname {
				ret = append(ret, pod)
				break
			}
		}
	}
	return ret
}
func getDisableStr(disable bool) string {
	if disable {
		return "Disabled"
	} else {
		return "OK"
	}
}
func describeNodes(clientset internalclientset.Interface, nodeName string, filter bool) {
	nodes, err := clientset.Core().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return
	}
	if len(nodes.Items) == 0 {
		return
	}
	pvs, err := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	pods, err := clientset.Core().Pods(v1.NamespaceAll).List(metav1.ListOptions{})

	for i, _ := range nodes.Items {
		node := &nodes.Items[i]
		if nodeName != "" && nodeName != node.Name {
			continue
		}

		displayer1 := NewDisplayer("   ", "  ", "DiskPath", "Capacity", "Quota", "Used", "PathNum", "Keep", "None", "Share", "Status")
		//displayer1.AddCutOffLine("-")

		quotaInfo := getNodeQuotaInfos(node, pvs)
		if quotaInfo.diskNum == 0 && filter == true {
			continue
		}
		fmt.Printf("Name:\t\t%s\n", node.Name)
		fmt.Printf("Quota Disks:\n")
		for _, diskInfo := range quotaInfo.diskInfos {
			displayer1.AddLine(diskInfo.path, convertIntToString(diskInfo.capacity),
				convertIntToString(diskInfo.keep+diskInfo.none+diskInfo.share)+getPercentStr(diskInfo.keep+diskInfo.none+diskInfo.share, diskInfo.capacity),
				convertIntToString(diskInfo.used)+getPercentStr(diskInfo.used, diskInfo.capacity),
				strconv.Itoa(diskInfo.dirNum),
				convertIntToString(diskInfo.keep), convertIntToString(diskInfo.none),
				convertIntToString(diskInfo.share), getDisableStr(diskInfo.disabled))
		}
		displayer1.Print(true)
		fmt.Printf("\n")
		displayers := make([]*Displayer, 0, len(quotaInfo.diskInfos))
		for _, diskInfo := range quotaInfo.diskInfos {
			displayer2 := NewDisplayer("  ", " ", "PVName", "Type", "HostPath", "Pod", "PodPath", "Quota", "Used")
			//displayer2.AddCutOffLine("-")
			displayers = append(displayers, displayer2)
			details := getDiskQuotaDetails(diskInfo.path, node, pvs, pods)
			for _, detail := range details {
				displayer2.AddLine(detail.pvname, detail.pvtype, path.Base(detail.hostpath), detail.pod+":"+detail.container,
					detail.podpath, convertIntToString(detail.quota), convertIntToString(detail.used))
			}

		}
		SyncDisplayersColumeLen(displayers)
		for i, diskInfo := range quotaInfo.diskInfos {
			fmt.Printf("%s quota info(%d) [%s]:\n", diskInfo.path, diskInfo.dirNum, getDisableStr(diskInfo.disabled))
			displayers[i].Print(true)
			fmt.Printf("\n")
		}

		fmt.Printf("\n\n")
	}
}

func getDiskQuotaDetails(diskpath string, node *core.Node, pvs *core.PersistentVolumeList, pods *core.PodList) []DiskQuotaDetail {
	ret := make([]DiskQuotaDetail, 0, 10)
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

		for _, item := range mountList {
			if item.NodeName == node.Name {
				for _, mountInfo := range item.MountInfos {
					if strings.HasPrefix(path.Clean(mountInfo.HostPath), diskpath) {
						if mountInfo.PodInfo != nil { // PodInfo != nil only for keeptrue and nonetrue pv
							_, _, uid := getPodInfo(mountInfo.PodInfo.Info)
							pod := getPodByUID(uid, pods)
							if pod != nil && pv.Spec.ClaimRef != nil {
								containers, paths := getPodPathByPVCName(pv.Spec.ClaimRef.Name, pod)
								for i, container := range containers {
									if i == 0 { // more than one container share the same hostpath only show the first
										ret = append(ret, DiskQuotaDetail{pvname: pv.Name, pvtype: getPVTypeStr(&pv), hostpath: mountInfo.HostPath,
											pod: pod.Namespace + "/" + pod.Name, container: container, podpath: paths[i], quota: mountInfo.VolumeQuotaSize,
											used: mountInfo.VolumeCurrentSize,
										})
									} else {
										ret = append(ret, DiskQuotaDetail{pvname: "", pvtype: "", hostpath: "",
											pod: pod.Namespace + "/" + pod.Name, container: container, podpath: paths[i], quota: mountInfo.VolumeQuotaSize,
											used: mountInfo.VolumeCurrentSize,
										})
									}
								}
								if len(containers) == 0 && len(paths) == 0 {
									ret = append(ret, DiskQuotaDetail{pvname: pv.Name, pvtype: getPVTypeStr(&pv), hostpath: mountInfo.HostPath,
										pod: pod.Namespace + "/" + pod.Name, container: "***", podpath: "***", quota: mountInfo.VolumeQuotaSize,
										used: mountInfo.VolumeCurrentSize,
									})
								}
							} else {
								ret = append(ret, DiskQuotaDetail{pvname: pv.Name, pvtype: getPVTypeStr(&pv), hostpath: mountInfo.HostPath,
									pod: "***", container: "***", podpath: "", quota: mountInfo.VolumeQuotaSize,
									used: mountInfo.VolumeCurrentSize,
								})
							}
						} else { // PodInfo == nil only for keepfalse and nonefalse pv
							if pv.Spec.ClaimRef == nil || getPVTypeStr(&pv) == "KeepTrue" || getPVTypeStr(&pv) == "NoneTrue" {
								ret = append(ret, DiskQuotaDetail{pvname: pv.Name, pvtype: getPVTypeStr(&pv), hostpath: mountInfo.HostPath,
									pod: "***", container: "***", podpath: "", quota: mountInfo.VolumeQuotaSize,
									used: mountInfo.VolumeCurrentSize,
								})
								continue
							}
							pvPods := getPodsWithPVCOfNode(pv.Spec.ClaimRef.Name, node.Name, pv.Spec.ClaimRef.Namespace, pods)
							if len(pvPods) > 0 {
								var index int
								for _, pvPod := range pvPods {
									containers, paths := getPodPathByPVCName(pv.Spec.ClaimRef.Name, pvPod)
									for i, container := range containers {
										if index == 0 { // share the same hostpath the first show the pv name type and hostpath
											ret = append(ret, DiskQuotaDetail{pvname: pv.Name, pvtype: getPVTypeStr(&pv), hostpath: mountInfo.HostPath,
												pod: pvPod.Namespace + "/" + pvPod.Name, container: container, podpath: paths[i], quota: mountInfo.VolumeQuotaSize,
												used: mountInfo.VolumeCurrentSize,
											})
										} else { // share the same hostpath others not show the pv name type and hostpath
											ret = append(ret, DiskQuotaDetail{pvname: "", pvtype: "", hostpath: "",
												pod: pvPod.Namespace + "/" + pvPod.Name, container: container, podpath: paths[i], quota: mountInfo.VolumeQuotaSize,
												used: mountInfo.VolumeCurrentSize,
											})
										}
										index++
									}
									if len(containers) == 0 && len(paths) == 0 {
										if index == 0 {
											ret = append(ret, DiskQuotaDetail{pvname: pv.Name, pvtype: getPVTypeStr(&pv), hostpath: mountInfo.HostPath,
												pod: pvPod.Namespace + "/" + pvPod.Name, container: "***", podpath: "***", quota: mountInfo.VolumeQuotaSize,
												used: mountInfo.VolumeCurrentSize,
											})
										} else {
											ret = append(ret, DiskQuotaDetail{pvname: "", pvtype: "", hostpath: "",
												pod: pvPod.Namespace + "/" + pvPod.Name, container: "***", podpath: "***", quota: mountInfo.VolumeQuotaSize,
												used: mountInfo.VolumeCurrentSize,
											})
										}
										index++
									}
								}
							} else { // pod is deleted
								ret = append(ret, DiskQuotaDetail{pvname: pv.Name, pvtype: getPVTypeStr(&pv), hostpath: mountInfo.HostPath,
									pod: "***", container: "***", podpath: "", quota: mountInfo.VolumeQuotaSize,
									used: mountInfo.VolumeCurrentSize,
								})
							}

						}
					}
				}
			}
		}
	}
	return ret
}

func getPodByUID(uid string, pods *core.PodList) *core.Pod {
	for i, _ := range pods.Items {
		if string(pods.Items[i].UID) == uid {
			return &pods.Items[i]
		}
	}
	return nil
}
func describePVs(clientset internalclientset.Interface, pvName string, filter bool) {
	pvs, err := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	if err != nil {
		fmt.Printf("describe pv get pv err:%v\n", err)
		return
	}
	pods, err := clientset.Core().Pods(v1.NamespaceAll).List(metav1.ListOptions{})
	if err != nil {
		fmt.Printf("describe pv get pod err:%v\n", err)
		return
	}
	for _, pv := range pvs.Items {
		if pvName != "" && pv.Name != pvName {
			continue
		}
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
		if filter && len(mountList) == 0 {
			continue
		}
		fmt.Printf("PVName:\t\t%s\n", pv.Name)
		fmt.Printf("PVType:\t\t%s\n", getPVTypeStr(&pv))
		fmt.Printf("Capacity:\t%s\n", strings.Trim(convertIntToString(getPVCapacity(&pv)), " "))
		fmt.Printf("MountNodes:\t%d\n", len(mountList))
		var sumQuota, sumUsed int64
		displayers := make([]*Displayer, 0, len(mountList))
		for _, item := range mountList {
			displayer := NewDisplayer("   ", "  ", "HostPath", "Pod", "PodPath", "Quota", "Used")
			//displayer.AddCutOffLine("-")
			displayers = append(displayers, displayer)
			for _, mountInfo := range item.MountInfos {
				sumQuota += mountInfo.VolumeQuotaSize
				sumUsed += mountInfo.VolumeCurrentSize
				if mountInfo.PodInfo != nil {
					_, _, uid := getPodInfo(mountInfo.PodInfo.Info)
					pod := getPodByUID(uid, pods)
					if pod != nil {
						if pv.Spec.ClaimRef != nil {
							containers, paths := getPodPathByPVCName(pv.Spec.ClaimRef.Name, pod)
							for i, container := range containers {
								var hostPath string
								if i == 0 {
									hostPath = mountInfo.HostPath
								}
								displayer.AddLine(hostPath, pod.Namespace+"/"+pod.Name+":"+container, paths[i],
									convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
							}
							if len(containers) == 0 && len(paths) == 0 {
								displayer.AddLine(mountInfo.HostPath, pod.Namespace+"/"+pod.Name+":"+"***", "***",
									convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))

							}
						} else {
							displayer.AddLine(mountInfo.HostPath, pod.Namespace+"/"+pod.Name+":"+"Unknow", "Unknow",
								convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
						}
					} else {
						displayer.AddLine(mountInfo.HostPath, "***", "***",
							convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
					}
				} else {
					if pv.Spec.ClaimRef != nil {
						if getPVTypeStr(&pv) != "KeepTrue" && getPVTypeStr(&pv) != "NoneTrue" {
							pvPods := getPodsWithPVCOfNode(pv.Spec.ClaimRef.Name, item.NodeName, pv.Spec.ClaimRef.Namespace, pods)
							var index int
							for _, pvPod := range pvPods {
								containers, paths := getPodPathByPVCName(pv.Spec.ClaimRef.Name, pvPod)
								for i, container := range containers {
									var hostpath string
									if index == 0 {
										hostpath = mountInfo.HostPath

									}
									displayer.AddLine(hostpath, pvPod.Namespace+"/"+pvPod.Name+":"+container, paths[i],
										convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
									index++
								}
								if len(containers) == 0 && len(paths) == 0 {
									if index == 0 {
										displayer.AddLine(mountInfo.HostPath, pvPod.Namespace+"/"+pvPod.Name+":"+"***", "***",
											convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
									} else {
										displayer.AddLine("", pvPod.Namespace+"/"+pvPod.Name+":"+"***", "***",
											convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
									}
									index++
								}
							}
							if len(pvPods) == 0 {
								displayer.AddLine(mountInfo.HostPath, "***", "***",
									convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
							}
						} else {
							displayer.AddLine(mountInfo.HostPath, "***", "***",
								convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
						}

					} else {
						displayer.AddLine(mountInfo.HostPath, "Unknow", "Unknow",
							convertIntToString(mountInfo.VolumeQuotaSize), convertIntToString(mountInfo.VolumeCurrentSize))
					}
				}
			}

		}
		fmt.Printf("AllQuota:\t%s\n", strings.Trim(convertIntToString(sumQuota), " "))
		fmt.Printf("AllUsed:\t%s\n", strings.Trim(convertIntToString(sumUsed)+getPercentStr(sumUsed, sumQuota), " "))
		SyncDisplayersColumeLen(displayers)
		for i, item := range mountList {
			fmt.Printf("%s: (Create quota path num %d)\n", item.NodeName, len(item.MountInfos))
			displayers[i].Print(true)
			fmt.Printf("\n")
		}

		fmt.Printf("\n\n")
	}
}
