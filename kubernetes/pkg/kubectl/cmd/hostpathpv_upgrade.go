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
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/kubectl/cmd/templates"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/util/i18n"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
)

var (
	upgrade_valid_resources = `Valid resource types include:

    * pv
    `

	upgrade_long = templates.LongDesc(`
		Upgrade hostpath pv to CSI hostpathpv.

		` + upgrade_valid_resources)

	upgrade_example = templates.Examples(`
		# Upgrade quota path.
		kubectl hostpathpv upgrade pv pvname
		`)
)

const (
	default_upgrade_busyboximage = "ihub.helium.io:30100/library/busybox:1.25"
	csihostpathpv_plugin_name    = "xfshostpathplugin"
)

func NewCmdHostPathPVUpgrade(f cmdutil.Factory, out io.Writer, errOut io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "upgrade (TYPE/NAME ...) [flags]",
		Short:   i18n.T("Upgrade hostpath pv"),
		Long:    upgrade_long,
		Example: upgrade_example,
		Run: func(cmd *cobra.Command, args []string) {
			gf = f
			err := RunUpgrade(f, out, errOut, cmd, args)
			cmdutil.CheckErr(err)
		},
	}
	cmd.Flags().String("upgradeimage", default_upgrade_busyboximage, "Image create to change quota dir type")
	cmd.Flags().Duration("deleteinterval", 10*time.Second, "Use quota pod deleting interval")
	cmd.Flags().Bool("force", false, "Upgrade force")
	return cmd
}

func RunUpgrade(f cmdutil.Factory, out, errOut io.Writer, cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, "You must specify the type of resource to get. ", upgrade_valid_resources)
		usageString := "Required resource not specified."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	clientset, err := f.ClientSet()
	if err != nil {
		return err
	}

	resource := args[0]
	upgradeImage := cmdutil.GetFlagString(cmd, "upgradeimage")
	delInterval := cmdutil.GetFlagDuration(cmd, "deleteinterval")
	force := cmdutil.GetFlagBool(cmd, "force")
	switch {
	case resource == "pv":
		pvName := ""
		if len(args) >= 2 {
			pvName = args[1]
		}
		if pvName != "" {
			err := upgradePV(clientset, upgradeImage, strings.Split(pvName, ","), force, delInterval)
			if err != nil {
				fmt.Fprintf(errOut, "\nupgrade pv %s err: %v\n", pvName, err)
			}
		} else {
			return fmt.Errorf("pvname should not be empty")
		}
	default:
		fmt.Fprint(errOut, "You must specify the type of resource to describe. ", upgrade_valid_resources)
		usageString := "Required resource not suport."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	return nil
}

func getPVsByNames(clientset internalclientset.Interface, pvNames []string) ([]*core.PersistentVolume, error) {
	ret := make([]*core.PersistentVolume, len(pvNames))
	for i, pvName := range pvNames {
		pv, err := clientset.Core().PersistentVolumes().Get(pvName, metav1.GetOptions{})
		if err != nil {
			return ret, fmt.Errorf("get pv %s err:%v", pvName, err)
		}
		ret[i] = pv
	}
	return ret, nil
}

func upgradePV(clientset internalclientset.Interface, upgradeImage string, pvNames []string, upgradeForce bool, delInterval time.Duration) error {
	// init clean up work
	//upgradeSuccess := false
	cleanDeferFunList = make([]cleanDeferFun, 0, 10)
	defer runCleanDeferFun()
	signalChan := make(chan os.Signal)
	go handleSignal(signalChan)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	//
	updatePVs, errGetPVs := getPVsByNames(clientset, pvNames)
	if errGetPVs != nil {
		return fmt.Errorf("get pvs err:%v", errGetPVs)
	}

	if err := isValidUpgradPVs(updatePVs); err != nil {
		return err
	}
	var needDeletePods []*core.Pod

	if dps, deleteNames, err := getPVsUsingPods(clientset, pvNames); err != nil {
		return err
	} else {
		if upgradeForce == false {
			fmt.Printf("Are you sure to upgrade pv %v by delete pods %v (y/n):", pvNames, deleteNames)
			var ans byte
			fmt.Scanf("%c", &ans)
			if ans != 'y' && ans != 'Y' {
				return nil
			}
		}
		needDeletePods = dps
	}

	statueChan := make(chan string, 0)
	var step int = 1
	completeStep := func(e error) error {
		if e == nil {
			step++
			statueChan <- "OK"
		} else {
			statueChan <- "Fail"
		}
		time.Sleep(10 * time.Microsecond)
		return e
	}
	defer close(statueChan)

	//************************************ step 1 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start create pods to change quotapath type:", step)
	createPods := make([]*core.Pod, 0)
	for _, updatePV := range updatePVs {
		nodeMountInfos, errInfo := getPVQuotaPaths(updatePV)
		if errInfo != nil {
			return completeStep(errInfo)
		}
		pods, err := createPodsToChangeQuotaPathType(clientset, updatePV.Name, upgradeImage, nodeMountInfos)
		addCleanDeferFun(func() {
			errPodDelete := waitPodsDeleted(clientset, "", pods, true)
			if errPodDelete != nil {
				fmt.Printf("\nclean : delele change quotapath type pod err:%v\n", errPodDelete)
			}
		})
		createPods = append(createPods, pods...)
		if err != nil {
			return completeStep(err)
		}
	}
	completeStep(nil)

	//************************************ step 2 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start wait change quotapath type pod exist:", step)
	if err := waitPodQuit(clientset, createPods, 120); err != nil {
		return completeStep(fmt.Errorf("wait change quotapath type pod exist err:%v\n", err))
	}
	completeStep(nil)

	//************************************ step 3 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start create CSI hostpath pv to keep quota dir:", step)
	for _, updatePV := range updatePVs {
		tmpPvName := fmt.Sprintf("%s-csihostpathpv-tmp", getMd5Hash(updatePV.Name, 10))
		if err := createCSIHostPathPV(clientset, tmpPvName, updatePV, false); err != nil {
			return completeStep(err)
		}
		addCleanDeferFun(func() {
			deletePv(clientset, tmpPvName)
		})
	}
	completeStep(nil)

	//************************************ step 4 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start delete old hostpath pv %v:", step, pvNames)
	for _, pvName := range pvNames {
		if err := deletePv(clientset, pvName); err != nil {
			return completeStep(err)
		}
	}
	completeStep(nil)

	//************************************ step 5 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start create CSI hostpath pv to replace:", step)
	for _, updatePV := range updatePVs {
		if err := createCSIHostPathPV(clientset, updatePV.Name, updatePV, true); err != nil {
			return completeStep(err)
		}
	}
	completeStep(nil)

	//************************************ step 6 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start wait CSI hostpath pv bound:", step)
	for _, updatePV := range updatePVs {
		if updatePV.Status.Phase == core.VolumeBound {
			if err := waitPVBound(clientset, updatePV.Name, 40); err != nil {
				return completeStep(err)
			}
		}
	}
	completeStep(nil)

	//************************************ step 7 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start delete using hostpath pv pods:", step)
	if errDelete := deletePods(clientset, "", needDeletePods, delInterval); errDelete != nil {
		return completeStep(errDelete)
	}
	completeStep(nil)

	fmt.Printf("\nUpgrade hostpath pv %v to csi hostpath pv success\n", pvNames)
	return nil
}

func getMd5Hash(str string, l int) string {
	has := md5.Sum([]byte(str))
	md5str1 := fmt.Sprintf("%x", has) //将[]byte转成16进制
	if l >= len(md5str1) {
		return md5str1
	}
	return md5str1[:l]
}

func waitPVBound(clientset internalclientset.Interface, pvName string, timeOut int) error {
	for i := 0; i < timeOut; i++ {
		pv, err := clientset.Core().PersistentVolumes().Get(pvName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pv.Status.Phase == core.VolumeBound {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("wait pod quit timeout")
}

func createPv(clientset internalclientset.Interface, pv *core.PersistentVolume) (*core.PersistentVolume, error) {
	clonePv := pv.DeepCopy()
	clonePv.ResourceVersion = ""
	return clientset.Core().PersistentVolumes().Create(clonePv)
}

func deletePv(clientset internalclientset.Interface, name string) error {
	return clientset.Core().PersistentVolumes().Delete(name, nil)
}

//func listAllPods(clientset internalclientset.Interface) ([]*core.Pod, error) {
//	allPods, errPods := clientset.Core().Pods(core.NamespaceAll).List(metav1.ListOptions{})
//	if errPods != nil {
//		return []*core.Pod{}, fmt.Errorf("get pods err:%v", errPods)
//	}
//	return allPods, nil
//}

func getPVsUsingPods(clientset internalclientset.Interface, pvNames []string) ([]*core.Pod, []string, error) {
	pvcMaps := make(map[string]bool)
	var namespaces string

	for _, pvName := range pvNames {
		pv, errGetPV := clientset.Core().PersistentVolumes().Get(pvName, metav1.GetOptions{})
		if errGetPV != nil {
			return []*core.Pod{}, []string{}, fmt.Errorf("get pv %s err:%v", pvName, errGetPV)
		}
		if pv.Spec.ClaimRef != nil {
			pvcNs := pv.Spec.ClaimRef.Namespace
			pvcName := pv.Spec.ClaimRef.Name
			if namespaces == "" {
				namespaces = pvcNs
			} else if namespaces != pvcNs {
				return []*core.Pod{}, []string{}, fmt.Errorf("has pv belong %s, %s two namespace", namespaces, pvcNs)
			}
			pvcMaps[pvcName] = true
		}
	}
	allPods, errPods := clientset.Core().Pods(namespaces).List(metav1.ListOptions{})
	if errPods != nil {
		return []*core.Pod{}, []string{}, errPods
	}
	ret := make([]*core.Pod, 0, len(allPods.Items))
	retName := make([]string, 0, len(allPods.Items))
	for i := range allPods.Items {
		pod := &allPods.Items[i]
		for _, v := range pod.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && pvcMaps[v.PersistentVolumeClaim.ClaimName] == true {
				ret = append(ret, pod)
				retName = append(retName, pod.Name)
				break
			}
		}
	}
	return ret, retName, nil
}

func createCSIHostPathPV(clientset internalclientset.Interface, pvName string, updatePV *core.PersistentVolume, copyLable bool) error {
	newAnns := make(map[string]string)
	newLable := make(map[string]string)
	if updatePV.Annotations != nil {
		for k, v := range updatePV.Annotations {
			newAnns[k] = v
		}
	}
	if copyLable == false {
		newLable["name"] = pvName
		newLable["app"] = "kubectlhostpathpv"
	} else {
		if updatePV.Labels != nil {
			for k, v := range updatePV.Labels {
				newLable[k] = v
			}
		}
	}
	pv := &core.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pvName,
			Labels:      newLable,
			Annotations: newAnns,
		},
		Spec: core.PersistentVolumeSpec{
			AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteMany},
			Capacity:    core.ResourceList{core.ResourceStorage: updatePV.Spec.Capacity[core.ResourceStorage]},
			PersistentVolumeSource: core.PersistentVolumeSource{
				CSI: &core.CSIPersistentVolumeSource{Driver: csihostpathpv_plugin_name, VolumeHandle: string(updatePV.UID)},
			},
		},
	}
	var lastErr error
	for i := 1; i <= 5; i++ {
		_, err := clientset.Core().PersistentVolumes().Create(pv)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(100 * time.Microsecond)
	}
	return lastErr
}

func createPodsToChangeQuotaPathType(clientset internalclientset.Interface, pvName, imageName string, nodeMountInfos map[string][]string) ([]*core.Pod, error) {
	ret := make([]*core.Pod, 0, len(nodeMountInfos))
	timeout := int64(100)

	getVolumeAndMountVolume := func(paths []string) ([]core.Volume, []core.VolumeMount, string) {
		vs := make([]core.Volume, 0, len(paths))
		vms := make([]core.VolumeMount, 0, len(paths))
		cmd := ""
		for i, p := range paths {
			p = path.Clean(p)
			dirName := fmt.Sprintf("dir%d", i)
			csisDirName := fmt.Sprintf("csis%d", i)
			csiDir := path.Join(path.Dir(p), "csis")
			csiFileName := path.Base(p)
			vms = append(vms, core.VolumeMount{Name: dirName, MountPath: fmt.Sprintf("/changequotapath/dir%d", i)})
			vms = append(vms, core.VolumeMount{Name: csisDirName, MountPath: fmt.Sprintf("/changequotapath/csis%d", i)})
			vs = append(vs, core.Volume{Name: dirName, VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: p}}})
			vs = append(vs, core.Volume{Name: csisDirName, VolumeSource: core.VolumeSource{HostPath: &core.HostPathVolumeSource{Path: csiDir}}})
			if i > 0 {
				cmd += fmt.Sprintf(" && (echo true > /changequotapath/dir%d/csi || echo true > /changequotapath/csis%d/%s)", i, i, csiFileName)
			} else {
				cmd = fmt.Sprintf("(echo true > /changequotapath/dir%d/csi || echo true > /changequotapath/csis%d/%s)", i, i, csiFileName)
			}
		}
		return vs, vms, cmd
	}
	for nodeName, paths := range nodeMountInfos {
		vs, vms, cmd := getVolumeAndMountVolume(paths)
		pod := &core.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("change-%s-%s-quotapath-tmppod", nodeName, pvName),
				Namespace: core.NamespaceSystem,
			},
			Spec: core.PodSpec{
				NodeName:              nodeName,
				ActiveDeadlineSeconds: &timeout,
				RestartPolicy:         core.RestartPolicyNever,
				Volumes:               vs,
				Containers: []core.Container{
					{
						Name:            "change",
						Image:           imageName,
						ImagePullPolicy: core.PullIfNotPresent,
						Command:         []string{"/bin/sh"},
						Args:            []string{"-c", cmd},
						VolumeMounts:    vms,
						Resources: core.ResourceRequirements{
							Limits:   core.ResourceList{"cpu": resource.MustParse("0m"), "memory": resource.MustParse("0Mi")},
							Requests: core.ResourceList{"cpu": resource.MustParse("0m"), "memory": resource.MustParse("0Mi")},
						},
					},
				},
			},
		}
		createPod, err := clientset.Core().Pods(core.NamespaceSystem).Create(pod)
		ret = append(ret, createPod)
		if err != nil {
			return ret, fmt.Errorf("create pod %s:%s err:%v", pod.Namespace, pod.Name, err)
		}
	}
	return ret, nil
}

func getPVQuotaPaths(pv *core.PersistentVolume) (map[string][]string, error) {
	if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
		return map[string][]string{}, nil
	}
	mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
	if errUmarshal != nil {
		return map[string][]string{}, errUmarshal
	}
	ret := make(map[string][]string)
	for _, nodeMount := range mountList {
		paths := make([]string, 0, len(nodeMount.MountInfos))
		for _, info := range nodeMount.MountInfos {
			paths = append(paths, info.HostPath)
		}
		ret[nodeMount.NodeName] = paths
	}
	return ret, nil
}

func isValidUpgradPVs(pvs []*core.PersistentVolume) error {
	for _, pv := range pvs {
		if pv.Spec.HostPath == nil {
			return fmt.Errorf("pv %s is not a hostpath pv", pv.Name)
		}
	}
	return nil
}
