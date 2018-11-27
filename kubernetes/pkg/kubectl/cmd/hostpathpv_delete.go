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
	"time"

	"github.com/spf13/cobra"
	//"k8s.io/api/core/v1"
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
	hostpathpv_delete_valid_resources = `Valid resource types include:

    * pv
    `

	hostpathpv_delete_long = templates.LongDesc(`
		Delete pv quota paths.

		` + hostpathpv_delete_valid_resources)

	hostpathpv_delete_example = templates.Examples(`
		# Delete pv nodename quota path.
		kubectl hostpathpv delete pv pvname --node=nodename --path=quotapath
		
		# Delete pv all quota path.
		kubectl hostpathpv delete pv pvname --all=true
		
		# Delete pv nodename quota path without verify.
		kubectl hostpathpv delete pv pvname --node=nodename --path=quotapath --force=true
		`)
)

func NewCmdHostPathPVDelete(f cmdutil.Factory, out io.Writer, errOut io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete (TYPE/NAME ...) [flags]",
		Short:   i18n.T("Delete pv quota path"),
		Long:    hostpathpv_delete_long,
		Example: hostpathpv_delete_example,
		Run: func(cmd *cobra.Command, args []string) {
			err := RunDelete(f, out, errOut, cmd, args)
			cmdutil.CheckErr(err)
		},
	}
	cmd.Flags().String("node", "", "Delete quota path of node")
	cmd.Flags().String("path", "", "Delete path used with --node")
	cmd.Flags().Bool("all", false, "Delete all quota path")
	cmd.Flags().Bool("force", false, "Delete force with no verify")
	return cmd
}

func RunDelete(f cmdutil.Factory, out, errOut io.Writer, cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, "You must specify the type of resource to get. ", hostpathpv_delete_valid_resources)
		usageString := "Required resource not specified."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	clientset, err := f.ClientSet()
	if err != nil {
		return err
	}

	resource := args[0]
	nodeName := cmdutil.GetFlagString(cmd, "node")
	hostPath := cmdutil.GetFlagString(cmd, "path")
	deleteAll := cmdutil.GetFlagBool(cmd, "all")
	deleteForce := cmdutil.GetFlagBool(cmd, "force")

	if nodeName == "" && deleteAll == false {
		fmt.Fprint(errOut, "You must specify the node. ")
		return cmdutil.UsageErrorf(cmd, "Required node")
	}
	if deleteAll == true {
		nodeName = ""
	}
	switch {
	case resource == "pvs" || resource == "pv":
		pvName := ""
		if len(args) >= 2 {
			pvName = args[1]
		}
		err := deletePVs(clientset, pvName, nodeName, hostPath, deleteForce)
		if err != nil {
			fmt.Fprintf(errOut, "delete err: %v\n", err)
		}
	default:
		fmt.Fprint(errOut, "You must specify the type of resource to describe. ", hostpathpv_delete_valid_resources)
		usageString := "Required resource not suport."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	return nil
}

func deletePVs(clientset internalclientset.Interface, pvName, nodeName, hostPath string, deleteForce bool) error {
	pv, err := clientset.Core().PersistentVolumes().Get(pvName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if pv.Spec.HostPath == nil && isHostPathCSIPV(pv) == false {
		return fmt.Errorf("pv %s is not hostpath pv", pv.Name)
	}

	if pv.Annotations == nil || pv.Annotations[xfs.PVCVolumeHostPathMountNode] == "" {
		return fmt.Errorf("pv %s is no quota path to delete", pv.Name)
	}

	mountInfo := pv.Annotations[xfs.PVCVolumeHostPathMountNode]

	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
	if errUmarshal != nil {
		return errUmarshal
	}

	newMountList, deletePaths := getDeleteInfo(mountList, nodeName, hostPath)
	if len(deletePaths) == 0 {
		fmt.Printf("not quota path to delete\n")
		return nil
	}
	if deleteForce != true {
		for _, path := range deletePaths {
			fmt.Printf("%s\n", path)
		}
		fmt.Printf("Are sure to delete above quota path (y/n):")
		var ok string
		fmt.Scanf("%s", &ok)
		if ok != "y" {
			return nil
		}
	}
	buf, _ := json.Marshal(newMountList)
	pv.Annotations[xfs.PVCVolumeHostPathMountNode] = string(buf)
	errUpdate := updatePV(clientset, pv)
	if errUpdate != nil {
		return errUpdate
	}
	for _, path := range deletePaths {
		fmt.Printf("%s delete ok\n", path)
	}
	return nil
}

func getDeleteInfo(list pvnodeaffinity.HostPathPVMountInfoList,
	nodeName, hostpath string) (newList pvnodeaffinity.HostPathPVMountInfoList, deletePaths []string) {
	for _, mountInfo := range list {
		if nodeName != "" && mountInfo.NodeName == nodeName {
			item := pvnodeaffinity.HostPathPVMountInfo{
				NodeName:   mountInfo.NodeName,
				MountInfos: make(pvnodeaffinity.MountInfoList, 0, len(mountInfo.MountInfos)),
			}
			for _, mountPath := range mountInfo.MountInfos {
				if hostpath == "" || (hostpath != "" && path.Clean(mountPath.HostPath) == path.Clean(hostpath)) {
					deletePaths = append(deletePaths, mountInfo.NodeName+":"+mountPath.HostPath)
				} else {
					item.MountInfos = append(item.MountInfos, mountPath)
				}
			}
			if len(item.MountInfos) > 0 {
				newList = append(newList, item)
			}
		} else if nodeName == "" {
			for _, mountPath := range mountInfo.MountInfos {
				deletePaths = append(deletePaths, mountInfo.NodeName+":"+mountPath.HostPath)
			}
		} else {
			item := pvnodeaffinity.HostPathPVMountInfo{
				NodeName:   mountInfo.NodeName,
				MountInfos: make(pvnodeaffinity.MountInfoList, 0, len(mountInfo.MountInfos)),
			}
			item.MountInfos = append(item.MountInfos, mountInfo.MountInfos...)
			if len(item.MountInfos) > 0 {
				newList = append(newList, item)
			}
		}
	}
	return
}

func updatePV(clientset internalclientset.Interface, pv *core.PersistentVolume) error {
	var err error
	for i := 0; i < 3; i++ {
		_, err = clientset.Core().PersistentVolumes().Update(pv)
		if err == nil {
			return nil
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return err
}
