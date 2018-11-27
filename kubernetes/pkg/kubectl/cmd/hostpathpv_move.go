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
	"os"
	"os/signal"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/resource"
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

// this is the scp image entrypoint.sh
// fGetSizeByHuman()
// {
//   sizeStr=""
//   sum=$1
//   if [ $sum -ge $GB ]; then
//      gb=$(awk 'BEGIN{printf "%.2f\n", '$sum'/'$GB'}')
//      sizeStr="$gb GB"
//   elif [ $sum -ge $MB ]; then
//      mb=$(awk 'BEGIN{printf "%.2f\n", '$sum'/'$MB'}')
//      sizeStr="$mb MB"
//   elif [ $sum -ge $KB ]; then
//      kb=$(awk 'BEGIN{printf "%.2f\n", '$sum'/'$KB'}')
//      sizeStr="$kb KB"
//   else
//      sizeStr="$sum B"
//   fi
//   echo $sizeStr
// }
//
// index=0
// todir="/todir-"
// echo "" > /fail.txt
// echo "" > /result.txt
//
// (( GB = 1024 * 1024 * 1024 ))
// (( MB = 1024 * 1024 ))
// (( KB = 1024 ))
//
// for dir in $*
// do
//   scpto=$todir$index
//   (( index = index +1 ))
//   {
//      echo "start scp $dir $scpto"
//      sleep 1
//      scp -r $dir $scpto 1>/dev/null 2>/dev/null
//      if [ "$?" == "0" ]; then
//         echo "stop scp $dir $scpto success"
//      else
//         echo "true" > /fail.txt
//         echo "stop scp $dir $scpto fail"
//      fi
//      echo "stop scp $dir $scpto" >> /result.txt
//   }&
// done
//
// {
//    lastsum=0
//    while true
//    do
//       sum=0
//       index=0
//       for dir in $*
//       do
//          dirname=${dir##*/}
//
//         scpto=$todir$index/$dirname
//          (( index = index + 1 ))
//          size=`du -b --max-depth=0 $scpto 2>/dev/null | awk '{print $1}'`
//          (( sum = sum + size ))
//       done
//
//       (( diff= sum - lastsum ))
//       lastsum=$sum
//
//       echo "Has moved $(fGetSizeByHuman $sum) ($(fGetSizeByHuman $diff)/s)"
//       if [ "$(cat /result.txt | grep stop | wc -l)" == "$index" ]; then
//          exit 0
//       fi
//       sleep 1
//    done
// }&
//
// wait
//
// if [ "$(cat /fail.txt)" == "true" ]; then
//    echo "scp fail"
//    exit 1
// else
//    echo "scp success"
//    exit 0
// fi

var (
	move_valid_resources = `Valid resource types include:

    * node
    `

	move_long = templates.LongDesc(`
		Move node quota path from one disk to other disk.

		` + move_valid_resources)

	move_example = templates.Examples(`
		# Move quota path.
		kubectl hostpathpv move node nodename --from=/xfs/disk1/dir --to=/xfs/disk2/
		
		kubectl hostpathpv move node --from=node1:/xfs/disk1/dir1,/xfs/disk2/dir2 --to=node2:/xfs/disk2,/xfs/disk3
		`)
)

const (
	default_move_busyboximage = "127.0.0.1:29006/library/hostpathscpmove:v2"
)

type cleanDeferFun func()

var gf cmdutil.Factory

func NewCmdHostPathPVMove(f cmdutil.Factory, out io.Writer, errOut io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "move (TYPE/NAME ...) [flags]",
		Short:   i18n.T("Move quota path"),
		Long:    move_long,
		Example: move_example,
		Run: func(cmd *cobra.Command, args []string) {
			gf = f
			err := RunMove(f, out, errOut, cmd, args)
			cmdutil.CheckErr(err)
		},
	}
	cmd.Flags().String("from", "", "Move quota path from")
	cmd.Flags().String("to", "", "Move quota to")
	cmd.Flags().Bool("force", false, "Move force with no confirmation")
	cmd.Flags().Int("movetimeout", 100000, "Move pod scp timeout")
	cmd.Flags().Int("tmppvkeepwait", 60, "Wait time at create tmp pv")
	cmd.Flags().String("moveimage", default_move_busyboximage, "Image create to move dir")
	return cmd
}

func RunMove(f cmdutil.Factory, out, errOut io.Writer, cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(errOut, "You must specify the type of resource to get. ", move_valid_resources)
		usageString := "Required resource not specified."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	clientset, err := f.ClientSet()
	if err != nil {
		return err
	}

	resource := args[0]
	fromDir := cmdutil.GetFlagString(cmd, "from")
	toDir := cmdutil.GetFlagString(cmd, "to")
	moveForce := cmdutil.GetFlagBool(cmd, "force")
	moveImage := cmdutil.GetFlagString(cmd, "moveimage")
	moveTimeout := cmdutil.GetFlagInt(cmd, "movetimeout")
	tmppvkeepwait := cmdutil.GetFlagInt(cmd, "tmppvkeepwait")

	if fromDir == "" || toDir == "" {
		fmt.Fprint(errOut, "Please input move from path and to path")
		return cmdutil.UsageErrorf(cmd, "input error")
	}

	switch {
	case resource == "node" || resource == "nodes":
		nodeName := ""
		if len(args) >= 2 {
			nodeName = args[1]
		}
		if nodeName != "" {
			err := moveNode(clientset, nodeName, fromDir, toDir, moveForce, moveImage, moveTimeout)
			if err != nil {
				fmt.Fprintf(errOut, "\nmove err: %v\n", err)
			}
		} else {
			err := moveNodeToNode(clientset, fromDir, toDir, moveForce, moveImage, moveTimeout, tmppvkeepwait)
			if err != nil {
				fmt.Fprintf(errOut, "\nmove err: %v\n", err)
			}
		}
	default:
		fmt.Fprint(errOut, "You must specify the type of resource to describe. ", move_valid_resources)
		usageString := "Required resource not suport."
		return cmdutil.UsageErrorf(cmd, usageString)
	}

	return nil
}

func checkMoveNodeToNodePath(p string) (node string, dirs []string, err error) {
	strs := strings.Split(p, ":")
	if len(strs) != 2 {
		return "", []string{}, fmt.Errorf("path %s err", p)
	}
	paths := strings.Split(strs[1], ",")
	for i := range paths {
		paths[i] = path.Clean(paths[i])
	}
	return strs[0], filterEmptyStr(paths), nil
}

func getPodHostPaths(podnamespace, podname, nodename string, pvs *core.PersistentVolumeList) (keepPaths, noneKeepPaths []string) {
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
			if item.NodeName == nodename {
				for _, mountInfo := range item.MountInfos {
					if mountInfo.PodInfo != nil {
						ns, name, _ := getPodInfo(mountInfo.PodInfo.Info)
						if ns != podnamespace || name != podname {
							continue
						}
						if pv.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathKeep {
							keepPaths = append(keepPaths, mountInfo.HostPath)
						} else {
							noneKeepPaths = append(noneKeepPaths, mountInfo.HostPath)
						}
					}
				}
			}
		}
	}
	return
}
func checkMovePathsIsBelongOnePod(paths []string, allPods *core.PodList, pvs *core.PersistentVolumeList, nodeName string) error {
	if len(paths) == 0 {
		return fmt.Errorf("check path is empty")
	}
	pods, err := getQuotaPathUsePods(allPods, pvs, nodeName, paths[0])
	if err != nil {
		return err
	}
	if len(pods) == 0 {
		return nil
	}
	kpaths, _ := getPodHostPaths(pods[0].Namespace, pods[0].Name, nodeName, pvs)
	if len(paths) < len(kpaths) {
		return fmt.Errorf("%v is not included int move paths", kpaths)
	}
	for _, kp := range kpaths {
		find := false
		for _, p := range paths {
			if path.Clean(kp) == path.Clean(p) {
				find = true
				break
			}
		}
		if find == false {
			return fmt.Errorf("%s is not include in move paths", kp)
		}
	}
	for _, p := range paths {
		find := false
		for _, kp := range kpaths {
			if path.Clean(kp) == path.Clean(p) {
				find = true
				break
			}
		}
		if find == false {
			return fmt.Errorf("%s should not include in move paths", p)
		}
	}
	return nil
}

func getTodirsByFromDirs(frompaths, topaths []string) []string {
	ret := make([]string, 0, len(topaths))
	if len(frompaths) != len(topaths) {
		return []string{}
	}
	for i := range topaths {
		ret = append(ret, path.Join(topaths[i], path.Base(frompaths[i])))
	}
	return ret
}

var cleanDeferFunList []cleanDeferFun

func addCleanDeferFun(fun cleanDeferFun) {
	cleanDeferFunList = append(cleanDeferFunList, fun)
}
func runCleanDeferFun() {
	fmt.Printf("Do cleanup work...")
	for _, fun := range cleanDeferFunList {
		fun()
	}
	fmt.Println()
	cleanDeferFunList = make([]cleanDeferFun, 0, 10)
}

func handleSignal(ch chan os.Signal) {
	select {
	case <-ch:
		break
	}
	runCleanDeferFun()
	os.Exit(0)
}

func moveNodeToNode(clientset internalclientset.Interface, fromDir, toDir string, moveForce bool, moveImage string, movetimeout, tmppvkeepwait int) error {
	cleanDeferFunList = make([]cleanDeferFun, 0, 10)
	defer runCleanDeferFun()
	signalChan := make(chan os.Signal)
	go handleSignal(signalChan)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)

	var fromNodeName, toNodeName string
	var fromDirs, toDirs []string
	var err error
	var step int = 1
	pvs, errGetPVs := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	if errGetPVs != nil {
		return fmt.Errorf("get pvs err:%v", errGetPVs)
	}
	allPods, errPods := clientset.Core().Pods(core.NamespaceAll).List(metav1.ListOptions{})
	if errPods != nil {
		return fmt.Errorf("get pods err:%v", errPods)
	}

	if fromNodeName, fromDirs, err = checkMoveNodeToNodePath(fromDir); err != nil {
		fmt.Printf(" Fail\n")
		return fmt.Errorf("from dir %s err", fromDir)
	}
	if toNodeName, toDirs, err = checkMoveNodeToNodePath(toDir); err != nil {
		fmt.Printf(" Fail\n")
		return fmt.Errorf("to dir %s err", toDir)
	}
	if len(fromDirs) != len(toDirs) {
		fmt.Printf(" Fail\n")
		return fmt.Errorf("len(fromDirs) != len(toDirs)")
	}

	nodeFrom, errGetNode1 := clientset.Core().Nodes().Get(fromNodeName, metav1.GetOptions{})
	if errGetNode1 != nil {
		fmt.Printf(" Fail\n")
		return fmt.Errorf("get from node %s err:%v", fromNodeName, errGetNode1)
	}

	nodeTo, errGetNode2 := clientset.Core().Nodes().Get(toNodeName, metav1.GetOptions{})
	if errGetNode2 != nil {
		fmt.Printf(" Fail\n")
		return fmt.Errorf("get to node %s err:%v", toNodeName, errGetNode2)
	}
	fmt.Printf("\n")
	for i := range fromDirs {
		fmt.Printf("%s:%s -> %s:%s\n", fromNodeName, path.Clean(fromDirs[i]), toNodeName, path.Clean(toDirs[i]))
	}
	if moveForce == false {
		fmt.Printf("Are you sure to move these hostpaths (y/n):")
		var ans byte
		fmt.Scanf("%c", &ans)
		if ans != 'y' && ans != 'Y' {
			return nil
		}
	}
	//************************************ step 1 ***********************************/
	//fmt.Printf("(Step %d) Start check quota path is moveable:", step)
	statueChan := make(chan string, 0)
	defer close(statueChan)
	stepPrintf(statueChan, "(Step %d) Start check quota path is moveable:", step)
	if err := checkMovePathsIsBelongOnePod(fromDirs, allPods, pvs, fromNodeName); err != nil {
		statueChan <- "Fail"
		return err
	}
	if err := checkCanMove(clientset, nodeFrom, nodeTo, pvs, fromDirs, toDirs); err != nil {
		statueChan <- "Fail"
		runtime.Gosched()
		return err
	}
	step++
	statueChan <- "OK"
	runtime.Gosched()

	//************************************ step 2 ***********************************/
	pvName := fmt.Sprintf("move-%s-tmp-pv-%s", toNodeName, getHashStr(getTodirsByFromDirs(fromDirs, toDirs), 5))
	stepPrintf(statueChan, "(Step %d) Start create tmp pv %s and sleep 10s to keep dir in %s:", step, pvName, toNodeName)

	//fromDirQuotaSize, _, _ := getNodeQuotaPathQuotaSize(pvs, nodeFrom.Name, fromDir)
	if err, pv := createTmpPV(clientset, pvName, toNodeName, getTodirsByFromDirs(fromDirs, toDirs), 0); err != nil {
		statueChan <- "Fail"
		runtime.Gosched()
		return fmt.Errorf("create pv:%s err:%v\n", pvName, err)
	} else {
		addCleanDeferFun(func() {
			errDeletePV := deletePV(clientset, pv)
			if errDeletePV != nil {
				fmt.Printf("\nclean : delete pv %s err:%v\n", pv.Name, errDeletePV)
			} else {
				//fmt.Printf("\nclean : delete pv %s sucess\n", pv.Name)
			}
		})
	}
	step++

	stop := make(chan struct{}, 0)
	printTimeDelay(getDelayStyle1(tmppvkeepwait, 1*time.Second, stop), stop)
	time.Sleep(time.Duration(tmppvkeepwait) * time.Second)
	stop <- struct{}{}
	close(stop)
	time.Sleep(10 * time.Microsecond)
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 3 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start set node %s, %s unscheduleable:", step, fromNodeName, toNodeName)
	var setNodeUnScheduled []string
	if setNodeUnScheduled, err = setNodeScheduleable(clientset, []string{fromNodeName, toNodeName}, true); err != nil {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("set node %s scheduleable false err:%v\n", fromNodeName, err)
	} else {
		addCleanDeferFun(func() {
			setNodeScheduleable(clientset, setNodeUnScheduled, false)
		})
	}
	step++
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 4 ***********************************/
	pods, err := getQuotaPathUsePods(allPods, pvs, fromNodeName, fromDirs[0])
	if err != nil {
		return fmt.Errorf("getQuotaPathUsePods err:%v", err)
	}
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
	}
	stepPrintf(statueChan, "(Step %d) Start delete pods[%v]:", step, names)
	if err := deletePods(clientset, fromNodeName, pods, 0); err != nil {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("delete pods err:%v\n", err)
	}
	step++
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 5 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start wait pods[%v] deleted:", step, names)
	if err := waitPodsDeleted(clientset, fromNodeName, pods, false); err != nil {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("wait delete pods err:%v\n", err)
	}
	step++
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 6 ***********************************/
	podFromName := fmt.Sprintf("move-%s-tmp-pod-from-%s", fromNodeName, getHashStr(fromDirs, 5))
	stepPrintf(statueChan, "(Step %d) Start create from pod to move:", step)
	var tmpPodFrom *core.Pod
	var errCreateTmpPodFrom error
	if errCreateTmpPodFrom, tmpPodFrom = createTmpPodFrom(clientset, fromNodeName, podFromName, fromDirs[0], moveImage); errCreateTmpPodFrom != nil {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("create pod:%s err:%v\n", podFromName, errCreateTmpPodFrom)
	} else {
		addCleanDeferFun(func() {
			errPodDelete := waitPodsDeleted(clientset, fromNodeName, []*core.Pod{tmpPodFrom}, true)
			if errPodDelete != nil {
				fmt.Printf("\nclean : delele pod %s:%s err:%v\n", tmpPodFrom.Namespace, tmpPodFrom.Name, errPodDelete)
			} else {
				//fmt.Printf("\nclean : delele pod %s:%s success\n", tmpPod.Namespace, tmpPod.Name)
			}
		})
	}
	step++
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 7 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start wait from pod [%s] to running:", step, tmpPodFrom.Name)
	waitPodIp := ""
	if pods, err := waitPodRunning(clientset, fromNodeName, []*core.Pod{tmpPodFrom}); err != nil || len(pods) != 1 {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("wait from pod [%s] to running err:%v\n", podFromName, err)
	} else {
		waitPodIp = pods[0].Status.PodIP
		statueChan <- "OK"
		time.Sleep(10 * time.Microsecond)
	}
	step++

	//************************************ step 8 ***********************************/
	var tmpPodMove *core.Pod
	var errCreateTmpPodMove error
	podToName := fmt.Sprintf("move-%s-tmp-pod-to-%s", toNodeName, getHashStr(getTodirsByFromDirs(fromDirs, toDirs), 5))
	stepPrintf(statueChan, "(Step %d) Start create scp move pod :%s", step, podToName)
	if errCreateTmpPodMove, tmpPodMove = createTmpPodMove(clientset, toNodeName, podToName, waitPodIp, fromDirs, toDirs, moveImage); errCreateTmpPodMove != nil {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("create pod scp move err:%v\n", errCreateTmpPodMove)
	} else {
		addCleanDeferFun(func() {
			errPodDelete := waitPodsDeleted(clientset, toNodeName, []*core.Pod{tmpPodMove}, true)
			if errPodDelete != nil {
				fmt.Printf("\nclean : delele pod %s:%s err:%v\n", tmpPodMove.Namespace, tmpPodMove.Name, errPodDelete)
			}
		})
	}
	step++
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 9 *********************************Volume**/
	stepPrintf(statueChan, "(Step %d) Start wait to pod [%s] to running:", step, tmpPodMove.Name)

	if pods, err := waitPodRunning(clientset, toNodeName, []*core.Pod{tmpPodMove}); err != nil || len(pods) != 1 {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("wait to pod [%s] to running err:%v\n", podFromName, err)
	} else {
		statueChan <- "OK"
		time.Sleep(10 * time.Microsecond)
	}
	step++

	//************************************ step 10 ***********************************/
	moveSize := getQuotaPathsUsedSize(pvs, fromNodeName, fromDirs)
	timeOut := calcSizeShouldMoveTime(moveSize, movetimeout)
	stepPrintf(statueChan, "(Step %d) Start wait move size=%s , timeOut=%ds:", step, strings.Trim(convertIntToString(moveSize), " "), timeOut)
	//go testfun(clientset, tmpPodMove)
	reader := getMovePodLogReader(clientset, tmpPodMove)
	stopRead := make(chan struct{}, 0)
	var stateLen int
	printTimeDelay(getFromReader(reader, &stateLen), stopRead)
	if err := waitPodQuit(clientset, []*core.Pod{tmpPodMove}, timeOut); err != nil {
		close(stopRead)
		statueChan <- strconv.Itoa(stateLen)
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("wait move pod:%s err:%v\n", tmpPodMove.Name, err)
	}
	close(stopRead)
	step++
	statueChan <- strconv.Itoa(stateLen)
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 11 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start change PV mount history", step)

	for i, fromDir := range fromDirs {
		pv := getPVByNodeMountPath(clientset, pvs, fromNodeName, fromDir)
		if pv == nil {
			statueChan <- "Fail"
			time.Sleep(10 * time.Microsecond)
			return fmt.Errorf("getPVByNodeMountPath err\n")
		}
		if err := changePVMountPath(clientset, pv, fromNodeName, toNodeName, fromDir, path.Join(toDirs[i], path.Base(fromDir))); err != nil {
			statueChan <- "Fail"
			time.Sleep(10 * time.Microsecond)
			return fmt.Errorf("changePVMountPath err:%v\n", err)
		}
	}
	step++
	statueChan <- "OK"
	time.Sleep(10 * time.Microsecond)

	//************************************ step 12 ***********************************/
	stepPrintf(statueChan, "(Step %d) Start set node %v scheduleable:", step, setNodeUnScheduled)
	if _, err := setNodeScheduleable(clientset, setNodeUnScheduled, false); err != nil {
		statueChan <- "Fail"
		time.Sleep(10 * time.Microsecond)
		return fmt.Errorf("set node %s scheduleable true err:%v\n", fromNodeName, err)
	}
	step++
	statueChan <- "OK"

	time.Sleep(10 * time.Microsecond)

	fmt.Printf("\nMove hostpaths from %s to %s success\n", fromNodeName, toNodeName)
	return nil
}

func getMovePodLogReader(clientset internalclientset.Interface, pod *core.Pod) io.ReadCloser {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("painc err:%v\n", err)
		}
	}()
	opts := &core.PodLogOptions{
		Container:  pod.Spec.Containers[0].Name,
		Follow:     true,
		Previous:   false,
		Timestamps: false,
	}
	req := clientset.Core().Pods(pod.Namespace).GetLogs(pod.Name, opts)

	readCloser, err := req.Stream()
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	return readCloser
}
func moveNode(clientset internalclientset.Interface, nodeName, fromDir, toDir string, moveForce bool, moveImage string, movetimeout int) error {
	pvs, errGetPVs := clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	if errGetPVs != nil {
		return fmt.Errorf("get pvs err:%v", errGetPVs)
	}
	node, errGetNode := clientset.Core().Nodes().Get(nodeName, metav1.GetOptions{})
	if errGetNode != nil {
		return fmt.Errorf("get node err:%v", errGetNode)
	}
	allPods, errPods := clientset.Core().Pods(core.NamespaceAll).List(metav1.ListOptions{})
	if errPods != nil {
		return fmt.Errorf("get pods err:%v", errPods)
	}
	var step int = 1
	// step 1
	fmt.Printf("(Step %d) Start check quota path is moveable:", step)
	if err := checkCanMove(clientset, node, node, pvs, []string{fromDir}, []string{toDir}); err != nil {
		fmt.Printf(" Fail\n")
		return err
	}
	step++
	fmt.Printf(" OK\n")

	// step 2
	fmt.Printf("(Step %d) Start create tmp pv to keep %s :", step, path.Join(toDir, path.Base(fromDir)))
	pvName := fmt.Sprintf("move-%s-tmp-pv", nodeName)
	fromDirQuotaSize, _, _ := getNodeQuotaPathQuotaSize(pvs, node.Name, fromDir)
	if err, pv := createTmpPV(clientset, pvName, nodeName, []string{path.Join(toDir, path.Base(fromDir))}, fromDirQuotaSize); err != nil {
		fmt.Printf(" Fail\n")
		fmt.Errorf("create pv:%s err:%v\n", pvName, err)
		return err
	} else {
		defer func() {
			errDeletePV := deletePV(clientset, pv)
			if errDeletePV != nil {
				fmt.Printf("\nclean : delete pv %s err:%v\n", pv.Name, errDeletePV)
			} else {
				//fmt.Printf("\nclean : delete pv %s sucess\n", pv.Name)
			}
		}()
	}
	step++
	fmt.Printf(" OK\n")

	// step 3
	fmt.Printf("(Step %d) Start set node %s unscheduleable:", step, nodeName)
	var setNodeUnScheduled []string
	var errSet error
	if setNodeUnScheduled, errSet = setNodeScheduleable(clientset, []string{nodeName}, true); errSet != nil {
		fmt.Printf(" Fail\n")
		return fmt.Errorf("set node %s scheduleable false err:%v\n", nodeName, errSet)
	} else {
		defer func() {
			setNodeScheduleable(clientset, setNodeUnScheduled, false)
		}()
	}
	step++
	fmt.Printf(" OK\n")

	// step 4
	pvs, errGetPVs = clientset.Core().PersistentVolumes().List(metav1.ListOptions{})
	if errGetPVs != nil {
		return fmt.Errorf("get pvs err:%v", errGetPVs)
	}
	pods, err := getQuotaPathUsePods(allPods, pvs, nodeName, fromDir)
	if err != nil {
		return fmt.Errorf("getQuotaPathUsePods err:%v", err)
	}
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
	}
	fmt.Printf("(Step %d) Start delete pods[%v]:", step, names)
	if err := deletePods(clientset, nodeName, pods, 0); err != nil {
		fmt.Printf(" Fail\n")
		fmt.Errorf("delete pods err:%v\n", err)
		return err
	}
	step++
	fmt.Printf(" OK\n")

	// step 5
	fmt.Printf("(Step %d) Start wait pods[%v] deleted:", step, names)
	if err := waitPodsDeleted(clientset, nodeName, pods, false); err != nil {
		fmt.Printf(" Fail\n")
		fmt.Errorf("wait delete pods err:%v\n", err)
		return err
	}
	step++
	fmt.Printf(" OK\n")

	// step 6
	podName := fmt.Sprintf("move-%s-tmp-pod", nodeName)
	fmt.Printf("(Step %d) Start create pod to move:", step)
	var tmpPod *core.Pod
	var errCreateTmpPod error
	if errCreateTmpPod, tmpPod = createTmpPod(clientset, nodeName, podName, fromDir, toDir, moveImage); errCreateTmpPod != nil {
		fmt.Printf(" Fail\n")
		fmt.Errorf("create pod:%s err:%v\n", podName, errCreateTmpPod)
		return errCreateTmpPod
	} else {
		defer func() {
			errPodDelete := waitPodsDeleted(clientset, nodeName, []*core.Pod{tmpPod}, true)
			if errPodDelete != nil {
				fmt.Printf("\nclean : delele pod %s:%s err:%v\n", tmpPod.Namespace, tmpPod.Name, errPodDelete)
			} else {
				//fmt.Printf("\nclean : delele pod %s:%s success\n", tmpPod.Namespace, tmpPod.Name)
			}
		}()
	}
	step++
	fmt.Printf(" OK\n")

	// step 7
	moveSize := getQuotaPathUsedSize(pvs, nodeName, fromDir)
	timeOut := calcSizeShouldMoveTime(moveSize, movetimeout)
	fmt.Printf("(Step %d) Start move size=%s , timeOut=%ds:", step, strings.Trim(convertIntToString(moveSize), " "), timeOut)
	if err := waitPodQuit(clientset, []*core.Pod{tmpPod}, timeOut); err != nil {
		fmt.Printf(" Fail\n")
		fmt.Errorf("wait move pod:%s err:%v\n", podName, err)
		return err
	}
	step++
	fmt.Printf(" OK\n")

	// step 8Volume
	pv := getPVByNodeMountPath(clientset, pvs, nodeName, fromDir)
	if pv == nil {
		fmt.Printf(" Fail\n")
		return fmt.Errorf("getPVByNodeMountPath err\n")
	}
	fmt.Printf("(Step %d) Start change PV[%s] mount history to %s:%s:", step, pv.Name, nodeName, path.Join(toDir, path.Base(fromDir)))
	if err := changePVMountPath(clientset, pv, nodeName, nodeName, fromDir, path.Join(toDir, path.Base(fromDir))); err != nil {
		fmt.Printf(" Fail\n")
		fmt.Errorf("changePVMountPath err:%v\n", err)
		return err
	}
	step++
	fmt.Printf(" OK\n")

	// step 9
	fmt.Printf("(Step %d) Start set node %s scheduleable:", step, nodeName)
	if _, err := setNodeScheduleable(clientset, setNodeUnScheduled, false); err != nil {
		fmt.Printf(" Fail\n")
		fmt.Errorf("set node %s scheduleable true err:%v\n", nodeName, err)
		return err
	}
	step++
	fmt.Printf(" OK\n")

	fmt.Printf("Move %s:%s to %s:%s success\n", nodeName, fromDir, nodeName, path.Join(toDir, path.Base(fromDir)))
	return nil
}

func deletePV(clientset internalclientset.Interface, pv *core.PersistentVolume) error {
	if pv != nil {
		err := clientset.Core().PersistentVolumes().Delete(pv.Name, &metav1.DeleteOptions{})
		return err
	}
	return nil
}
func changePVMountPath(clientset internalclientset.Interface, pv *core.PersistentVolume, nodeNameFrom, nodeNameTo, fromDir, toDir string) error {
	curPv, err := clientset.Core().PersistentVolumes().Get(pv.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if curPv.Spec.HostPath == nil && isHostPathCSIPV(pv) == false {
		return fmt.Errorf("pv:%s is not hostpath pv", curPv.Name)
	}
	mountInfo := curPv.Annotations[xfs.PVCVolumeHostPathMountNode]

	mountList := pvnodeaffinity.HostPathPVMountInfoList{}
	errUmarshal := json.Unmarshal([]byte(mountInfo), &mountList)
	if errUmarshal != nil {
		return errUmarshal
	}
	deletePath := false
	var deleteMountInfo pvnodeaffinity.MountInfo
	for i, item := range mountList {
		if item.NodeName == nodeNameFrom {
			for j, mountInfo := range item.MountInfos {
				if path.Clean(mountInfo.HostPath) == path.Clean(fromDir) {
					deleteMountInfo = mountList[i].MountInfos[j]
					deleteMountInfo.HostPath = toDir
					mountList[i].MountInfos = append(mountList[i].MountInfos[0:j], mountList[i].MountInfos[j+1:]...)
					deletePath = true
					break
				}
			}
			if len(mountList[i].MountInfos) == 0 {
				mountList = append(mountList[0:i], mountList[i+1:]...)
			}
			break
		}
	}
	if deletePath == false {
		return fmt.Errorf("%s is not find in pv:%s", fromDir, curPv.Name)
	}
	updateTo := false
	for i, item := range mountList {
		if item.NodeName == nodeNameTo {
			mountList[i].MountInfos = append(item.MountInfos, deleteMountInfo)
			updateTo = true
			break
		}
	}
	if updateTo == false {
		mountList = append(mountList, pvnodeaffinity.HostPathPVMountInfo{
			NodeName:   nodeNameTo,
			MountInfos: pvnodeaffinity.MountInfoList{deleteMountInfo},
		})
	}
	buf, err := json.Marshal(mountList)
	if err != nil {
		return err
	}
	curPv.Annotations[xfs.PVCVolumeHostPathMountNode] = string(buf)
	return updatePV(clientset, curPv)
}
func getPVByNodeMountPath(clientset internalclientset.Interface, pvs *core.PersistentVolumeList, nodeName, qutapath string) *core.PersistentVolume {
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
			if item.NodeName == nodeName {
				for _, mountInfo := range item.MountInfos {
					if path.Clean(mountInfo.HostPath) == path.Clean(qutapath) {
						return &pv
					}
				}
			}
		}
	}
	return nil
}

func IsPodNotFound(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

func waitPodRunning(clientset internalclientset.Interface, nodeName string, pods []*core.Pod) ([]*core.Pod, error) {
	retPods := make([]*core.Pod, len(pods))
	for {
		allRunning := true
		for i, pod := range pods {
			curPod, err := clientset.Core().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})

			if err != nil {
				return retPods, err
			}
			if curPod.Status.Phase != core.PodRunning && curPod.Status.Phase != core.PodSucceeded {
				allRunning = false
				break
			} else {
				retPods[i] = curPod
			}
		}
		if allRunning == true {
			return retPods, nil
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	return retPods, nil
}
func waitPodsDeleted(clientset internalclientset.Interface, nodeName string, pods []*core.Pod, force bool) error {
	for _, pod := range pods {
		if force == true {
			err := clientset.Core().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
			if err != nil {
				if !IsPodNotFound(err) {
					return err
				}
			}
		}
		for {
			_, err := clientset.Core().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})
			if err != nil {
				if !IsPodNotFound(err) {
					return err
				}
				break
			}
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}
func deletePods(clientset internalclientset.Interface, nodeName string, pods []*core.Pod, interval time.Duration) error {
	for i, pod := range pods {
		err := clientset.Core().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
		if err != nil {
			if !IsPodNotFound(err) {
				return err
			}
		}
		if i < len(pods)-1 {
			time.Sleep(interval)
		}
	}
	return nil
}
func getPodFromList(ns, name string, pods *core.PodList) (*core.Pod, error) {
	for i := range pods.Items {
		if pods.Items[i].Namespace == ns && pods.Items[i].Name == name {
			return &pods.Items[i], nil
		}
	}
	return nil, fmt.Errorf("not found")
}
func getQuotaPathUsePods(pods *core.PodList, pvs *core.PersistentVolumeList, nodeName, qutapath string) ([]*core.Pod, error) {
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
			if item.NodeName == nodeName {
				for _, mountInfo := range item.MountInfos {
					if path.Clean(mountInfo.HostPath) == path.Clean(qutapath) {
						if mountInfo.PodInfo != nil {
							ns, name, uid := getPodInfo(mountInfo.PodInfo.Info)
							pod, err := getPodFromList(ns, name, pods)
							if err == nil && pod != nil && string(pod.UID) == uid {
								return []*core.Pod{pod}, nil
							} else {
								if pod != nil {
									fmt.Printf("uid:%s, %s\n", uid, pod.UID)
								}

								//return []*api.Pod{}, nil
							}
						} else if pv.Spec.ClaimRef != nil {
							pvPods := getPodsWithPVCOfNode(pv.Spec.ClaimRef.Name, nodeName, pv.Spec.ClaimRef.Namespace, pods)
							return pvPods, nil
						}
					}
				}
			}
		}
	}
	return []*core.Pod{}, nil
}
func setNodeScheduleable(clientset internalclientset.Interface, nodeNames []string, unschedulable bool) ([]string, error) {
	changed := make([]string, 0, len(nodeNames))
	tryCount := 3
	for _, nodeName := range nodeNames {
		for i := 1; i <= tryCount; i++ {
			node, errGetNode := clientset.Core().Nodes().Get(nodeName, metav1.GetOptions{})
			if errGetNode != nil {
				return changed, errGetNode
			}
			if node.Spec.Unschedulable != unschedulable {
				node.Spec.Unschedulable = unschedulable
				_, errUpdate := clientset.Core().Nodes().Update(node)
				if errUpdate != nil {
					if i == tryCount {
						return changed, errUpdate
					}
					time.Sleep(100 * time.Microsecond)
					continue
				}
				changed = append(changed, nodeName)
				break
			}
		}
	}
	return changed, nil
}
func waitPodQuit(clientset internalclientset.Interface, pods []*core.Pod, timeOut int) error {
	for i := 0; i < timeOut; i++ {
		completed := true
		for _, pod := range pods {
			curPod, err := clientset.Core().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})
			if err != nil {
				if !IsPodNotFound(err) {
					return err
				}
				return nil
			}
			if curPod.Status.Phase == core.PodSucceeded {
				continue
			} else if curPod.Status.Phase == core.PodFailed {
				return fmt.Errorf("pod:%s is failed", pod.Name)
			}
			completed = false
			break
		}
		if completed == true {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("wait pod quit timeout")
}

func calcSizeShouldMoveTime(size int64, movetimeout int) int {
	/*size = size / (1024 * 1024) // MB
	ret := size / 40            // 40MB/s
	if ret < 10 {
		ret = 10
	}
	return int(ret)*/
	return movetimeout
}
func getQuotaPathsUsedSize(pvs *core.PersistentVolumeList, nodeName string, quotapaths []string) int64 {
	var ret int64
	for _, path := range quotapaths {
		ret += getQuotaPathUsedSize(pvs, nodeName, path)
	}
	return ret
}

func getQuotaPathUsedSize(pvs *core.PersistentVolumeList, nodeName, qutapath string) int64 {
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
			if item.NodeName == nodeName {
				for _, mountInfo := range item.MountInfos {
					if path.Clean(mountInfo.HostPath) == path.Clean(qutapath) {
						return mountInfo.VolumeCurrentSize
					}
				}
			}
		}
	}
	return 0
}
func createTmpPodMove(clientset internalclientset.Interface, nodeName, podName, serverip string, fromDirs, toDirs []string, image string) (error, *core.Pod) {
	volumes := make([]core.Volume, 0, len(toDirs))
	volumeMounts := make([]core.VolumeMount, 0, len(toDirs))
	for i, toDir := range toDirs {
		volumes = append(volumes, core.Volume{
			Name: fmt.Sprintf("todir-%d", i),
			VolumeSource: core.VolumeSource{
				HostPath: &core.HostPathVolumeSource{
					Path: toDir,
				},
			},
		})
		volumeMounts = append(volumeMounts, core.VolumeMount{
			Name:      fmt.Sprintf("todir-%d", i),
			MountPath: fmt.Sprintf("/todir-%d", i),
		})
	}
	cmd := "/entrypoint.sh"
	/*for i, fromDir := range fromDirs {
		fromDir = path.Clean(fromDir)
		tmp := path.Join(path.Base(path.Dir(fromDir)), path.Base(fromDir))
		if i == 0 {
			cmd = fmt.Sprintf("scp -r %s:/fromdir/%s /todir-%d/", serverip, tmp, i)
		} else {
			cmd += " && " + fmt.Sprintf("scp -r %s:/fromdir/%s /todir-%d/", serverip, tmp, i)
		}
	}*/
	for _, fromDir := range fromDirs {
		fromDir = path.Clean(fromDir)
		tmp := path.Join(path.Base(path.Dir(fromDir)), path.Base(fromDir))
		cmd += " " + fmt.Sprintf("%s:/fromdir/%s", serverip, tmp)
	}
	//fmt.Printf("cmd=%s\n", cmd)
	pod := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: core.NamespaceSystem,
		},
		Spec: core.PodSpec{
			NodeName:      nodeName,
			RestartPolicy: core.RestartPolicyNever,
			Volumes:       volumes,
			Containers: []core.Container{
				{
					Name:            "move",
					Image:           image,
					ImagePullPolicy: core.PullIfNotPresent,
					Command:         []string{"/bin/bash"},
					Args:            []string{"-c", cmd},
					VolumeMounts:    volumeMounts,
					Resources: core.ResourceRequirements{
						Limits:   core.ResourceList{"cpu": resource.MustParse("200m"), "memory": resource.MustParse("8000Mi")},
						Requests: core.ResourceList{"cpu": resource.MustParse("200m"), "memory": resource.MustParse("1000Mi")},
					},
				},
			},
		},
	}
	waitPodsDeleted(clientset, nodeName, []*core.Pod{pod}, true)
	createPod, err := clientset.Core().Pods(core.NamespaceSystem).Create(pod)
	return err, createPod
}

func createTmpPodFrom(clientset internalclientset.Interface, nodeName, podName, fromDir string, image string) (error, *core.Pod) {
	fromDir = path.Clean(fromDir)
	pod := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: core.NamespaceSystem,
		},
		Spec: core.PodSpec{
			NodeName:      nodeName,
			RestartPolicy: core.RestartPolicyNever,
			Volumes: []core.Volume{
				{
					Name: "fromdir",
					VolumeSource: core.VolumeSource{
						HostPath: &core.HostPathVolumeSource{
							Path: path.Dir(path.Dir(fromDir)),
						},
					},
				},
			},
			Containers: []core.Container{
				{
					Name:            "move",
					Image:           image,
					ImagePullPolicy: core.PullIfNotPresent,
					Command:         []string{"/bin/bash"},
					Args:            []string{"-c", "/usr/sbin/sshd -D"},
					VolumeMounts: []core.VolumeMount{
						{
							Name:      "fromdir",
							MountPath: "/fromdir",
						},
					},
					Resources: core.ResourceRequirements{
						Limits:   core.ResourceList{"cpu": resource.MustParse("200m"), "memory": resource.MustParse("256Mi")},
						Requests: core.ResourceList{"cpu": resource.MustParse("200m"), "memory": resource.MustParse("256Mi")},
					},
				},
			},
		},
	}
	waitPodsDeleted(clientset, nodeName, []*core.Pod{pod}, true)
	createPod, err := clientset.Core().Pods(core.NamespaceSystem).Create(pod)
	return err, createPod
}

func createTmpPod(clientset internalclientset.Interface, nodeName, podName, fromDir, toDir string, image string) (error, *core.Pod) {
	timeout := int64(100)
	pod := &core.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: core.NamespaceSystem,
		},
		Spec: core.PodSpec{
			NodeName:              nodeName,
			ActiveDeadlineSeconds: &timeout,
			RestartPolicy:         core.RestartPolicyNever,
			Volumes: []core.Volume{
				{
					Name: "fromdir",
					VolumeSource: core.VolumeSource{
						HostPath: &core.HostPathVolumeSource{
							Path: path.Dir(fromDir),
						},
					},
				},
				{
					Name: "todir",
					VolumeSource: core.VolumeSource{
						HostPath: &core.HostPathVolumeSource{
							Path: toDir,
						},
					},
				},
			},
			Containers: []core.Container{
				{
					Name:            "move",
					Image:           image,
					ImagePullPolicy: core.PullIfNotPresent,
					Command:         []string{"/bin/sh"},
					Args: []string{"-c", fmt.Sprintf("mv  /fromdir/%s /todir/",
						path.Base(fromDir))},
					//Args: []string{"-c", "sleep 10000000"},
					VolumeMounts: []core.VolumeMount{
						{
							Name:      "fromdir",
							MountPath: "/fromdir",
						},
						{
							Name:      "todir",
							MountPath: "/todir",
						},
					},
				},
			},
		},
	}
	waitPodsDeleted(clientset, nodeName, []*core.Pod{pod}, true)
	createPod, err := clientset.Core().Pods(core.NamespaceSystem).Create(pod)
	return err, createPod
}

func createTmpPV(clientset internalclientset.Interface, pvName, nodeName string, toDirs []string, quotaSize int64) (error, *core.PersistentVolume) {
	pv := NewTmpHostPathPV(pvName)
	var ml pvnodeaffinity.MountInfoList
	for _, toDir := range toDirs {
		ml = append(ml, pvnodeaffinity.MountInfo{
			HostPath:             toDir,
			VolumeQuotaSize:      0,
			VolumeCurrentSize:    0,
			VolumeCurrentFileNum: 0,
		})
	}
	mountList := pvnodeaffinity.HostPathPVMountInfoList{
		pvnodeaffinity.HostPathPVMountInfo{
			NodeName:   nodeName,
			MountInfos: ml,
		},
	}

	buf, _ := json.Marshal(mountList)
	pv.Annotations = map[string]string{
		xfs.PVCVolumeHostPathMountNode: string(buf),
		xfs.PVHostPathMountPolicyAnn:   xfs.PVHostPathKeep,
		xfs.PVHostPathQuotaForOnePod:   "true",
	}

	clientset.Core().PersistentVolumes().Delete(pv.Name, nil)
	createPv, err := clientset.Core().PersistentVolumes().Create(pv)

	return err, createPv
}

func checkCanMove(clientset internalclientset.Interface, nodeFrom, nodeTo *core.Node, pvs *core.PersistentVolumeList, fromDirs, toDirs []string) error {
	if len(fromDirs) != len(toDirs) {
		return fmt.Errorf("len(fromDirs) != len(toDirs)")
	}
	if nodeFrom.Name == nodeTo.Name {
		for i := range fromDirs {
			if strings.HasPrefix(path.Clean(fromDirs[i]), path.Clean(toDirs[i])) {
				return fmt.Errorf("can't move to the same node and disk %s", toDirs[i])
			}
		}
	}
	quotaInfoFrom := getNodeQuotaInfos(nodeFrom, pvs)
	if quotaInfoFrom.diskNum == 0 {
		return fmt.Errorf("node %s has no quota disk", nodeFrom.Name)
	}

	quotaInfoTo := getNodeQuotaInfos(nodeTo, pvs)
	if quotaInfoTo.diskNum == 0 {
		return fmt.Errorf("node %s has no quota disk", nodeTo.Name)
	}

	toDiskAvaliabeQuotaSize := make([]int64, len(toDirs))
	for _, fromDir := range fromDirs {
		fromDirOk := false
		for _, disk := range quotaInfoFrom.diskInfos {
			if strings.HasPrefix(path.Clean(fromDir), disk.path) {
				fromDirOk = true
			}
		}
		if fromDirOk == false {
			return fmt.Errorf("%s is not valid quota path at %s", fromDir, nodeFrom.Name)
		}
	}

	for i, toDir := range toDirs {
		toDirOk := false
		for _, disk := range quotaInfoTo.diskInfos {
			if path.Clean(toDir) == path.Clean(disk.path) {
				toDirOk = true
				toDiskAvaliabeQuotaSize[i] = disk.capacity - disk.keep - disk.none - disk.share
				if disk.disabled == true {
					return fmt.Errorf("%s is disabled", toDir)
				}
			}
		}
		if toDirOk == false {
			return fmt.Errorf("%s is not valid quota disk at %s", path.Dir(toDir), nodeTo.Name)
		}
	}

	for i := range fromDirs {
		fromDirQuotaSize, findFromDir, pvType := getNodeQuotaPathQuotaSize(pvs, nodeFrom.Name, fromDirs[i])
		if findFromDir == false {
			return fmt.Errorf("%s is not a quota path at %s", fromDirs[i], nodeFrom.Name)
		}

		if pvType != KeepTrue && pvType != KeepFalse && pvType != CSIKeepTrue && pvType != CSIKeepFalse {
			return fmt.Errorf("only keeptrue and keepfalse support")
		}

		if fromDirQuotaSize > toDiskAvaliabeQuotaSize[i] {
			return fmt.Errorf("move need %s quota size %s only has %s quota size", fromDirs[i],
				strings.Trim(convertIntToString(fromDirQuotaSize), " "),
				strings.Trim(convertIntToString(toDiskAvaliabeQuotaSize[i]), " "))
		}
	}
	return nil
}

func getNodeQuotaPathQuotaSize(pvs *core.PersistentVolumeList, nodename, qutapath string) (int64, bool, int) {
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
			if item.NodeName == nodename {
				for _, mountInfo := range item.MountInfos {
					if path.Clean(mountInfo.HostPath) == path.Clean(qutapath) {
						return mountInfo.VolumeQuotaSize, true, getPVType(&pv)
					}
				}
			}
		}
	}
	return 0, false, PVUnknow
}

func NewTmpHostPathPV(name string) *core.PersistentVolume {
	pv := &core.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"name": name, "app": "kubectlhostpathpv"},
		},
		Spec: core.PersistentVolumeSpec{
			AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteMany},
			Capacity:    core.ResourceList{core.ResourceStorage: *resource.NewQuantity(1, resource.DecimalSI)},
			PersistentVolumeSource: core.PersistentVolumeSource{
				HostPath: &core.HostPathVolumeSource{Path: "\\"},
			},
		},
	}
	return pv
}

func filterEmptyStr(strs []string) []string {
	ret := make([]string, 0, len(strs))
	for _, str := range strs {
		if strings.Trim(str, " ") != "" {
			ret = append(ret, strings.Trim(str, " "))
		}
	}
	return ret
}
