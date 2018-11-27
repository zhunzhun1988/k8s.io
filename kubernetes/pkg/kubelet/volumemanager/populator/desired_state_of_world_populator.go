/*
Copyright 2016 The Kubernetes Authors.

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

/*
Package populator implements interfaces that monitor and keep the states of the
caches in sync with the "ground truth".
*/
package populator

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/diskquota/xfs"
	"k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/pvnodeaffinity"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

type HostPathVolumeHandled struct {
	handledmap map[string]map[string]bool
	lock       sync.Mutex
}

func NewHostPathVolumeHandled() *HostPathVolumeHandled {
	return &HostPathVolumeHandled{
		handledmap: make(map[string]map[string]bool),
	}
}

func (hpvh *HostPathVolumeHandled) isPodVolumeHandled(podUID string, volumeName string) bool {
	hpvh.lock.Lock()
	defer hpvh.lock.Unlock()
	podMap, find := hpvh.handledmap[podUID]
	if find == false {
		return false
	}
	handled, _ := podMap[volumeName]
	return handled
}

func (hpvh *HostPathVolumeHandled) addPodVolumeHandled(podUID string, volumeName string) {
	hpvh.lock.Lock()
	defer hpvh.lock.Unlock()
	podMap, find := hpvh.handledmap[podUID]
	if find == false {
		podMap = make(map[string]bool)
		hpvh.handledmap[podUID] = podMap
	}
	podMap[volumeName] = true
}

func (hpvh *HostPathVolumeHandled) deletePodVolumeHandled(podUID string, volumeName string) {
	hpvh.lock.Lock()
	defer hpvh.lock.Unlock()
	podMap, find := hpvh.handledmap[podUID]
	if find == true {
		delete(podMap, volumeName)
		if len(podMap) == 0 {
			delete(hpvh.handledmap, podUID)
		}
	}
}

func (hpvh *HostPathVolumeHandled) deletePod(pod *v1.Pod) {
	hpvh.lock.Lock()
	defer hpvh.lock.Unlock()
	delete(hpvh.handledmap, string(pod.UID))
}

// DesiredStateOfWorldPopulator periodically loops through the list of active
// pods and ensures that each one exists in the desired state of the world cache
// if it has volumes. It also verifies that the pods in the desired state of the
// world cache still exist, if not, it removes them.
type DesiredStateOfWorldPopulator interface {
	Run(sourcesReady config.SourcesReady, stopCh <-chan struct{})

	// ReprocessPod removes the specified pod from the list of processedPods
	// (if it exists) forcing it to be reprocessed. This is required to enable
	// remounting volumes on pod updates (volumes like Downward API volumes
	// depend on this behavior to ensure volume content is updated).
	ReprocessPod(podName volumetypes.UniquePodName)

	// HasAddedPods returns whether the populator has looped through the list
	// of active pods and added them to the desired state of the world cache,
	// at a time after sources are all ready, at least once. It does not
	// return true before sources are all ready because before then, there is
	// a chance many or all pods are missing from the list of active pods and
	// so few to none will have been added.
	HasAddedPods() bool
}

// NewDesiredStateOfWorldPopulator returns a new instance of
// DesiredStateOfWorldPopulator.
//
// kubeClient - used to fetch PV and PVC objects from the API server
// loopSleepDuration - the amount of time the populator loop sleeps between
//     successive executions
// podManager - the kubelet podManager that is the source of truth for the pods
//     that exist on this host
// desiredStateOfWorld - the cache to populate
func NewDesiredStateOfWorldPopulator(
	kubeClient clientset.Interface,
	loopSleepDuration time.Duration,
	getPodStatusRetryDuration time.Duration,
	podManager pod.Manager,
	podStatusProvider status.PodStatusProvider,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld,
	kubeContainerRuntime kubecontainer.Runtime,
	keepTerminatedPodVolumes bool,
	pVAffinityManager pvnodeaffinity.PVNodeAffinity,
	dqm xfs.DiskQuotaManager,
	nodename string) DesiredStateOfWorldPopulator {
	return &desiredStateOfWorldPopulator{
		kubeClient:                kubeClient,
		loopSleepDuration:         loopSleepDuration,
		getPodStatusRetryDuration: getPodStatusRetryDuration,
		podManager:                podManager,
		podStatusProvider:         podStatusProvider,
		desiredStateOfWorld:       desiredStateOfWorld,
		actualStateOfWorld:        actualStateOfWorld,
		pods: processedPods{
			processedPods: make(map[volumetypes.UniquePodName]bool)},
		kubeContainerRuntime:     kubeContainerRuntime,
		keepTerminatedPodVolumes: keepTerminatedPodVolumes,
		hasAddedPods:             false,
		hasAddedPodsLock:         sync.RWMutex{},
		pVAffinityManager:        pVAffinityManager,
		dqm:                      dqm,
		nodeName:                 nodename,
		hostPathVolumeHandled:    NewHostPathVolumeHandled(),
	}
}

type desiredStateOfWorldPopulator struct {
	kubeClient                clientset.Interface
	loopSleepDuration         time.Duration
	getPodStatusRetryDuration time.Duration
	podManager                pod.Manager
	podStatusProvider         status.PodStatusProvider
	desiredStateOfWorld       cache.DesiredStateOfWorld
	actualStateOfWorld        cache.ActualStateOfWorld
	pods                      processedPods
	kubeContainerRuntime      kubecontainer.Runtime
	timeOfLastGetPodStatus    time.Time
	keepTerminatedPodVolumes  bool
	hasAddedPods              bool
	hasAddedPodsLock          sync.RWMutex
	pVAffinityManager         pvnodeaffinity.PVNodeAffinity
	dqm                       xfs.DiskQuotaManager
	hostPathVolumeHandled     *HostPathVolumeHandled
	nodeName                  string
}

type processedPods struct {
	processedPods map[volumetypes.UniquePodName]bool
	sync.RWMutex
}

func (dswp *desiredStateOfWorldPopulator) Run(sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	// Wait for the completion of a loop that started after sources are all ready, then set hasAddedPods accordingly
	glog.Infof("Desired state populator starts to run")
	wait.PollUntil(dswp.loopSleepDuration, func() (bool, error) {
		done := sourcesReady.AllReady()
		dswp.populatorLoopFunc()()
		return done, nil
	}, stopCh)
	dswp.hasAddedPodsLock.Lock()
	dswp.hasAddedPods = true
	dswp.hasAddedPodsLock.Unlock()
	wait.Until(dswp.populatorLoopFunc(), dswp.loopSleepDuration, stopCh)
}

func (dswp *desiredStateOfWorldPopulator) ReprocessPod(
	podName volumetypes.UniquePodName) {
	dswp.deleteProcessedPod(podName)
}

func (dswp *desiredStateOfWorldPopulator) HasAddedPods() bool {
	dswp.hasAddedPodsLock.RLock()
	defer dswp.hasAddedPodsLock.RUnlock()
	return dswp.hasAddedPods
}

func (dswp *desiredStateOfWorldPopulator) populatorLoopFunc() func() {
	return func() {
		dswp.findAndAddNewPods()

		// findAndRemoveDeletedPods() calls out to the container runtime to
		// determine if the containers for a given pod are terminated. This is
		// an expensive operation, therefore we limit the rate that
		// findAndRemoveDeletedPods() is called independently of the main
		// populator loop.
		if time.Since(dswp.timeOfLastGetPodStatus) < dswp.getPodStatusRetryDuration {
			glog.V(5).Infof(
				"Skipping findAndRemoveDeletedPods(). Not permitted until %v (getPodStatusRetryDuration %v).",
				dswp.timeOfLastGetPodStatus.Add(dswp.getPodStatusRetryDuration),
				dswp.getPodStatusRetryDuration)

			return
		}

		dswp.findAndRemoveDeletedPods()
	}
}

func (dswp *desiredStateOfWorldPopulator) isPodTerminated(pod *v1.Pod) bool {
	podStatus, found := dswp.podStatusProvider.GetPodStatus(pod.UID)
	if !found {
		podStatus = pod.Status
	}
	return util.IsPodTerminated(pod, podStatus)
}

// Iterate through all pods and add to desired state of world if they don't
// exist but should
func (dswp *desiredStateOfWorldPopulator) findAndAddNewPods() {
	// Map unique pod name to outer volume name to MountedVolume.
	mountedVolumesForPod := make(map[volumetypes.UniquePodName]map[string]cache.MountedVolume)
	if utilfeature.DefaultFeatureGate.Enabled(features.ExpandInUsePersistentVolumes) {
		for _, mountedVolume := range dswp.actualStateOfWorld.GetMountedVolumes() {
			mountedVolumes, exist := mountedVolumesForPod[mountedVolume.PodName]
			if !exist {
				mountedVolumes = make(map[string]cache.MountedVolume)
				mountedVolumesForPod[mountedVolume.PodName] = mountedVolumes
			}
			mountedVolumes[mountedVolume.OuterVolumeSpecName] = mountedVolume
		}
	}

	processedVolumesForFSResize := sets.NewString()
	for _, pod := range dswp.podManager.GetPods() {
		if dswp.isPodTerminated(pod) {
			// Do not (re)add volumes for terminated pods
			continue
		}
		dswp.processPodVolumes(pod, mountedVolumesForPod, processedVolumesForFSResize)
	}
}

// Iterate through all pods in desired state of world, and remove if they no
// longer exist
func (dswp *desiredStateOfWorldPopulator) findAndRemoveDeletedPods() {
	var runningPods []*kubecontainer.Pod

	runningPodsFetched := false
	for _, volumeToMount := range dswp.desiredStateOfWorld.GetVolumesToMount() {
		pod, podExists := dswp.podManager.GetPodByUID(volumeToMount.Pod.UID)
		if podExists {
			// Skip running pods
			if !dswp.isPodTerminated(pod) {
				continue
			}
			if dswp.keepTerminatedPodVolumes {
				continue
			}
		}

		// Once a pod has been deleted from kubelet pod manager, do not delete
		// it immediately from volume manager. Instead, check the kubelet
		// containerRuntime to verify that all containers in the pod have been
		// terminated.
		if !runningPodsFetched {
			var getPodsErr error
			runningPods, getPodsErr = dswp.kubeContainerRuntime.GetPods(false)
			if getPodsErr != nil {
				glog.Errorf(
					"kubeContainerRuntime.findAndRemoveDeletedPods returned error %v.",
					getPodsErr)
				continue
			}

			runningPodsFetched = true
			dswp.timeOfLastGetPodStatus = time.Now()
		}

		runningContainers := false
		for _, runningPod := range runningPods {
			if runningPod.ID == volumeToMount.Pod.UID {
				if len(runningPod.Containers) > 0 {
					runningContainers = true
				}

				break
			}
		}

		if runningContainers {
			glog.V(4).Infof(
				"Pod %q has been removed from pod manager. However, it still has one or more containers in the non-exited state. Therefore, it will not be removed from volume manager.",
				format.Pod(volumeToMount.Pod))
			continue
		}

		if !dswp.actualStateOfWorld.VolumeExists(volumeToMount.VolumeName) && podExists {
			glog.V(4).Infof(volumeToMount.GenerateMsgDetailed("Actual state has not yet has this information skip removing volume from desired state", ""))
			continue
		}
		glog.V(4).Infof(volumeToMount.GenerateMsgDetailed("Removing volume from desired state", ""))

		dswp.desiredStateOfWorld.DeletePodFromVolume(
			volumeToMount.PodName, volumeToMount.VolumeName)
		dswp.deleteProcessedPod(volumeToMount.PodName)
		dswp.handleUnmountHostPathPvc(volumeToMount.Pod, volumeToMount.VolumeSpec, func() {
			dswp.hostPathVolumeHandled.deletePodVolumeHandled(string(volumeToMount.Pod.UID),
				volumeToMount.OuterVolumeSpecName)
		})
	}
}
func (dswp *desiredStateOfWorldPopulator) handleUnmountHostPathPvc(pod *v1.Pod, volumeSpec *volume.Spec, cleanFun func()) error {
	if dswp.dqm != nil && volumeSpec.PersistentVolume != nil && volumeSpec.PersistentVolume.Spec.HostPath != nil {
		defer cleanFun()

		defer dswp.pVAffinityManager.Remove(pvnodeaffinity.PVSyncItem{
			PVName:    volumeSpec.PersistentVolume.Name,
			PodUid:    string(pod.UID),
			MountPath: volumeSpec.PersistentVolume.Spec.HostPath.Path,
		})
		id := string(volumeSpec.PersistentVolume.UID)
		subid := ""
		if volumeSpec.PersistentVolume.Annotations != nil &&
			volumeSpec.PersistentVolume.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
			subid = string(pod.UID)
		}
		ok, err := dswp.dqm.DeletePathQuota(id, subid)
		if ok == false || err != nil {
			return fmt.Errorf("delete pod[%s:%s]: pv %s quota err=%v\n", pod.Namespace, pod.Name, volumeSpec.PersistentVolume.Name, err)
		}
	}
	return nil
}

func (dswp *desiredStateOfWorldPopulator) isPodActive(podid, pvid string) bool {
	if podid == "" && pvid != "" {
		pvs, err := dswp.kubeClient.Core().PersistentVolumes().List(metav1.ListOptions{})
		if err != nil {
			return false
		}
		for _, pv := range pvs.Items {
			if pv.Spec.HostPath == nil { // skip not hostpath pv
				continue
			}
			if string(pv.UID) == pvid {
				return true
			}
		}
		return false
	}
	if podid == "" {
		return true
	}
	if pod, ok := dswp.podManager.GetPodByUID(types.UID(podid)); ok == false || pod == nil {
		return false
	} else {
		podCur, errGet := dswp.kubeClient.Core().Pods(pod.Namespace).Get(pod.Name, metav1.GetOptions{})
		if errGet != nil {
			if errors.IsNotFound(errGet) {
				return false
			} else {
				return true // other error we return true
			}
		}
		return v1.PodSucceeded != podCur.Status.Phase &&
			v1.PodFailed != podCur.Status.Phase &&
			podCur.DeletionTimestamp == nil
	}
}

func getPodOwnerReferencesId(pod *v1.Pod) string {
	if pod != nil && len(pod.OwnerReferences) > 0 {
		if len(pod.OwnerReferences) == 1 {
			return string(pod.OwnerReferences[0].UID)
		} else {
			for _, or := range pod.OwnerReferences {
				if or.Kind == "ReplicationController" || or.Kind == "ReplicaSet" || or.Kind == "StatefulSet" {
					return string(or.UID)
				}
			}
			return string(pod.OwnerReferences[0].UID)
		}
	}
	return ""

}
func (dswp *desiredStateOfWorldPopulator) hasAnyCanUsedQuotaPath(pvAnn map[string]string) (exist bool, unavailablePath string) {
	if pvAnn == nil || pvAnn[xfs.PVCVolumeHostPathMountNode] == "" {
		return false, ""
	}
	mountInfo := pvAnn[xfs.PVCVolumeHostPathMountNode]
	hppmil := pvnodeaffinity.HostPathPVMountInfoList{}
	err := json.Unmarshal([]byte(mountInfo), &hppmil)
	if err != nil {
		return false, ""
	}
	unexistPath := []string{}
	for _, mn := range hppmil {
		if mn.NodeName == dswp.nodeName {
			for _, info := range mn.MountInfos {
				exist, used, _ := dswp.dqm.IsQuotaPathExistAndUsed(info.HostPath, dswp.isPodActive)
				if exist == true && used == false {
					return true, ""
				}
				if exist == false {
					unexistPath = append(unexistPath, info.HostPath)
				}
			}
			break
		}
	}
	if len(unexistPath) > 0 {
		return false, strings.Join(unexistPath, ",")
	}
	return false, ""
}

func isPodNotRunning(pod *v1.Pod) bool {
	if pod == nil || pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded || pod.DeletionTimestamp != nil {
		return true
	}
	return false
}

func (dswp *desiredStateOfWorldPopulator) handleHostPathPvc(pod *v1.Pod, podVolume v1.Volume, volumeSpec *volume.Spec) error {

	if dswp.dqm != nil && volumeSpec.PersistentVolume != nil && volumeSpec.PersistentVolume.Spec.HostPath != nil &&
		podVolume.VolumeSource.PersistentVolumeClaim != nil {
		glog.Infof("handleHostPathPvc %s:%s %s\n", pod.Namespace, pod.Name, podVolume.Name)
		if isPodNotRunning(pod) == true {
			return fmt.Errorf("pod %s:%s is not running", pod.Namespace, pod.Name)
		}
		storage, exists := volumeSpec.PersistentVolume.Spec.Capacity[v1.ResourceStorage]
		if exists == false {
			return nil
		}
		ownerid := string(pod.UID)
		id := string(volumeSpec.PersistentVolume.UID)
		subid := ""
		if uid := getPodOwnerReferencesId(pod); uid != "" {
			// dqm is use the ownid to keep the distribution
			// so if pod is created by rc or rs we use the rc or rs uid as the ownerid
			// it can make the pods of the same rc use different disk to improve the
			// read and write performance
			ownerid = uid
		}
		isUsedOld := false
		if volumeSpec.PersistentVolume.Annotations != nil &&
			volumeSpec.PersistentVolume.Annotations[xfs.PVHostPathQuotaForOnePod] == "true" {
			subid = string(pod.UID)
			if volumeSpec.PersistentVolume.Annotations[xfs.PVHostPathMountPolicyAnn] == xfs.PVHostPathKeep {
				isUsedOld = true
			}
		}
		capacity := storage.Value()
		if volumeSpec.PersistentVolume.Annotations != nil &&
			volumeSpec.PersistentVolume.Annotations[xfs.PVHostPathCapacityAnn] != "" {
			capacityAnn, errParse := strconv.ParseInt(volumeSpec.PersistentVolume.Annotations[xfs.PVHostPathCapacityAnn], 10, 64)
			if errParse == nil {
				capacity = capacityAnn
			}
		}
		if exists, info := dswp.dqm.IsIdXFSQuota(id, subid); exists == true {
			_, errChange := dswp.dqm.ChangeQuota(id, subid, capacity, capacity)
			if errChange != nil {
				glog.Errorf("mount change %s quota to %d error :%v", id, capacity, errChange)
			}
			volumeSpec.PersistentVolume.Spec.HostPath.Path = info.GetPath()
		} else {
			if isUsedOld == true {
				// fixed when disk is unmounted the new quota path will be created at other disk
				if exist, p := dswp.hasAnyCanUsedQuotaPath(volumeSpec.PersistentVolume.Annotations); exist == false && p != "" {
					return fmt.Errorf("quotapath %s is unavailable", p)
				}
			}
			ok, path, err := dswp.dqm.AddPathQuota(ownerid, id, subid, "", string(pod.UID), // mount the pvc
				isUsedOld, capacity, capacity, dswp.isPodActive)
			if err != nil && ok == false {
				glog.Errorf("desiredStateOfWorldPopulator AddPathQuota %s:%s %s err:%v", pod.Namespace, pod.Name, podVolume.Name, err)
				return err
			}
			volumeSpec.PersistentVolume.Spec.HostPath.Path = path
		}
		dswp.pVAffinityManager.Add(pvnodeaffinity.PVSyncItem{
			PVName:    volumeSpec.PersistentVolume.Name,
			MountPath: volumeSpec.PersistentVolume.Spec.HostPath.Path,
			PodUid:    string(pod.UID),
			PodNS:     pod.Namespace,
			PodName:   pod.Name,
		})
		dswp.hostPathVolumeHandled.addPodVolumeHandled(string(pod.UID), podVolume.Name)
		return nil
	}
	return nil
}

// processPodVolumes processes the volumes in the given pod and adds them to the
// desired state of the world.
func (dswp *desiredStateOfWorldPopulator) processPodVolumes(
	pod *v1.Pod,
	mountedVolumesForPod map[volumetypes.UniquePodName]map[string]cache.MountedVolume,
	processedVolumesForFSResize sets.String) {
	if pod == nil {
		return
	}

	uniquePodName := util.GetUniquePodName(pod)
	if dswp.podPreviouslyProcessed(uniquePodName) {
		return
	}

	allVolumesAdded := true
	mountsMap, devicesMap := dswp.makeVolumeMap(pod.Spec.Containers)

	// Process volume spec for each volume defined in pod
	for _, podVolume := range pod.Spec.Volumes {
		if dswp.hostPathVolumeHandled.isPodVolumeHandled(string(pod.UID), podVolume.Name) {
			continue
		}
		pvc, volumeSpec, volumeGidValue, err :=
			dswp.createVolumeSpec(podVolume, pod.Name, pod.Namespace, mountsMap, devicesMap)
		if err != nil {
			glog.Errorf(
				"Error processing volume %q for pod %q: %v",
				podVolume.Name,
				format.Pod(pod),
				err)
			allVolumesAdded = false
			continue
		}
		if err := dswp.handleHostPathPvc(pod, podVolume, volumeSpec); err != nil {
			glog.Errorf("Error processing volume handleHostPathPvc err:%v", err)
			continue
		}
		// Add volume to desired state of world
		_, err = dswp.desiredStateOfWorld.AddPodToVolume(
			uniquePodName, pod, volumeSpec, podVolume.Name, volumeGidValue)
		if err != nil {
			glog.Errorf(
				"Failed to add volume %q (specName: %q) for pod %q to desiredStateOfWorld. err=%v",
				podVolume.Name,
				volumeSpec.Name(),
				uniquePodName,
				err)
			allVolumesAdded = false
		}

		glog.V(4).Infof(
			"Added volume %q (volSpec=%q) for pod %q to desired state.",
			podVolume.Name,
			volumeSpec.Name(),
			uniquePodName)

		if utilfeature.DefaultFeatureGate.Enabled(features.ExpandInUsePersistentVolumes) {
			dswp.checkVolumeFSResize(pod, podVolume, pvc, volumeSpec,
				uniquePodName, mountedVolumesForPod, processedVolumesForFSResize)
		}
	}

	// some of the volume additions may have failed, should not mark this pod as fully processed
	if allVolumesAdded {
		dswp.markPodProcessed(uniquePodName)
		// New pod has been synced. Re-mount all volumes that need it
		// (e.g. DownwardAPI)
		dswp.actualStateOfWorld.MarkRemountRequired(uniquePodName)
	}

}

// checkVolumeFSResize checks whether a PVC mounted by the pod requires file
// system resize or not. If so, marks this volume as fsResizeRequired in ASW.
// - mountedVolumesForPod stores all mounted volumes in ASW, because online
//   volume resize only considers mounted volumes.
// - processedVolumesForFSResize stores all volumes we have checked in current loop,
//   because file system resize operation is a global operation for volume, so
//   we only need to check it once if more than one pod use it.
func (dswp *desiredStateOfWorldPopulator) checkVolumeFSResize(
	pod *v1.Pod,
	podVolume v1.Volume,
	pvc *v1.PersistentVolumeClaim,
	volumeSpec *volume.Spec,
	uniquePodName volumetypes.UniquePodName,
	mountedVolumesForPod map[volumetypes.UniquePodName]map[string]cache.MountedVolume,
	processedVolumesForFSResize sets.String) {
	if podVolume.PersistentVolumeClaim == nil {
		// Only PVC supports resize operation.
		return
	}
	uniqueVolumeName, exist := getUniqueVolumeName(uniquePodName, podVolume.Name, mountedVolumesForPod)
	if !exist {
		// Volume not exist in ASW, we assume it hasn't been mounted yet. If it needs resize,
		// it will be handled as offline resize(if it indeed hasn't been mounted yet),
		// or online resize in subsequent loop(after we confirm it has been mounted).
		return
	}
	fsVolume, err := util.CheckVolumeModeFilesystem(volumeSpec)
	if err != nil {
		glog.Errorf("Check volume mode failed for volume %s(OuterVolumeSpecName %s): %v",
			uniqueVolumeName, podVolume.Name, err)
		return
	}
	if !fsVolume {
		glog.V(5).Infof("Block mode volume needn't to check file system resize request")
		return
	}
	if processedVolumesForFSResize.Has(string(uniqueVolumeName)) {
		// File system resize operation is a global operation for volume,
		// so we only need to check it once if more than one pod use it.
		return
	}
	if mountedReadOnlyByPod(podVolume, pod) {
		// This volume is used as read only by this pod, we don't perform resize for read only volumes.
		glog.V(5).Infof("Skip file system resize check for volume %s in pod %s/%s "+
			"as the volume is mounted as readonly", podVolume.Name, pod.Namespace, pod.Name)
		return
	}
	if volumeRequiresFSResize(pvc, volumeSpec.PersistentVolume) {
		dswp.actualStateOfWorld.MarkFSResizeRequired(uniqueVolumeName, uniquePodName)
	}
	processedVolumesForFSResize.Insert(string(uniqueVolumeName))
}

func mountedReadOnlyByPod(podVolume v1.Volume, pod *v1.Pod) bool {
	if podVolume.PersistentVolumeClaim.ReadOnly {
		return true
	}
	for _, container := range pod.Spec.InitContainers {
		if !mountedReadOnlyByContainer(podVolume.Name, &container) {
			return false
		}
	}
	for _, container := range pod.Spec.Containers {
		if !mountedReadOnlyByContainer(podVolume.Name, &container) {
			return false
		}
	}
	return true
}

func mountedReadOnlyByContainer(volumeName string, container *v1.Container) bool {
	for _, volumeMount := range container.VolumeMounts {
		if volumeMount.Name == volumeName && !volumeMount.ReadOnly {
			return false
		}
	}
	return true
}

func getUniqueVolumeName(
	podName volumetypes.UniquePodName,
	outerVolumeSpecName string,
	mountedVolumesForPod map[volumetypes.UniquePodName]map[string]cache.MountedVolume) (v1.UniqueVolumeName, bool) {
	mountedVolumes, exist := mountedVolumesForPod[podName]
	if !exist {
		return "", false
	}
	mountedVolume, exist := mountedVolumes[outerVolumeSpecName]
	if !exist {
		return "", false
	}
	return mountedVolume.VolumeName, true
}

func volumeRequiresFSResize(pvc *v1.PersistentVolumeClaim, pv *v1.PersistentVolume) bool {
	capacity := pvc.Status.Capacity[v1.ResourceStorage]
	requested := pv.Spec.Capacity[v1.ResourceStorage]
	return requested.Cmp(capacity) > 0
}

// podPreviouslyProcessed returns true if the volumes for this pod have already
// been processed by the populator
func (dswp *desiredStateOfWorldPopulator) podPreviouslyProcessed(
	podName volumetypes.UniquePodName) bool {
	dswp.pods.RLock()
	defer dswp.pods.RUnlock()

	_, exists := dswp.pods.processedPods[podName]
	return exists
}

// markPodProcessed records that the volumes for the specified pod have been
// processed by the populator
func (dswp *desiredStateOfWorldPopulator) markPodProcessed(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	dswp.pods.processedPods[podName] = true
}

// markPodProcessed removes the specified pod from processedPods
func (dswp *desiredStateOfWorldPopulator) deleteProcessedPod(
	podName volumetypes.UniquePodName) {
	dswp.pods.Lock()
	defer dswp.pods.Unlock()

	delete(dswp.pods.processedPods, podName)
}

// createVolumeSpec creates and returns a mutatable volume.Spec object for the
// specified volume. It dereference any PVC to get PV objects, if needed.
// Returns an error if unable to obtain the volume at this time.
func (dswp *desiredStateOfWorldPopulator) createVolumeSpec(
	podVolume v1.Volume, podName string, podNamespace string, mountsMap map[string]bool, devicesMap map[string]bool) (*v1.PersistentVolumeClaim, *volume.Spec, string, error) {
	if pvcSource :=
		podVolume.VolumeSource.PersistentVolumeClaim; pvcSource != nil {
		glog.V(5).Infof(
			"Found PVC, ClaimName: %q/%q",
			podNamespace,
			pvcSource.ClaimName)

		// If podVolume is a PVC, fetch the real PV behind the claim
		pvc, err := dswp.getPVCExtractPV(
			podNamespace, pvcSource.ClaimName)
		if err != nil {
			return nil, nil, "", fmt.Errorf(
				"error processing PVC %q/%q: %v",
				podNamespace,
				pvcSource.ClaimName,
				err)
		}
		pvName, pvcUID := pvc.Spec.VolumeName, pvc.UID

		glog.V(5).Infof(
			"Found bound PV for PVC (ClaimName %q/%q pvcUID %v): pvName=%q",
			podNamespace,
			pvcSource.ClaimName,
			pvcUID,
			pvName)

		// Fetch actual PV object
		volumeSpec, volumeGidValue, err :=
			dswp.getPVSpec(pvName, pvcSource.ReadOnly, pvcUID)
		if err != nil {
			return nil, nil, "", fmt.Errorf(
				"error processing PVC %q/%q: %v",
				podNamespace,
				pvcSource.ClaimName,
				err)
		}

		glog.V(5).Infof(
			"Extracted volumeSpec (%v) from bound PV (pvName %q) and PVC (ClaimName %q/%q pvcUID %v)",
			volumeSpec.Name,
			pvName,
			podNamespace,
			pvcSource.ClaimName,
			pvcUID)

		// TODO: remove feature gate check after no longer needed
		if utilfeature.DefaultFeatureGate.Enabled(features.BlockVolume) {
			volumeMode, err := util.GetVolumeMode(volumeSpec)
			if err != nil {
				return nil, nil, "", err
			}
			// Error if a container has volumeMounts but the volumeMode of PVC isn't Filesystem
			if mountsMap[podVolume.Name] && volumeMode != v1.PersistentVolumeFilesystem {
				return nil, nil, "", fmt.Errorf(
					"Volume %q has volumeMode %q, but is specified in volumeMounts for pod %q/%q",
					podVolume.Name,
					volumeMode,
					podNamespace,
					podName)
			}
			// Error if a container has volumeDevices but the volumeMode of PVC isn't Block
			if devicesMap[podVolume.Name] && volumeMode != v1.PersistentVolumeBlock {
				return nil, nil, "", fmt.Errorf(
					"Volume %q has volumeMode %q, but is specified in volumeDevices for pod %q/%q",
					podVolume.Name,
					volumeMode,
					podNamespace,
					podName)
			}
		}
		return pvc, volumeSpec, volumeGidValue, nil
	}

	// Do not return the original volume object, since the source could mutate it
	clonedPodVolume := podVolume.DeepCopy()

	return nil, volume.NewSpecFromVolume(clonedPodVolume), "", nil
}

// getPVCExtractPV fetches the PVC object with the given namespace and name from
// the API server, checks whether PVC is being deleted, extracts the name of the PV
// it is pointing to and returns it.
// An error is returned if the PVC object's phase is not "Bound".
func (dswp *desiredStateOfWorldPopulator) getPVCExtractPV(
	namespace string, claimName string) (*v1.PersistentVolumeClaim, error) {
	pvc, err :=
		dswp.kubeClient.CoreV1().PersistentVolumeClaims(namespace).Get(claimName, metav1.GetOptions{})
	if err != nil || pvc == nil {
		return nil, fmt.Errorf(
			"failed to fetch PVC %s/%s from API server. err=%v",
			namespace,
			claimName,
			err)
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.StorageObjectInUseProtection) {
		// Pods that uses a PVC that is being deleted must not be started.
		//
		// In case an old kubelet is running without this check or some kubelets
		// have this feature disabled, the worst that can happen is that such
		// pod is scheduled. This was the default behavior in 1.8 and earlier
		// and users should not be that surprised.
		// It should happen only in very rare case when scheduler schedules
		// a pod and user deletes a PVC that's used by it at the same time.
		if pvc.ObjectMeta.DeletionTimestamp != nil {
			return nil, fmt.Errorf(
				"can't start pod because PVC %s/%s is being deleted",
				namespace,
				claimName)
		}
	}

	if pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName == "" {

		return nil, fmt.Errorf(
			"PVC %s/%s has non-bound phase (%q) or empty pvc.Spec.VolumeName (%q)",
			namespace,
			claimName,
			pvc.Status.Phase,
			pvc.Spec.VolumeName)
	}

	return pvc, nil
}

// getPVSpec fetches the PV object with the given name from the API server
// and returns a volume.Spec representing it.
// An error is returned if the call to fetch the PV object fails.
func (dswp *desiredStateOfWorldPopulator) getPVSpec(
	name string,
	pvcReadOnly bool,
	expectedClaimUID types.UID) (*volume.Spec, string, error) {
	pv, err := dswp.kubeClient.CoreV1().PersistentVolumes().Get(name, metav1.GetOptions{})
	if err != nil || pv == nil {
		return nil, "", fmt.Errorf(
			"failed to fetch PV %q from API server. err=%v", name, err)
	}

	if pv.Spec.ClaimRef == nil {
		return nil, "", fmt.Errorf(
			"found PV object %q but it has a nil pv.Spec.ClaimRef indicating it is not yet bound to the claim",
			name)
	}

	if pv.Spec.ClaimRef.UID != expectedClaimUID {
		return nil, "", fmt.Errorf(
			"found PV object %q but its pv.Spec.ClaimRef.UID (%q) does not point to claim.UID (%q)",
			name,
			pv.Spec.ClaimRef.UID,
			expectedClaimUID)
	}

	volumeGidValue := getPVVolumeGidAnnotationValue(pv)
	return volume.NewSpecFromPersistentVolume(pv, pvcReadOnly), volumeGidValue, nil
}

func (dswp *desiredStateOfWorldPopulator) makeVolumeMap(containers []v1.Container) (map[string]bool, map[string]bool) {
	volumeDevicesMap := make(map[string]bool)
	volumeMountsMap := make(map[string]bool)

	for _, container := range containers {
		if container.VolumeMounts != nil {
			for _, mount := range container.VolumeMounts {
				volumeMountsMap[mount.Name] = true
			}
		}
		// TODO: remove feature gate check after no longer needed
		if utilfeature.DefaultFeatureGate.Enabled(features.BlockVolume) &&
			container.VolumeDevices != nil {
			for _, device := range container.VolumeDevices {
				volumeDevicesMap[device.Name] = true
			}
		}
	}

	return volumeMountsMap, volumeDevicesMap
}

func getPVVolumeGidAnnotationValue(pv *v1.PersistentVolume) string {
	if volumeGid, ok := pv.Annotations[util.VolumeGidAnnotationKey]; ok {
		return volumeGid
	}

	return ""
}
