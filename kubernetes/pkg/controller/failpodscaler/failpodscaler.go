/*
Copyright 2018 The Kubernetes Authors.

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

package failpodscaler

import (
	"fmt"
	"strconv"
	//	"math"
	"time"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/errors"
	//	autoscalingv2 "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/api/core/v1"
	//	apiequality "k8s.io/apimachinery/pkg/api/equality"
	//	"k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	//	"k8s.io/apimachinery/pkg/api/resource"
	//	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	scaleclient "k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"
)

const (
	oldReplicaSaveAnn = "io.enndata.cn/fail-pod-scaler-replica-save"
	failIgnoreAnn     = "io.enndata.cn/fail-pod-scaler-ignore"
)

// FailPodScaleController is responsible for scale down
// the scalers whose pods are crasch all the time
type FailPodScalerController struct {
	scaleNamespacer scaleclient.ScalesGetter
	mapper          apimeta.RESTMapper

	eventRecorder record.EventRecorder

	// podListerSynced returns true if the pod store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	podListerSynced cache.InformerSynced
	// A store of pods, populated by the shared informer passed to NewFailPodScalerController
	podLister corelisters.PodLister

	rsLister       appslisters.ReplicaSetLister
	rsListerSynced cache.InformerSynced

	queue      workqueue.RateLimitingInterface
	kubeClient clientset.Interface

	overFailLimit int32
}

// NewFailPodScaleController creates a new FailPodScaleController.
func NewFailPodScalerController(
	evtNamespacer v1core.EventsGetter,
	scaleNamespacer scaleclient.ScalesGetter,
	mapper apimeta.RESTMapper,
	podInformer coreinformers.PodInformer,
	rsInformer appsinformers.ReplicaSetInformer,
	kubeClient clientset.Interface,
	overFailLimit int32,
) *FailPodScalerController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartLogging(glog.Infof)
	broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: evtNamespacer.Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "fail-pod-scaler"})

	fpsController := &FailPodScalerController{
		eventRecorder:   recorder,
		scaleNamespacer: scaleNamespacer,
		queue:           workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "failpodscaler"),
		mapper:          mapper,
		overFailLimit:   overFailLimit,
		kubeClient:      kubeClient,
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    fpsController.enqueuePod,
		UpdateFunc: fpsController.updatePod,
		DeleteFunc: fpsController.deletePod,
	})
	fpsController.podLister = podInformer.Lister()
	fpsController.podListerSynced = podInformer.Informer().HasSynced

	fpsController.rsLister = rsInformer.Lister()
	fpsController.rsListerSynced = rsInformer.Informer().HasSynced

	return fpsController
}

// Run begins watching and syncing.
func (fps *FailPodScalerController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer fps.queue.ShutDown()

	glog.Infof("Starting FPS controller")
	defer glog.Infof("Shutting down FPS controller")

	if !controller.WaitForCacheSync("FPS", stopCh, fps.podListerSynced, fps.rsListerSynced) {
		return
	}

	// start a single worker (we may wish to start more in the future)
	go wait.Until(fps.worker, time.Second, stopCh)

	<-stopCh
}

func (fps *FailPodScalerController) enqueuePod(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}
	fps.queue.Add(key)
}

// obj could be an *v1.Pod, or a DeletionFinalStateUnknown marker item.
func (fps *FailPodScalerController) updatePod(old, cur interface{}) {
	fps.enqueuePod(cur)
}

func (fps *FailPodScalerController) deletePod(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}

	fps.queue.Forget(key)
}

func (fps *FailPodScalerController) worker() {
	for fps.processNextWorkItem() {
	}
	glog.Infof("fail pod scaler controller worker shutting down")
}

func (fps *FailPodScalerController) processNextWorkItem() bool {
	key, quit := fps.queue.Get()
	if quit {
		return false
	}
	defer fps.queue.Done(key)

	err := fps.syncPod(key.(string))
	if err == nil {
		fps.queue.Forget(key)
		return true
	}

	fps.queue.AddRateLimited(key)
	utilruntime.HandleError(err)
	return true
}

func (fps *FailPodScalerController) syncPod(key string) error {
	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing pod %q (%v)", key, time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pod, err := fps.podLister.Pods(namespace).Get(name)
	if errors.IsNotFound(err) {
		glog.V(4).Infof("pod %v has been deleted", key)
		return nil
	}
	if err != nil {
		return err
	}

	scale, targetGR, errScale := fps.getPodBelongScaler(pod)

	if errScale != nil {
		return fmt.Errorf("getPodBelongScaler %s err:%v", key, errScale)
	}

	if scale == nil { // pod has no controller
		return nil
	}
	if scale.Spec.Replicas == 0 {
		glog.V(4).Infof("pod %s:%s belong scale %s cur replica is 0", pod.Namespace, pod.Name, scale.Name)
		return nil
	}
	runningPod, lowFailPod, overFailPod, otherPod, errStatus := fps.getScalePodsStatus(scale, fps.overFailLimit)
	if errStatus != nil {
		return fmt.Errorf("getScalePodsStatus err:%v", errStatus)
	}

	if scale.Spec.Replicas > runningPod+lowFailPod+overFailPod+otherPod {
		glog.V(4).Infof("scale replica %s but created pod %d, return", scale.Spec.Replicas, runningPod+lowFailPod+overFailPod+otherPod)
		return nil
	}
	if overFailPod >= scale.Spec.Replicas && runningPod == 0 && lowFailPod == 0 && otherPod == 0 {
		if err := fps.recordAndEventScaleDown(scale.Namespace, scale.Name, targetGR, scale.Spec.Replicas); err != nil {
			return fmt.Errorf("recordAndEventScaleDown err:%v", err)
		}
		scale.Spec.Replicas = 0
		_, errUpdate := fps.scaleNamespacer.Scales(namespace).Update(targetGR, scale)
		if errUpdate != nil {
			return fmt.Errorf("failed to rescale %s's scale %s: %v", key, scale.Name, errUpdate)
		} else {
			glog.V(2).Infof("scale %s:%s scale to 0 success\n", scale.Namespace, scale.Name)
		}
	}
	return nil
}

func getPodFailCount(pod *v1.Pod) (failCount int32, err error) {
	if pod.Annotations != nil && pod.Annotations[failIgnoreAnn] == "true" {
		return -1, nil
	}
	if pod.Status.Phase != v1.PodRunning { // pod fail complete pending we do not care, so take it as other pod
		return -1, nil
	}
	if pod.Status.ContainerStatuses == nil || len(pod.Status.ContainerStatuses) == 0 {
		return -1, nil
	}
	if len(pod.Spec.Containers) != len(pod.Status.ContainerStatuses) { // some container has no status
		return -1, nil
	}
	allContainerRunning := true
	var totalFailCount int32
	for i := range pod.Status.ContainerStatuses {
		totalFailCount += pod.Status.ContainerStatuses[i].RestartCount
		if pod.Status.ContainerStatuses[i].LastTerminationState.Terminated != nil {
			startTime := pod.Status.ContainerStatuses[i].LastTerminationState.Terminated.StartedAt
			stopTime := pod.Status.ContainerStatuses[i].LastTerminationState.Terminated.FinishedAt
			if stopTime.Sub(startTime.Time) > time.Minute { // we only care crash container which crach 1 min later
				continue
			}
		}
		if pod.Status.ContainerStatuses[i].State.Running == nil && pod.Status.ContainerStatuses[i].LastTerminationState.Terminated != nil {
			allContainerRunning = false
		}
	}
	if allContainerRunning == true { // if pod's all containers are running, we not care the failcount before
		return 0, nil
	}
	return totalFailCount, nil
}

func (fps *FailPodScalerController) getScalePodsStatus(scale *autoscalingv1.Scale, failCountLimit int32) (runningPod, lowFailPod, overFailPod, otherPod int32, errRet error) {
	if scale.Status.Selector == "" {
		return 0, 0, 0, 0, fmt.Errorf("getScalePodsStatus scale %s Status.Selector is empty", scale.Name)
	}

	selector, errParse := labels.Parse(scale.Status.Selector)
	if errParse != nil {
		return 0, 0, 0, 0, fmt.Errorf("getScalePodsStatus scale %s Status.Selector parse err:%v", scale.Name, errParse)
	}
	pods, errList := fps.podLister.List(selector)
	if errList != nil {
		return 0, 0, 0, 0, fmt.Errorf("getScalePodsStatus scale %s list pod err:%v", scale.Name, errList)
	}
	for _, pod := range pods {
		c, err := getPodFailCount(pod)
		if err != nil {
			return runningPod, lowFailPod, overFailPod, otherPod, fmt.Errorf("get pod %s:%s failcount err:%v", pod.Namespace, pod.Name, err)
		}
		switch {
		case c < 0: // pod is creating or pending
			otherPod++
		case c == 0:
			runningPod++
		case c > failCountLimit:
			overFailPod++
		default:
			lowFailPod++
		}
	}
	return
}

func (fps *FailPodScalerController) getGroupKindByOwnerReferences(or metav1.OwnerReference) (*schema.GroupKind, string, error) {
	targetGV, err := schema.ParseGroupVersion(or.APIVersion)
	if err != nil {
		return nil, "", fmt.Errorf("invalid API version reference: %v", err)
	}
	targetGK := &schema.GroupKind{
		Group: targetGV.Group,
		Kind:  or.Kind,
	}
	return targetGK, or.Name, nil
}

func (fps *FailPodScalerController) recordAndEventScaleDown(namespace, name string, gr schema.GroupResource, fromReplica int32) error {
	var obj runtime.Object
	switch gr.Resource {
	case "replicasets":
		rs, err := fps.kubeClient.Extensions().ReplicaSets(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get rs %s:%s err:%v", namespace, name, err)
		}
		if rs.Annotations == nil {
			rs.Annotations = make(map[string]string)
		}
		rs.Annotations[oldReplicaSaveAnn] = strconv.Itoa(int(fromReplica))
		obj, err = fps.kubeClient.Extensions().ReplicaSets(namespace).Update(rs)
		if err != nil {
			fmt.Errorf("update rs %s:%s err:%v", namespace, name, err)
		}
	case "deployments":
		deploy, err := fps.kubeClient.Extensions().Deployments(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get deploy %s:%s err:%v", namespace, name, err)
		}
		if deploy.Annotations == nil {
			deploy.Annotations = make(map[string]string)
		}
		deploy.Annotations[oldReplicaSaveAnn] = strconv.Itoa(int(fromReplica))
		obj, err = fps.kubeClient.Extensions().Deployments(namespace).Update(deploy)
		if err != nil {
			fmt.Errorf("update deploy %s:%s err:%v", namespace, name, err)
		}
	case "statefulsets":
		ss, err := fps.kubeClient.Apps().StatefulSets(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get StatefulSets %s:%s err:%v", namespace, name, err)
		}
		if ss.Annotations == nil {
			ss.Annotations = make(map[string]string)
		}
		ss.Annotations[oldReplicaSaveAnn] = strconv.Itoa(int(fromReplica))
		obj, err = fps.kubeClient.Apps().StatefulSets(namespace).Update(ss)
		if err != nil {
			fmt.Errorf("update StatefulSets %s:%s err:%v", namespace, name, err)
		}
	case "replicationcontrollers":
		rc, err := fps.kubeClient.Core().ReplicationControllers(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get rc %s:%s err:%v", namespace, name, err)
		}
		if rc.Annotations == nil {
			rc.Annotations = make(map[string]string)
		}
		rc.Annotations[oldReplicaSaveAnn] = strconv.Itoa(int(fromReplica))
		obj, err = fps.kubeClient.Core().ReplicationControllers(namespace).Update(rc)
		if err != nil {
			fmt.Errorf("update rc %s:%s err:%v", namespace, name, err)
		}
	default:
		return fmt.Errorf("unknow resource %s", gr.Resource)
	}
	fps.eventRecorder.Eventf(obj, v1.EventTypeNormal, "FailPodScaleDown", "Start scale down %d pod because crash count > %d", fromReplica, fps.overFailLimit)
	return nil
}

// is rs is controller by deploy return deployment groupkind
func (fps *FailPodScalerController) getRSGroupKindByOwnerReferences(namespace string, or metav1.OwnerReference) (*schema.GroupKind, string, error) {
	if or.Kind == "ReplicaSet" {
		rs, err := fps.rsLister.ReplicaSets(namespace).Get(or.Name)
		if err != nil {
			return nil, "", fmt.Errorf("getRSGroupKindByOwnerReferences get rs %s:%s err:%v", namespace, or.Name, err)
		}
		if rs.UID != or.UID {
			return nil, "", fmt.Errorf("getRSGroupKindByOwnerReferences owner uid:%s != rs %s:%s uid:%v", or.UID, namespace, or.Name, rs.UID)
		}
		return fps.getControllerGroupKindFromOwnerReferences(namespace, rs.OwnerReferences)
	} else {
		return nil, "", fmt.Errorf("OwnerReference %v is not ReplicaSet", or)
	}
}

func (fps *FailPodScalerController) isScaleController(kind string) bool {
	kindScalerMap := map[string]bool{
		"ReplicaSet":            true,
		"Deployment":            true,
		"StatefulSet":           true,
		"ReplicationController": true,
	}
	_, ok := kindScalerMap[kind]
	return ok
}

func (fps *FailPodScalerController) getControllerGroupKindFromOwnerReferences(namespace string, ors []metav1.OwnerReference) (*schema.GroupKind, string, error) {
	if ors != nil && len(ors) > 0 {
		for i := range ors {
			if ors[i].Controller != nil && *ors[i].Controller == true {
				if fps.isScaleController(ors[i].Kind) == false {
					return nil, "", nil
				}
				if ors[i].Kind != "ReplicaSet" {
					return fps.getGroupKindByOwnerReferences(ors[i])
				} else {
					return fps.getRSGroupKindByOwnerReferences(namespace, ors[i])
				}
			}
		}
	}
	return nil, "", nil
}

func (fps *FailPodScalerController) getPodBelongScaler(pod *v1.Pod) (*autoscalingv1.Scale, schema.GroupResource, error) {
	controllerGK, controllerName, errGet := fps.getControllerGroupKindFromOwnerReferences(pod.Namespace, pod.OwnerReferences)
	if errGet != nil {
		return nil, schema.GroupResource{}, fmt.Errorf("getControllerGroupKindFromOwnerReferences err:%v", errGet)
	}
	if controllerGK == nil {
		return nil, schema.GroupResource{}, nil
	}

	mappings, errMapping := fps.mapper.RESTMappings(*controllerGK)
	if errMapping != nil {
		return nil, schema.GroupResource{}, fmt.Errorf("unable to determine resource for scale target reference: %v", errMapping)
	}

	var firstErr error
	for i, mapping := range mappings {
		targetGR := mapping.Resource.GroupResource()
		scale, err := fps.scaleNamespacer.Scales(pod.Namespace).Get(targetGR, controllerName)
		if err == nil {
			return scale, targetGR, nil
		}

		// if this is the first error, remember it,
		// then go on and try other mappings until we find a good one
		if i == 0 {
			firstErr = fmt.Errorf("get scale %s:%s ,%v err:%v", pod.Namespace, controllerName, targetGR, err)
		}
	}

	// make sure we handle an empty set of mappings
	if firstErr == nil {
		firstErr = fmt.Errorf("unrecognized resource")
	}

	return nil, schema.GroupResource{}, firstErr
}
