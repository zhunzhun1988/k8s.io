package rescheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	ca_simulator "k8s.io/kubernetes/cmd/kube-rescheduler/simulator"
	"k8s.io/kubernetes/pkg/rescheduler/debug"
	"k8s.io/kubernetes/pkg/rescheduler/utils"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

const (
	RCRescheduing    = "io.enndata.rescheduler/rescheduling"
	NSCanRescheduler = "io.enndata.rescheduler/enableinsulate"
)

type PodsByRescheduleable struct {
	pods    []*v1.Pod
	nsCount map[string]int
}

func maxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func NewPodsByRescheduleable(pods []*v1.Pod) *PodsByRescheduleable {
	ret := &PodsByRescheduleable{
		pods:    pods,
		nsCount: make(map[string]int),
	}
	for _, pod := range pods {
		if c, ok := ret.nsCount[pod.Namespace]; ok {
			ret.nsCount[pod.Namespace] = c + 1
		} else {
			ret.nsCount[pod.Namespace] = 1
		}
	}
	return ret
}
func (p PodsByRescheduleable) Len() int      { return len(p.pods) }
func (p PodsByRescheduleable) Swap(i, j int) { p.pods[i], p.pods[j] = p.pods[j], p.pods[i] }
func (p PodsByRescheduleable) Less(i, j int) bool {
	if p.pods[i].Namespace == p.pods[j].Namespace {
		if p.pods[i].CreationTimestamp.Equal(&p.pods[j].CreationTimestamp) {
			return p.pods[i].Name < p.pods[j].Name
		}
		return p.pods[i].CreationTimestamp.Before(&p.pods[j].CreationTimestamp)
	} else if p.nsCount[p.pods[i].Namespace] != p.nsCount[p.pods[j].Namespace] {
		return p.nsCount[p.pods[i].Namespace] < p.nsCount[p.pods[j].Namespace]
	} else {
		return p.pods[i].Namespace < p.pods[j].Namespace
	}
}

type NodeScore struct {
	node  *v1.Node
	score int
}
type NodeScoreArray []NodeScore

func (nsa NodeScoreArray) Len() int      { return len(nsa) }
func (nsa NodeScoreArray) Swap(i, j int) { nsa[i], nsa[j] = nsa[j], nsa[i] }
func (nsa NodeScoreArray) Less(i, j int) bool {
	if nsa[i].score == nsa[j].score {
		return nsa[i].node.Name < nsa[j].node.Name
	}
	return nsa[i].score < nsa[j].score
}

type NodeInfoCach struct {
	pods         []*v1.Pod
	node         *v1.Node
	nsCount      map[string]int
	numNamespace int
}

type NodesInfoCach struct {
	nodecach    map[string]*NodeInfoCach
	maxpodsize  int
	allPodsSize int
	mutex       sync.Mutex
}

func NewNodesInfoCach() *NodesInfoCach {
	//ret := NodesInfoCach(make(map[string]*NodeInfoCach))
	return &NodesInfoCach{
		nodecach:    make(map[string]*NodeInfoCach),
		mutex:       sync.Mutex{},
		maxpodsize:  0,
		allPodsSize: 0,
	}
}

func (nic *NodesInfoCach) GetNodePods(node *v1.Node) []*v1.Pod {
	nic.mutex.Lock()
	defer nic.mutex.Unlock()
	n, find := nic.nodecach[node.Name]
	if find {
		return n.pods
	}
	return nil
}
func (nic *NodesInfoCach) DeletePod(node *v1.Node, pod *v1.Pod) {
	nic.mutex.Lock()
	defer nic.mutex.Unlock()
	n, find := nic.nodecach[node.Name]
	if find == true {
		var find bool = false
		for i, p := range n.pods {
			if pod.Name == p.Name {
				tmp := n.pods[:i]
				tmp = append(tmp, n.pods[i+1:]...)
				n.pods = tmp
				find = true
				break
			}
		}
		if find == false {
			return
		}
		nic.allPodsSize -= 1
		if nic.maxpodsize == len(n.pods)+1 { // should recalculate the maxpodsize
			nic.maxpodsize = nic.getNodeMaxPodSize()
		}
		if count, ok := n.nsCount[pod.Namespace]; ok {
			if count > 0 {
				n.nsCount[pod.Namespace] = count - 1
			} else {
				n.nsCount[pod.Namespace] = 0
			}
		}
		if n.nsCount[pod.Namespace] == 0 {
			n.numNamespace -= 1
		}
	}
}
func (nic *NodesInfoCach) getNodeMaxPodSize() int {
	var max int = 0
	for _, nc := range nic.nodecach {
		l := len(nc.pods)
		if l > max {
			max = l
		}
	}
	return max
}
func (nic *NodesInfoCach) AddNode(node *v1.Node) {
	nic.mutex.Lock()
	defer nic.mutex.Unlock()
	_, find := nic.nodecach[node.Name]
	if find == false {
		n := &NodeInfoCach{
			pods:         make([]*v1.Pod, 0),
			nsCount:      make(map[string]int),
			node:         node,
			numNamespace: 0,
		}
		nic.nodecach[node.Name] = n
	}
}
func (nic *NodesInfoCach) AddPod(node *v1.Node, pod *v1.Pod) {
	nic.mutex.Lock()
	defer nic.mutex.Unlock()
	n, find := nic.nodecach[node.Name]
	if find == false {
		n = &NodeInfoCach{
			pods:         make([]*v1.Pod, 0),
			nsCount:      make(map[string]int),
			node:         node,
			numNamespace: 0,
		}
		nic.nodecach[node.Name] = n
	}
	n.pods = append(n.pods, pod)
	nic.allPodsSize += 1
	if nic.maxpodsize < len(n.pods) {
		nic.maxpodsize = len(n.pods)
	}
	if count, ok := n.nsCount[pod.Namespace]; ok {
		n.nsCount[pod.Namespace] = count + 1
	} else {
		n.nsCount[pod.Namespace] = 1
	}
	if n.nsCount[pod.Namespace] == 1 {
		n.numNamespace += 1
	}
}

type NSReschedule struct {
	apiClient          utils.ApiClientInterface
	rsDebug            *debug.RescheduleDebug
	nodeList           []*v1.Node
	predicateChecker   *ca_simulator.PredicateChecker
	podsBeingProcessed *utils.PodSet
	rcBeingProcessed   *utils.ObjectSet
	rsBeingProcessed   *utils.ObjectSet
	canRescheduleNS    map[string]bool
	reschedulertimeout time.Duration
}

func NewNSReschedule(apiClient utils.ApiClientInterface, predicateChecker *ca_simulator.PredicateChecker,
	podsBeingProcessed *utils.PodSet, enableDebug bool, debugPort int, reschedulertimeout time.Duration) *NSReschedule {
	var rsDebug *debug.RescheduleDebug = nil
	if enableDebug {
		rsDebug = debug.NewRescheduleDebug(apiClient, debugPort)
	}
	ret := &NSReschedule{
		apiClient:          apiClient,
		rsDebug:            rsDebug,
		predicateChecker:   predicateChecker,
		podsBeingProcessed: podsBeingProcessed,
		reschedulertimeout: reschedulertimeout,
		canRescheduleNS:    make(map[string]bool, 0),
		rcBeingProcessed: utils.NewObjectSet(func(o interface{}) string {
			if o == nil {
				return ""
			}
			rc, ok := o.(*v1.ReplicationController)
			if ok {
				if rc.Namespace != "" {
					return rc.Namespace + "/" + rc.Name
				}
				return rc.Name
			}
			return ""
		}),
		rsBeingProcessed: utils.NewObjectSet(func(o interface{}) string {
			if o == nil {
				return ""
			}
			rs, ok := o.(*extensions.ReplicaSet)
			if ok {
				if rs.Namespace != "" {
					return rs.Namespace + "/" + rs.Name
				}
				return rs.Name
			}
			return ""
		}),
	}
	if ret.rsDebug != nil {
		ret.rsDebug.Run()
	}
	return ret
}

func (nsRs *NSReschedule) CheckAndReschedule() {
	nsRs.initCanReschedulerNS()
	if nsRs.rsDebug != nil {
		nsRs.rsDebug.SetCanReschedulerNS(nsRs.canRescheduleNS)
		nsRs.rsDebug.BeginOneReschedule()
	}

	nsRs.checkAndReschedule()
	if nsRs.rsDebug != nil {
		nsRs.rsDebug.EndOneReschedule()
	}
}

func isNamespaceCanReschedule(ns *v1.Namespace) bool {
	if ns.Annotations == nil {
		return false
	} else {
		if v, find := ns.Annotations[NSCanRescheduler]; find {
			return v == "true"
		} else {
			return false
		}
	}
}
func (nsRs *NSReschedule) initCanReschedulerNS() {
	allNS := nsRs.apiClient.ListNamespace()
	nsRs.canRescheduleNS = make(map[string]bool, 0)
	for _, ns := range allNS {
		if isNamespaceCanReschedule(ns) == true {
			nsRs.canRescheduleNS[ns.Name] = true
		}
	}
}

func (nsRs *NSReschedule) checkAndReschedule() error {

	nodes := nsRs.apiClient.ListReadyNodes()

	rcs, _ := nsRs.apiClient.GetReplications(v1.NamespaceAll)
	rss, _ := nsRs.apiClient.GetReplicaSets(v1.NamespaceAll)

	infoCach := nsRs.getNodsInfoCach(nodes)
	//var flag bool = false
	for _, node := range nodes {
		nodePods := infoCach.GetNodePods(node)
		if nodePods == nil {
			glog.Infof("node %s get no pods", node.Name)
			continue
		}
		if nsRs.isPodsAllTheSameNS(nodePods) {
			//glog.Infof("node %s %d pods are all the same namespace", node.Name, len(nodePods))
			continue
		}
		//flag = true
		canRsPods := nsRs.fiterCanReschedulePods(nodePods)

		canRsPodsList := NewPodsByRescheduleable(canRsPods)
		sort.Sort(canRsPodsList)

		for _, pod := range canRsPodsList.pods {
			nodePods := infoCach.GetNodePods(node)
			if nodePods == nil || nsRs.isPodsAllTheSameNS(nodePods) {
				break
			}
			toNode, score := nsRs.getPodRescheduleNode(pod, node, infoCach, rcs, rss)
			if toNode != nil {
				glog.Infof("pod %s:%s reschdule to node %s score %d", pod.Namespace, pod.Name, toNode.Name, score)
				if ok, err := nsRs.reschedulePodToNode(pod, node, toNode, rcs, rss); ok {
					glog.Infof("pod %s:%s reschedule from node %s to node %s success", pod.Namespace, pod.Name, node.Name, toNode.Name)
					infoCach.AddPod(toNode, pod)
					infoCach.DeletePod(node, pod)
				} else {
					glog.Errorf("pod %s:%s reschedule from node %s to node %s fail %v", pod.Namespace, pod.Name, node.Name, toNode.Name, err)
				}
			}
		}
	}
	/*
		if flag == false && false {
			fmt.Printf("patrick debug cachnodesize = %d\n", len(infoCach.nodecach))
			for _, node := range nodes {
				nodePods := infoCach.GetNodePods(node)
				if nodePods == nil {
					glog.Infof("node %s get no pods", node.Name)
					continue
				}
				canRsPods := nsRs.fiterCanReschedulePods(nodePods)

				for _, pod := range canRsPods {
					var maxScore int = 0
					var toNode *v1.Node = nil
					for _, n := range nodes {
						if n.Name == node.Name {
							continue
						}
						score, ok, err := nsRs.getPodRescheduleScore(infoCach, pod, node, n, rcs, rss)
						if err != nil {
							glog.Infof("pod %s can't reschedule from %s to %s, %v", pod.Name, node.Name, n.Name, err)
						}
						if ok && score > maxScore {
							maxScore = score
							toNode = n
						}
					}

					if toNode != nil {
						glog.Infof("pod %s:%s reschdule to node %s score %d", pod.Namespace, pod.Name, toNode.Name, maxScore)
						if ok, err := nsRs.reschedulePodToNode(pod, node, toNode, rcs, rss); ok {
							glog.Infof("pod %s:%s reschedule from node %s to node %s success", pod.Namespace, pod.Name, node.Name, toNode.Name)
							infoCach.AddPod(toNode, pod)
							infoCach.DeletePod(node, pod)
						} else {
							glog.Errorf("pod %s:%s reschedule from node %s to node %s fail %v", pod.Namespace, pod.Name, node.Name, toNode.Name, err)
						}
					}
				}
			}
		}*/
	nsRs.relaseAllReschedulingTag(rcs, rss)
	return nil
}
func (nsRs *NSReschedule) relaseAllReschedulingTag(rcs []*v1.ReplicationController, rss []*extensions.ReplicaSet) {
	for _, rc := range rcs {
		if nsRs.rcBeingProcessed.Has(rc) == false && rc.Annotations[RCRescheduing] == "true" {
			nsRs.signRCRescheduling(rc, "false")
		}
	}
	for _, rs := range rss {
		if nsRs.rsBeingProcessed.Has(rs) == false && rs.Annotations[RCRescheduing] == "true" {
			nsRs.signRSRescheduling(rs, "false")
		}
	}
}
func (nsRs *NSReschedule) copyPod(pod *v1.Pod) (*v1.Pod, error) {
	copied := pod.DeepCopy()

	return copied, nil
}

func rspodId(pod *v1.Pod) string {
	return fmt.Sprintf("%s_%sreschedule", pod.Namespace, pod.GenerateName)
}

func (nsRs *NSReschedule) getPodForScheduleToNode(pod *v1.Pod, tonode *v1.Node) (*v1.Pod, error) {
	podNew, errCopy := nsRs.copyPod(pod)
	if errCopy != nil {
		return nil, errCopy
	}
	//tolerations := podNew.Spec.Tolerations //kube_api.GetTolerationsFromPodAnnotations(podNew.Annotations)
	if podNew.Spec.Tolerations == nil {
		podNew.Spec.Tolerations = []v1.Toleration{}
	}
	toleration := v1.Toleration{
		Key:      utils.CriticalPodAnnotation,
		Operator: v1.TolerationOpEqual,
		Value:    rspodId(podNew),
		Effect:   v1.TaintEffectNoSchedule,
	}
	podNew.Spec.Tolerations = append(podNew.Spec.Tolerations, toleration)

	//podNew.Annotations[kube_api.TolerationsAnnotationKey] = string(tolerationsJson)

	//podNew.Status.Phase = v1.PodUnknown
	podNew.ResourceVersion = ""
	podNew.Spec.NodeName = tonode.Name

	if nsRs.isPodCreateByRcOrRcs(pod) {
		podNew.Name = ""
	} else {
		podNew.Name = pod.Name
	}

	return podNew, nil
}

func (nsRs *NSReschedule) reschedulePodToNode(pod *v1.Pod, fromnode, tonode *v1.Node,
	rcs []*v1.ReplicationController, rss []*extensions.ReplicaSet) (bool, error) {

	podNew, errGetPod := nsRs.getPodForScheduleToNode(pod, tonode)
	if errGetPod != nil {
		return false, errGetPod
	}

	toNodeNew, errGetNode := nsRs.apiClient.GetNode(tonode.Name)
	if errGetNode != nil {
		return false, fmt.Errorf("error get node %s", tonode.Name)
	}

	errTaint := utils.AddTaint(nsRs.apiClient.UpdateNode, toNodeNew, rspodId(podNew))
	if errTaint != nil {
		return false, fmt.Errorf("Error while adding taint: %v", errTaint)
	}

	if ok, checkError := nsRs.isPodCanBeRescheduledToNode(podNew, tonode); !ok {
		return false, checkError
	}

	podRcs := nsRs.getPodReplicationController(pod, rcs)
	if len(podRcs) > 0 {
		err := nsRs.signRCRescheduling(podRcs[0], "true")
		if err != nil {
			return false, fmt.Errorf("Pod rc %v signed error %v", podRcs[0], err)
		}
	}
	podRss := nsRs.getPodReplicaSets(pod, rss)
	if len(podRss) > 0 {
		err := nsRs.signRSRescheduling(podRss[0], "true")
		if err != nil {
			return false, fmt.Errorf("Pod rs %v signed error %v", podRss[0], err)
		}
	}

	toNodePods, errGet := nsRs.getNodePods(tonode.Name)
	if errGet != nil {
		return false, errGet
	}
	podMap := make(map[string]bool)
	for _, p := range toNodePods {
		podMap[utils.PodId(p)] = true
	}

	delErr := nsRs.apiClient.DeletePod(pod.Namespace, pod.Name, 0)
	if delErr != nil {
		return false, fmt.Errorf("Failed to delete pod %s: %v", utils.PodId(pod), delErr)
	}

	_, errCreate := nsRs.apiClient.CreatePod(podNew)
	if errCreate != nil {
		if nsRs.rsDebug != nil {
			nsRs.rsDebug.PodRescheduled(pod.Namespace, pod.Name, "", fromnode.Name, tonode.Name, errCreate.Error(), false)
		}
		return false, fmt.Errorf("Failed to create pod %s   %v", utils.PodId(podNew), errCreate)
	}

	nsRs.podsBeingProcessed.AddKey(rspodId(podNew))

	var rc *v1.ReplicationController = nil
	var rs *extensions.ReplicaSet = nil
	if len(podRcs) > 0 {
		rc = podRcs[0]
		nsRs.rcBeingProcessed.Add(rc)
	}
	if len(podRss) > 0 {

		rs = podRss[0]
		nsRs.rsBeingProcessed.Add(rs)
	}
	go nsRs.waitForReScheduled(pod, podNew, fromnode.Name, tonode.Name, podMap, rc, rs)

	return true, nil
}

func (nsRs *NSReschedule) waitForReScheduled(podOld *v1.Pod, podNew *v1.Pod, fromNode, toNode string, nodePodMap map[string]bool,
	rc *v1.ReplicationController, rs *extensions.ReplicaSet) {
	glog.Infof("Waiting for pod %s to be scheduled", utils.PodId(podNew))
	toPodName := podNew.Name
	err := wait.Poll(time.Second, nsRs.reschedulertimeout, func() (bool, error) {
		podsOnNode, err := nsRs.getNodePods(toNode)
		if err != nil {
			return false, nil
		}
		for _, p := range podsOnNode {
			_, find := nodePodMap[utils.PodId(p)]
			if !find {
				toPodName = p.Name
				return true, nil
			}
		}
		return false, nil
	})

	if err != nil {
		glog.Warningf("Timeout while waiting for pod %s to be scheduled after %v.", utils.PodId(podNew), nsRs.reschedulertimeout)
		if nsRs.rsDebug != nil {
			nsRs.rsDebug.PodRescheduled(podNew.Namespace, podNew.Name, "", fromNode, toNode, err.Error(), false)
		}
	} else {
		//glog.Infof("Pod %v was successfully scheduled.", utils.PodId(podNew))
		//nsRs.rsDebug.PodRescheduled(podNew.Namespace, podOld.Name, toPodName, fromNode, toNode, "OK", true)
		//delErr := nsRs.client.Pods(podOld.Namespace).Delete(podOld.Name, v1.NewDeleteOptions(0)) // should create podnew success then delete the old pod

		//if delErr != nil {
		//glog.Warningf("Failed to delete pod %s: %v", utils.PodId(podOld), delErr)
		//} else {
		errWait := wait.Poll(time.Second, nsRs.reschedulertimeout, func() (bool, error) {
			podsOnNode, err := nsRs.getNodePods(fromNode)
			if err != nil {
				return false, nil
			}
			for _, p := range podsOnNode {
				if p.Name == podOld.Name {
					return false, nil
				}
			}
			return true, nil
		})
		if errWait == nil {
			glog.Infof("Pod %v was successfully scheduled.", utils.PodId(podNew))
			if nsRs.rsDebug != nil {
				nsRs.rsDebug.PodRescheduled(podNew.Namespace, podOld.Name, toPodName, fromNode, toNode, "OK", true)
			}
		}
		//}
	}
	if rc != nil {
		nsRs.signRCRescheduling(rc, "false")
		nsRs.rcBeingProcessed.Remove(rc)
	}
	if rs != nil {
		nsRs.signRSRescheduling(rs, "false")
		nsRs.rsBeingProcessed.Remove(rs)
	}
	nsRs.podsBeingProcessed.RemoveByKey(rspodId(podNew))
}

func (nsRs *NSReschedule) getNameSpaceScore(pod *v1.Pod, node *v1.Node, cach *NodesInfoCach) int {
	var score int = 0
	if len(cach.nodecach[node.Name].pods) == 0 {
		return 100
	}
	var maxCount int = 0
	for _, c := range cach.nodecach {
		if c.nsCount[pod.Namespace] != len(c.pods) {
			continue
		}
		if maxCount < c.nsCount[pod.Namespace] {
			maxCount = c.nsCount[pod.Namespace]
		}
	}

	if nodeCach, ok := cach.nodecach[node.Name]; ok &&
		cach.nodecach[node.Name].nsCount[pod.Namespace] == len(cach.nodecach[node.Name].pods) {
		t := nodeCach.nsCount[pod.Namespace]
		score = int((float64(maxCount-t) / float64(maxCount)) * 100)
		if score == 0 {
			score = 1
		}
	}

	return score
}
func (nsRs *NSReschedule) getScorePodToNode(pod *v1.Pod, node *v1.Node, cach *NodesInfoCach) int {
	var score int = 0
	score += nsRs.getNameSpaceScore(pod, node, cach)
	return score
}

//
func (nsRs *NSReschedule) getPodRescheduleNode(pod *v1.Pod, fromNode *v1.Node, cach *NodesInfoCach,
	rcs []*v1.ReplicationController, rss []*extensions.ReplicaSet) (*v1.Node, int) {
	numNode := len(cach.nodecach)
	if numNode <= 1 {
		return nil, 0
	}

	podRcs := nsRs.getPodReplicationController(pod, rcs)
	podRss := nsRs.getPodReplicaSets(pod, rss)

	nodeScoreArray := make([]NodeScore, 0, numNode)
	for _, c := range cach.nodecach {
		if c.node.Name == fromNode.Name {
			continue
		}
		if ok, err := nsRs.isPodCanBeRescheduledToNode(pod, c.node); ok {
			theRcOrRsPodSize := nsRs.getPodSizeOfRcsAndRss(c.pods, podRcs, podRss)
			if theRcOrRsPodSize == 0 {
				nodeScoreArray = append(nodeScoreArray, NodeScore{score: nsRs.getScorePodToNode(pod, c.node, cach), node: c.node})
			} else {
				glog.Infof("pod %s:%s can not be reschedule to node %s because node has %d pod belong to the same rc or rcs",
					pod.Namespace, pod.Name, c.node.Name, theRcOrRsPodSize)
			}
		} else {
			glog.Infof("pod %s:%s can not be reschedule to node %s , %v", pod.Namespace, pod.Name, c.node.Name, err)
		}
	}
	sort.Sort(sort.Reverse(NodeScoreArray(nodeScoreArray)))

	for _, ns := range nodeScoreArray {
		if ns.node.Name != fromNode.Name && ns.score > 0 {
			return ns.node, ns.score
		}
	}
	return nil, 0
}
func (nsRs *NSReschedule) getNodsInfoCach(nodes []*v1.Node) *NodesInfoCach {
	ret := NewNodesInfoCach()
	for _, node := range nodes {
		nodePods, err := nsRs.getNodePods(node.Name)
		if err != nil {
			glog.Errorf("Failed to getPods of node %s, %v", node.Name, err)
			continue
		}
		ret.AddNode(node)
		for _, pod := range nodePods {
			ret.AddPod(node, pod)
		}
	}
	return ret
}
func (nsRs *NSReschedule) isPodCanReschedule(pod *v1.Pod) bool {
	creatorRef, err := utils.CreatorRefKind(pod)
	if err != nil {
		glog.Errorf("pod %s:%s CreatorRefKind error %v", pod.Namespace, pod.Name, err)
		return true
	}
	if utils.IsMirrorPod(pod) || creatorRef == "DaemonSet" || creatorRef == "Job" || utils.IsCriticalPod(pod) {
		return false
	}
	return true
}

func (nsRs *NSReschedule) isPodCreateByRcOrRcs(pod *v1.Pod) bool {
	creatorRef, err := utils.CreatorRefKind(pod)
	if err != nil {
		glog.Errorf("pod %s:%s CreatorRefKind error %v", pod.Namespace, pod.Name, err)
		return false
	}
	if creatorRef == "ReplicationController" || creatorRef == "ReplicaSet" {
		return true
	}
	return false
}
func (nsRs *NSReschedule) isPodCanBeRescheduledToNode(pod *v1.Pod, node *v1.Node) (bool, error) {
	podCopy, errCopy := nsRs.copyPod(pod)
	if errCopy != nil {
		return false, errCopy
	}
	podCopy.Spec.NodeName = ""
	nodePods, errGetPods := nsRs.getNodeAllPods(node.Name)
	if errGetPods != nil {
		return false, errGetPods
	}
	nodeInfo := schedulercache.NewNodeInfo(nodePods...)
	nodeInfo.SetNode(node)

	if err := nsRs.predicateChecker.CheckPredicates(podCopy, nodeInfo); err == nil {
		return true, nil
	} else {
		return false, err
	}
}
func (nsRs *NSReschedule) fiterCanReschedulePods(pods []*v1.Pod) []*v1.Pod {
	ret := make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		if nsRs.isPodCanReschedule(pod) {
			ret = append(ret, pod)
		}
	}
	return ret
}
func (nsRs *NSReschedule) isPodsAllTheSameNS(pods []*v1.Pod) bool {
	if pods == nil {
		return true
	}
	lastNs := ""
	for _, pod := range pods {
		if nsRs.isPodCanReschedule(pod) == false {
			continue
		}
		if lastNs != "" && pod.Namespace != lastNs {
			return false
		}
		lastNs = pod.Namespace
	}
	return true
}

// get pods of node not include namespace is kube-system
func (nsRs *NSReschedule) getNodePods(nodeName string) ([]*v1.Pod, error) {
	allPods, err := nsRs.getNodeAllPods(nodeName)
	if err != nil {
		return nil, err
	}
	ret := make([]*v1.Pod, 0, len(allPods))
	for _, p := range allPods {
		if nsRs.canRescheduleNS[p.Namespace] == true {
			ret = append(ret, p)
		}
	}
	return ret, nil
}
func (nsRs *NSReschedule) getNodeAllPods(nodeName string) ([]*v1.Pod, error) {
	podsOnNode, err := nsRs.apiClient.GetPods(v1.NamespaceAll, nodeName)
	return podsOnNode, err
}

/*func (nsRs *NSReschedule) getReplicationControllers() []*kube_api.ReplicationController {
	rcs := make([]*kube_api.ReplicationController, 0)

	rcList, err := nsRs.client.ReplicationControllers(kube_api.NamespaceAll).List(kube_api.ListOptions{})
	if err != nil {
		return rcs
	}
	for i, _ := range rcList.Items {
		rcs = append(rcs, &rcList.Items[i])
	}
	return rcs
}

func (nsRs *NSReschedule) getReplicaSets() []*extensions.ReplicaSet {
	rss := make([]*extensions.ReplicaSet, 0)

	rsList, err := nsRs.client.ReplicaSets(kube_api.NamespaceAll).List(kube_api.ListOptions{})
	if err != nil {
		return rss
	}
	for i, _ := range rsList.Items {
		rss = append(rss, &rsList.Items[i])
	}
	return rss
}*/
func isPodBelongToRc(pod *v1.Pod, rc *v1.ReplicationController) bool {
	labelSet := labels.Set(rc.Spec.Selector)
	selector := labels.Set(rc.Spec.Selector).AsSelector()

	if labelSet.AsSelector().Empty() || !selector.Matches(labels.Set(pod.Labels)) {
		return false
	}
	return true
}
func isPodBelongToRs(pod *v1.Pod, rs *extensions.ReplicaSet) bool {
	selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
	if err != nil {

		return false
	}

	// If a ReplicaSet with a nil or empty selector creeps in, it should match nothing, not everything.
	if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
		return false
	}
	return true
}
func (nsRs *NSReschedule) getPodReplicationController(pod *v1.Pod, rcs []*v1.ReplicationController) (controllers []*v1.ReplicationController) {
	for _, rc := range rcs {
		if isPodBelongToRc(pod, rc) {
			controllers = append(controllers, rc)
		}
	}
	return
}

func (nsRs *NSReschedule) getPodReplicaSets(pod *v1.Pod, rss []*extensions.ReplicaSet) (controllers []*extensions.ReplicaSet) {
	for _, rs := range rss {
		if isPodBelongToRs(pod, rs) {
			controllers = append(controllers, rs)
		}
	}

	return
}

func (nsRs *NSReschedule) signRCRescheduling(rc *v1.ReplicationController, str string) error {
	rcNew, errGet := nsRs.apiClient.GetReplication(rc.Namespace, rc.Name)
	if errGet != nil {
		return errGet
	}
	if rcNew.Annotations == nil {
		rcNew.Annotations = make(map[string]string)
	}
	if rcNew.Annotations[RCRescheduing] == str {
		if rc.Annotations == nil {
			rc.Annotations = make(map[string]string)
		}
		rc.Annotations[RCRescheduing] = str
		return nil
	}
	rcNew.Annotations[RCRescheduing] = str
	_, err := nsRs.apiClient.UpdateReplication(rcNew)
	if err != nil {
		glog.Errorf("signRCRescheduling %s %s error %v", rcNew.Name, str, err)
		return err
	}
	glog.Infof("signRCRescheduling %s %s success", rcNew.Name, str)
	if rc.Annotations == nil {
		rc.Annotations = make(map[string]string)
	}
	rc.Annotations[RCRescheduing] = str
	return nil
}

func (nsRs *NSReschedule) signRSRescheduling(rs *extensions.ReplicaSet, str string) error {
	rsNew, errGet := nsRs.apiClient.GetReplicaSet(rs.Namespace, rs.Name)
	if errGet != nil {
		return errGet
	}
	if rsNew.Annotations == nil {
		rsNew.Annotations = make(map[string]string)
	}
	if rsNew.Annotations[RCRescheduing] == str {
		if rs.Annotations == nil {
			rs.Annotations = make(map[string]string)
		}
		rs.Annotations[RCRescheduing] = str
		return nil
	}
	rsNew.Annotations[RCRescheduing] = str
	_, err := nsRs.apiClient.UpdateReplicaSet(rsNew)
	if err != nil {
		glog.Errorf("signRSRescheduling %s %s error %v", rsNew.Name, str, err)
		return err
	}
	glog.Infof("signRSRescheduling %s %s success", rsNew.Name, str)
	if rs.Annotations == nil {
		rs.Annotations = make(map[string]string)
	}
	rs.Annotations[RCRescheduing] = str
	return nil
}
func (nsRs *NSReschedule) getNodeMaxPodSize(cach *NodesInfoCach) int {
	var max int = 0
	for _, nc := range cach.nodecach {
		l := len(nc.pods)
		if l > max {
			max = l
		}
	}
	return max
}

//func (nsRs *NSReschedule) getTimeScore(t unversione.Time) int {
//	now := time.Now()
//	d1, _ := time.ParseDuration("-10m")
//	d2, _ := time.ParseDuration("-1h")
//	d3, _ := time.ParseDuration("-24h")

//	if t.After(now.Add(d1)) {
//		return 0
//	} else if t.After(now.Add(d2)) {
//		return 20
//	} else if t.After(now.Add(d3)) {
//		return 50
//	} else {
//		return 100
//	}
//}
func (nsRs *NSReschedule) getNodePodSizeScore(size, maxsize int) int {
	if maxsize == 0 {
		return 100
	} else {
		return int(100 * (float64(size) / float64(maxsize)))
	}
}
func (nsRs *NSReschedule) getPodSizeOfRcsAndRss(pods []*v1.Pod, rcs []*v1.ReplicationController, rss []*extensions.ReplicaSet) int {
	var size int = 0
	for _, pod := range pods {
		var flag bool = false
		for _, rc := range rcs {
			if isPodBelongToRc(pod, rc) {
				flag = true
				break
			}
		}
		if flag {
			size += 1
			continue
		}
		for _, rs := range rss {
			if isPodBelongToRs(pod, rs) {
				flag = true
				break
			}

		}
		if flag {
			size += 1
		}
	}
	return size
}
func (nsRs *NSReschedule) getRcsAndRssReplicas(rcs []*v1.ReplicationController, rss []*extensions.ReplicaSet) int {
	var maxReplicas int32 = 0
	for _, rc := range rcs {
		var rcReplicas int32
		if rc.Spec.Replicas != nil {
			rcReplicas = *rc.Spec.Replicas
		}
		if rcReplicas > maxReplicas {
			maxReplicas = rcReplicas
		}
	}
	for _, rs := range rss {
		var rsReplicas int32
		if rs.Spec.Replicas != nil {
			rsReplicas = *rs.Spec.Replicas
		}
		if rsReplicas > maxReplicas {
			maxReplicas = rsReplicas
		}
	}
	return int(maxReplicas)
}
func (nsRs *NSReschedule) getRcsAndRssScore(theSameRcOrRsPodSize, maxReplicas int) int {
	if maxReplicas <= 0 || theSameRcOrRsPodSize == 1 {
		return 100
	}
	if theSameRcOrRsPodSize >= maxReplicas || theSameRcOrRsPodSize == 0 {
		return 0
	}
	return int(100 * float64(maxReplicas-theSameRcOrRsPodSize+1) / float64(maxReplicas))
}
func (nsRs *NSReschedule) getNamespaceScore(size, sumPods int) int {
	if sumPods <= 0 || size <= 0 {
		return 0
	}
	if size >= sumPods {
		return 100
	}
	return int(100 * (float64(size) / float64(sumPods)))
}
func (nsRs *NSReschedule) getPodRescheduleScore(cach *NodesInfoCach, pod *v1.Pod, fromNode, toNode *v1.Node,
	rcs []*v1.ReplicationController, rss []*extensions.ReplicaSet) (int, bool, error) {
	if ok, err := nsRs.isPodCanBeRescheduledToNode(pod, toNode); !ok {
		return 0, false, err
	}

	ncOld, findOld := cach.nodecach[fromNode.Name]
	ncNew, findNew := cach.nodecach[toNode.Name]
	if findOld == false || findNew == false {
		return 0, false, fmt.Errorf("error find ncOld or ncNew")
	}
	var oldScore, newScore int = 0, 0
	podRcs := nsRs.getPodReplicationController(pod, rcs)
	podRss := nsRs.getPodReplicaSets(pod, rss)
	maxReplicas := nsRs.getRcsAndRssReplicas(podRcs, podRss)
	theOldNodeSameRcOrRsPodSize := nsRs.getPodSizeOfRcsAndRss(ncOld.pods, podRcs, podRss)
	theNewNodeSameRcOrRsPodSize := nsRs.getPodSizeOfRcsAndRss(ncNew.pods, podRcs, podRss)

	//oldS1 := maxInt(nsRs.getNamespaceScore(ncOld.nsCount[pod.Namespace], len(ncOld.pods)), nsRs.getNamespaceScore(ncNew.nsCount[pod.Namespace], len(ncNew.pods))) +
	//	maxInt(ncOld.nsCount[pod.Namespace], ncNew.nsCount[pod.Namespace])
	//oldS2 := nsRs.getTimeScore(pod.CreationTimestamp)
	oldS3 := nsRs.getRcsAndRssScore(maxInt(theOldNodeSameRcOrRsPodSize, theNewNodeSameRcOrRsPodSize), maxReplicas)
	oldScore = /*oldS1 + oldS2 + */ oldS3

	//newS1 := maxInt(nsRs.getNamespaceScore(ncOld.nsCount[pod.Namespace]-1, len(ncOld.pods)-1), nsRs.getNamespaceScore(ncNew.nsCount[pod.Namespace]+1, len(ncNew.pods)+1)) +
	//	maxInt(ncOld.nsCount[pod.Namespace]-1, ncNew.nsCount[pod.Namespace]+1)
	//newS2 := nsRs.getTimeScore(unversioned.NewTime(time.Now()))
	newS3 := nsRs.getRcsAndRssScore(maxInt(theOldNodeSameRcOrRsPodSize-1, theNewNodeSameRcOrRsPodSize+1), maxReplicas)
	newScore = /*newS1 + newS2 + */ newS3

	if newScore > oldScore {
		return newScore, true, nil
	} else {
		return newScore, false, nil
	}
}
