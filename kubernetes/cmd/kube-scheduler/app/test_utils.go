package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/watch"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	api "k8s.io/kubernetes/pkg/apis/core"

	policy "k8s.io/api/policy/v1beta1"
	restclient "k8s.io/client-go/rest"

	"github.com/evanphx/json-patch"
	"github.com/golang/glog"
)

var FakeNode *FakeNodeHandler = &FakeNodeHandler{}
var FakePod *FakePodHandler = &FakePodHandler{}
var FakeConfigMap *FakeConfigMapNodePodHandler = &FakeConfigMapNodePodHandler{}

type FakeNodeHandler struct {
	*fake.Clientset
	// Input: Hooks determine if request is valid or not
	CreateHook func(*FakeNodeHandler, *v1.Node) bool
	Existing   []*v1.Node

	// Output
	CreatedNodes        []*v1.Node
	DeletedNodes        []*v1.Node
	UpdatedNodes        []*v1.Node
	UpdatedNodeStatuses []*v1.Node
	RequestCount        int

	// Synchronization
	lock           sync.Mutex
	DeleteWaitChan chan struct{}
}
type FakePodHandler struct {
	*fake.Clientset
	ns string
	// Input: Hooks determine if request is valid or not
	CreateHook func(*FakePodHandler, *v1.Pod) bool
	Existing   []*v1.Pod

	// Output
	CreatedPods        []*v1.Pod
	DeletedPods        []*v1.Pod
	UpdatedPods        []*v1.Pod
	UpdatedPodStatuses []*v1.Pod
	RequestCount       int

	// Synchronization
	lock           sync.Mutex
	DeleteWaitChan chan struct{}
}

type FakeConfigMapNodePodHandler struct {
	*fake.Clientset
	ns string
	// Input: Hooks determine if request is valid or not
	CreateHook    func(*FakeConfigMapNodePodHandler, *v1.ConfigMap) bool
	Existing      []*v1.ConfigMap
	ExistingNodes []*v1.Node
	ExistingPods  []*v1.Pod

	// Output
	Createdconfigmaps         []*v1.ConfigMap
	Deletedconfigmaps         []*v1.ConfigMap
	Updatedconfigmaps         []*v1.ConfigMap
	UpdatedconfigmapsStatuses []*v1.ConfigMap
	RequestCount              int

	// Synchronization
	lock           sync.Mutex
	DeleteWaitChan chan struct{}
}

type FakeLegacyHandler struct {
	v1core.CoreV1Interface
	n *FakeNodeHandler
	p *FakePodHandler
	c *FakeConfigMapNodePodHandler
}

// Core returns fake CoreInterface.
func (n *FakeNodeHandler) CoreV1() v1core.CoreV1Interface {
	return &FakeLegacyHandler{n.Clientset.Core(), n, &FakePodHandler{Clientset: n.Clientset}, &FakeConfigMapNodePodHandler{Clientset: n.Clientset}}
}

// Core returns fake CoreInterface.
func (p *FakePodHandler) CoreV1() v1core.CoreV1Interface {
	return &FakeLegacyHandler{p.Clientset.CoreV1(), &FakeNodeHandler{Clientset: p.Clientset}, p, &FakeConfigMapNodePodHandler{Clientset: p.Clientset}}
}

// Core returns fake CoreInterface.
func (c *FakeConfigMapNodePodHandler) CoreV1() v1core.CoreV1Interface {
	if FakeNode.Clientset == nil {
		FakeNode = &FakeNodeHandler{
			Clientset: c.Clientset,
			Existing:  c.ExistingNodes,
		}
	}
	if FakePod.Clientset == nil {
		FakePod = &FakePodHandler{
			Clientset: c.Clientset,
			Existing:  c.ExistingPods,
		}
	}
	return &FakeLegacyHandler{c.Clientset.CoreV1(), FakeNode, FakePod, c}
}

func SetClientnil() {
	FakeNode.Clientset = nil
	FakePod.Clientset = nil

}

// Nodes return fake nodeInterfaces.
func (m *FakeLegacyHandler) Nodes() v1core.NodeInterface {
	return m.n
}

// Create adds a new Node to the fake store.
func (m *FakeNodeHandler) Create(node *v1.Node) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	for _, n := range m.Existing {
		if n.Name == node.Name {
			return nil, apierrors.NewAlreadyExists(api.Resource("nodes"), node.Name)
		}
	}
	if m.CreateHook == nil || m.CreateHook(m, node) {
		nodeCopy := *node
		m.CreatedNodes = append(m.CreatedNodes, &nodeCopy)
		return node, nil
	} else {
		return nil, errors.New("Create error.")
	}
}

// Get returns a Node from the fake store.
func (m *FakeNodeHandler) Get(name string, opts metav1.GetOptions) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	for i := range m.UpdatedNodes {
		if m.UpdatedNodes[i].Name == name {
			nodeCopy := *m.UpdatedNodes[i]
			return &nodeCopy, nil
		}
	}
	for i := range m.Existing {
		if m.Existing[i].Name == name {
			nodeCopy := *m.Existing[i]
			return &nodeCopy, nil
		}
	}
	return nil, nil
}

// List returns a list of Nodes from the fake store.
func (m *FakeNodeHandler) List(opts metav1.ListOptions) (*v1.NodeList, error) {

	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	var nodes []*v1.Node
	for i := 0; i < len(m.UpdatedNodes); i++ {
		if !contains(m.UpdatedNodes[i], m.DeletedNodes) {
			nodes = append(nodes, m.UpdatedNodes[i])
		}
	}
	for i := 0; i < len(m.Existing); i++ {
		if !contains(m.Existing[i], m.DeletedNodes) && !contains(m.Existing[i], nodes) {
			nodes = append(nodes, m.Existing[i])
		}
	}
	for i := 0; i < len(m.CreatedNodes); i++ {
		if !contains(m.CreatedNodes[i], m.DeletedNodes) && !contains(m.CreatedNodes[i], nodes) {
			nodes = append(nodes, m.CreatedNodes[i])
		}
	}
	nodeList := &v1.NodeList{}
	for _, node := range nodes {
		nodeList.Items = append(nodeList.Items, *node)
	}
	return nodeList, nil
}

// Delete delets a Node from the fake store.
func (m *FakeNodeHandler) Delete(id string, opt *metav1.DeleteOptions) error {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		if m.DeleteWaitChan != nil {
			m.DeleteWaitChan <- struct{}{}
		}
		m.lock.Unlock()
	}()
	m.DeletedNodes = append(m.DeletedNodes, NewNode(id))
	return nil
}

// DeleteCollection deletes a collection of Nodes from the fake store.
func (m *FakeNodeHandler) DeleteCollection(opt *metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return nil
}

// Update updates a Node in the fake store.
func (m *FakeNodeHandler) Update(node *v1.Node) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	nodeCopy := *node
	for i, updateNode := range m.UpdatedNodes {
		if updateNode.Name == nodeCopy.Name {
			m.UpdatedNodes[i] = &nodeCopy
			return node, nil
		}
	}
	m.UpdatedNodes = append(m.UpdatedNodes, &nodeCopy)
	return node, nil
}

// UpdateStatus updates a status of a Node in the fake store.
func (m *FakeNodeHandler) UpdateStatus(node *v1.Node) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	var origNodeCopy v1.Node
	found := false
	for i := range m.Existing {
		if m.Existing[i].Name == node.Name {
			origNodeCopy = *m.Existing[i]
			found = true
		}
	}
	updatedNodeIndex := -1
	for i := range m.UpdatedNodes {
		if m.UpdatedNodes[i].Name == node.Name {
			origNodeCopy = *m.UpdatedNodes[i]
			updatedNodeIndex = i
			found = true
		}
	}

	if !found {
		return nil, fmt.Errorf("Not found node %v", node)
	}

	origNodeCopy.Status = node.Status
	if updatedNodeIndex < 0 {
		m.UpdatedNodes = append(m.UpdatedNodes, &origNodeCopy)
	} else {
		m.UpdatedNodes[updatedNodeIndex] = &origNodeCopy
	}

	nodeCopy := *node
	m.UpdatedNodeStatuses = append(m.UpdatedNodeStatuses, &nodeCopy)
	return node, nil
}

// PatchStatus patches a status of a Node in the fake store.
func (m *FakeNodeHandler) PatchStatus(nodeName string, data []byte) (*v1.Node, error) {
	m.RequestCount++
	return &v1.Node{}, nil
}

// Watch watches Nodes in a fake store.
func (m *FakeNodeHandler) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

// Patch patches a Node in the fake store.
func (m *FakeNodeHandler) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (*v1.Node, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	var nodeCopy v1.Node
	for i := range m.Existing {
		if m.Existing[i].Name == name {
			nodeCopy = *m.Existing[i]
		}
	}
	updatedNodeIndex := -1
	for i := range m.UpdatedNodes {
		if m.UpdatedNodes[i].Name == name {
			nodeCopy = *m.UpdatedNodes[i]
			updatedNodeIndex = i
		}
	}

	originalObjJS, err := json.Marshal(nodeCopy)
	if err != nil {
		glog.Errorf("Failed to marshal %v", nodeCopy)
		return nil, nil
	}
	var originalNode v1.Node
	if err = json.Unmarshal(originalObjJS, &originalNode); err != nil {
		glog.Errorf("Failed to unmarshall original object: %v", err)
		return nil, nil
	}

	var patchedObjJS []byte
	switch pt {
	case types.JSONPatchType:
		patchObj, err := jsonpatch.DecodePatch(data)
		if err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
		if patchedObjJS, err = patchObj.Apply(originalObjJS); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	case types.MergePatchType:
		if patchedObjJS, err = jsonpatch.MergePatch(originalObjJS, data); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	case types.StrategicMergePatchType:
		if patchedObjJS, err = strategicpatch.StrategicMergePatch(originalObjJS, data, originalNode); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	default:
		glog.Errorf("unknown Content-Type header for patch: %v", pt)
		return nil, nil
	}

	var updatedNode v1.Node
	if err = json.Unmarshal(patchedObjJS, &updatedNode); err != nil {
		glog.Errorf("Failed to unmarshall patched object: %v", err)
		return nil, nil
	}

	if updatedNodeIndex < 0 {
		m.UpdatedNodes = append(m.UpdatedNodes, &updatedNode)
	} else {
		m.UpdatedNodes[updatedNodeIndex] = &updatedNode
	}

	return &updatedNode, nil
}

// Pods return fake podInterfaces.
func (m *FakeLegacyHandler) Pods(name string) v1core.PodInterface {
	m.p.ns = name
	return m.p
}

// Create adds a new pod to the fake store.
func (m *FakePodHandler) Create(pod *v1.Pod) (*v1.Pod, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	for _, n := range m.Existing {
		if n.Name == pod.Name {
			return nil, apierrors.NewAlreadyExists(api.Resource("pods"), pod.Name)
		}
	}
	if m.CreateHook == nil || m.CreateHook(m, pod) {
		podCopy := *pod
		m.CreatedPods = append(m.CreatedPods, &podCopy)
		return pod, nil
	} else {
		return nil, errors.New("Create error.")
	}
}

// Get returns a pod from the fake store.
func (m *FakePodHandler) Get(name string, opts metav1.GetOptions) (*v1.Pod, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	for i := range m.DeletedPods {
		if m.DeletedPods[i].Name == name && m.DeletedPods[i].Namespace == m.ns {
			return nil, nil
		}
	}
	for i := range m.UpdatedPods {
		if m.UpdatedPods[i].Name == name && m.UpdatedPods[i].Namespace == m.ns {
			podCopy := *m.UpdatedPods[i]
			return &podCopy, nil
		}
	}
	for i := range m.Existing {
		if m.Existing[i].Name == name && m.Existing[i].Namespace == m.ns {
			podCopy := *m.Existing[i]
			return &podCopy, nil
		}
	}
	return nil, errors.New("NotFound")
}

// List returns a list of pods from the fake store.
func (m *FakePodHandler) List(opts metav1.ListOptions) (*v1.PodList, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	var pods []*v1.Pod
	for i := 0; i < len(m.UpdatedPods); i++ {
		if !containPod(m.UpdatedPods[i], m.DeletedPods) {
			pods = append(pods, m.UpdatedPods[i])
		}
	}
	for i := 0; i < len(m.Existing); i++ {
		if !containPod(m.Existing[i], m.DeletedPods) && !containPod(m.Existing[i], pods) {
			pods = append(pods, m.Existing[i])
		}
	}
	for i := 0; i < len(m.CreatedPods); i++ {
		if !containPod(m.CreatedPods[i], m.DeletedPods) && !containPod(m.CreatedPods[i], pods) {
			pods = append(pods, m.CreatedPods[i])
		}
	}
	podList := &v1.PodList{}
	for _, pod := range pods {
		podList.Items = append(podList.Items, *pod)
	}
	return podList, nil
}

// Delete delets a pod from the fake store.
func (m *FakePodHandler) Delete(id string, opt *metav1.DeleteOptions) error {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		if m.DeleteWaitChan != nil {
			m.DeleteWaitChan <- struct{}{}
		}
		m.lock.Unlock()
	}()
	m.DeletedPods = append(m.DeletedPods, NewPod(id, m.ns, ""))
	return nil
}

// DeleteCollection deletes a collection of pods from the fake store.
func (m *FakePodHandler) DeleteCollection(opt *metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return nil
}

// Update updates a pod in the fake store.
func (m *FakePodHandler) Update(pod *v1.Pod) (*v1.Pod, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	podCopy := *pod
	for i, updatepod := range m.UpdatedPods {
		if updatepod.Name == podCopy.Name {
			m.UpdatedPods[i] = &podCopy
			return pod, nil
		}
	}
	m.UpdatedPods = append(m.UpdatedPods, &podCopy)
	return pod, nil
}

// UpdateStatus updates a status of a pod in the fake store.
func (m *FakePodHandler) UpdateStatus(pod *v1.Pod) (*v1.Pod, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	var origpodCopy v1.Pod
	found := false
	for i := range m.Existing {
		if m.Existing[i].Name == pod.Name {
			origpodCopy = *m.Existing[i]
			found = true
		}
	}
	updatedpodIndex := -1
	for i := range m.UpdatedPods {
		if m.UpdatedPods[i].Name == pod.Name {
			origpodCopy = *m.UpdatedPods[i]
			updatedpodIndex = i
			found = true
		}
	}

	if !found {
		return nil, fmt.Errorf("Not found pod %v", pod)
	}

	origpodCopy.Status = pod.Status
	if updatedpodIndex < 0 {
		m.UpdatedPods = append(m.UpdatedPods, &origpodCopy)
	} else {
		m.UpdatedPods[updatedpodIndex] = &origpodCopy
	}

	podCopy := *pod
	m.UpdatedPodStatuses = append(m.UpdatedPodStatuses, &podCopy)
	return pod, nil
}

// PatchStatus patches a status of a pod in the fake store.
func (m *FakePodHandler) PatchStatus(podName string, data []byte) (*v1.Pod, error) {
	m.RequestCount++
	return &v1.Pod{}, nil
}

// Watch watches pods in a fake store.
func (m *FakePodHandler) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

// Patch patches a pod in the fake store.
func (m *FakePodHandler) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (*v1.Pod, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	var podCopy v1.Pod
	for i := range m.Existing {
		if m.Existing[i].Name == name {
			podCopy = *m.Existing[i]
		}
	}
	updatedpodIndex := -1
	for i := range m.UpdatedPods {
		if m.UpdatedPods[i].Name == name {
			podCopy = *m.UpdatedPods[i]
			updatedpodIndex = i
		}
	}

	originalObjJS, err := json.Marshal(podCopy)
	if err != nil {
		glog.Errorf("Failed to marshal %v", podCopy)
		return nil, nil
	}
	var originalpod v1.Pod
	if err = json.Unmarshal(originalObjJS, &originalpod); err != nil {
		glog.Errorf("Failed to unmarshall original object: %v", err)
		return nil, nil
	}

	var patchedObjJS []byte
	switch pt {
	case types.JSONPatchType:
		patchObj, err := jsonpatch.DecodePatch(data)
		if err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
		if patchedObjJS, err = patchObj.Apply(originalObjJS); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	case types.MergePatchType:
		if patchedObjJS, err = jsonpatch.MergePatch(originalObjJS, data); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	case types.StrategicMergePatchType:
		if patchedObjJS, err = strategicpatch.StrategicMergePatch(originalObjJS, data, originalpod); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	default:
		glog.Errorf("unknown Content-Type header for patch: %v", pt)
		return nil, nil
	}

	var updatedpod v1.Pod
	if err = json.Unmarshal(patchedObjJS, &updatedpod); err != nil {
		glog.Errorf("Failed to unmarshall patched object: %v", err)
		return nil, nil
	}

	if updatedpodIndex < 0 {
		m.UpdatedPods = append(m.UpdatedPods, &updatedpod)
	} else {
		m.UpdatedPods[updatedpodIndex] = &updatedpod
	}

	return &updatedpod, nil
}

//pod  expansion

// Bind applies the provided binding to the named pod in the current namespace (binding.Namespace is ignored).
func (c *FakePodHandler) Bind(binding *v1.Binding) error {
	return nil
}

func (c *FakePodHandler) Evict(eviction *policy.Eviction) error {
	return nil
}

// Get constructs a request for getting the logs for a pod
func (c *FakePodHandler) GetLogs(name string, opts *v1.PodLogOptions) *restclient.Request {
	return nil
}

// ConfigMaps return fake podInterfaces.
func (m *FakeLegacyHandler) ConfigMaps(name string) v1core.ConfigMapInterface {
	m.c.ns = name
	return m.c
}

// Create adds a new configmap to the fake store.
func (m *FakeConfigMapNodePodHandler) Create(configmap *v1.ConfigMap) (*v1.ConfigMap, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	for _, n := range m.Existing {
		if n.Name == configmap.Name {
			return nil, apierrors.NewAlreadyExists(api.Resource("configmaps"), configmap.Name)
		}
	}
	if m.CreateHook == nil || m.CreateHook(m, configmap) {
		configmapCopy := *configmap
		m.Createdconfigmaps = append(m.Createdconfigmaps, &configmapCopy)
		return configmap, nil
	} else {
		return nil, nil
	}
}

// Get returns a configmap from the fake store.
func (m *FakeConfigMapNodePodHandler) Get(name string, opts metav1.GetOptions) (*v1.ConfigMap, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	for i := range m.Updatedconfigmaps {
		if m.Updatedconfigmaps[i].Name == name {
			configmapCopy := *m.Updatedconfigmaps[i]
			return &configmapCopy, nil
		}
	}
	for i := range m.Existing {
		if m.Existing[i].Name == name {
			configmapCopy := *m.Existing[i]
			return &configmapCopy, nil
		}
	}
	for i := range m.Createdconfigmaps {
		if m.Createdconfigmaps[i].Name == name {
			configmapCopy := *m.Createdconfigmaps[i]
			return &configmapCopy, nil
		}
	}

	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "configmap"}, "")
}

// List returns a list of configmaps from the fake store.
func (m *FakeConfigMapNodePodHandler) List(opts metav1.ListOptions) (*v1.ConfigMapList, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	var configmaps []*v1.ConfigMap
	for i := 0; i < len(m.Updatedconfigmaps); i++ {
		if !containConfig(m.Updatedconfigmaps[i], m.Deletedconfigmaps) {
			configmaps = append(configmaps, m.Updatedconfigmaps[i])
		}
	}
	for i := 0; i < len(m.Existing); i++ {
		if !containConfig(m.Existing[i], m.Deletedconfigmaps) && !containConfig(m.Existing[i], configmaps) {
			configmaps = append(configmaps, m.Existing[i])
		}
	}
	for i := 0; i < len(m.Createdconfigmaps); i++ {
		if !containConfig(m.Createdconfigmaps[i], m.Deletedconfigmaps) && !containConfig(m.Createdconfigmaps[i], configmaps) {
			configmaps = append(configmaps, m.Createdconfigmaps[i])
		}
	}
	configmapList := &v1.ConfigMapList{}
	for _, configmap := range configmaps {
		configmapList.Items = append(configmapList.Items, *configmap)
	}
	return configmapList, nil
}

// Delete delets a configmap from the fake store.
func (m *FakeConfigMapNodePodHandler) Delete(id string, opt *metav1.DeleteOptions) error {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		if m.DeleteWaitChan != nil {
			m.DeleteWaitChan <- struct{}{}
		}
		m.lock.Unlock()
	}()
	m.Deletedconfigmaps = append(m.Deletedconfigmaps, Newconfigmap(id))
	return nil
}

// DeleteCollection deletes a collection of configmaps from the fake store.
func (m *FakeConfigMapNodePodHandler) DeleteCollection(opt *metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return nil
}

// Update updates a configmap in the fake store.
func (m *FakeConfigMapNodePodHandler) Update(configmap *v1.ConfigMap) (*v1.ConfigMap, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()

	configmapCopy := *configmap
	for i, updateconfigmap := range m.Updatedconfigmaps {
		if updateconfigmap.Name == configmapCopy.Name {
			m.Updatedconfigmaps[i] = &configmapCopy
			return configmap, nil
		}
	}
	m.Updatedconfigmaps = append(m.Updatedconfigmaps, &configmapCopy)
	return configmap, nil
}

// PatchStatus patches a status of a configmap in the fake store.
func (m *FakeConfigMapNodePodHandler) PatchStatus(configmapName string, data []byte) (*v1.ConfigMap, error) {
	m.RequestCount++
	return &v1.ConfigMap{}, nil
}

// Watch watches configmaps in a fake store.
func (m *FakeConfigMapNodePodHandler) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

// Patch patches a configmap in the fake store.
func (m *FakeConfigMapNodePodHandler) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (*v1.ConfigMap, error) {
	m.lock.Lock()
	defer func() {
		m.RequestCount++
		m.lock.Unlock()
	}()
	var configmapCopy v1.ConfigMap
	for i := range m.Existing {
		if m.Existing[i].Name == name {
			configmapCopy = *m.Existing[i]
		}
	}
	updatedconfigmapIndex := -1
	for i := range m.Updatedconfigmaps {
		if m.Updatedconfigmaps[i].Name == name {
			configmapCopy = *m.Updatedconfigmaps[i]
			updatedconfigmapIndex = i
		}
	}

	originalObjJS, err := json.Marshal(configmapCopy)
	if err != nil {
		glog.Errorf("Failed to marshal %v", configmapCopy)
		return nil, nil
	}
	var originalconfigmap v1.ConfigMap
	if err = json.Unmarshal(originalObjJS, &originalconfigmap); err != nil {
		glog.Errorf("Failed to unmarshall original object: %v", err)
		return nil, nil
	}

	var patchedObjJS []byte
	switch pt {
	case types.JSONPatchType:
		patchObj, err := jsonpatch.DecodePatch(data)
		if err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
		if patchedObjJS, err = patchObj.Apply(originalObjJS); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	case types.MergePatchType:
		if patchedObjJS, err = jsonpatch.MergePatch(originalObjJS, data); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	case types.StrategicMergePatchType:
		if patchedObjJS, err = strategicpatch.StrategicMergePatch(originalObjJS, data, originalconfigmap); err != nil {
			glog.Error(err.Error())
			return nil, nil
		}
	default:
		glog.Errorf("unknown Content-Type header for patch: %v", pt)
		return nil, nil
	}

	var updatedconfigmap v1.ConfigMap
	if err = json.Unmarshal(patchedObjJS, &updatedconfigmap); err != nil {
		glog.Errorf("Failed to unmarshall patched object: %v", err)
		return nil, nil
	}

	if updatedconfigmapIndex < 0 {
		m.Updatedconfigmaps = append(m.Updatedconfigmaps, &updatedconfigmap)
	} else {
		m.Updatedconfigmaps[updatedconfigmapIndex] = &updatedconfigmap
	}

	return &updatedconfigmap, nil
}

// NewNode is a helper function for creating Nodes for testing.
func NewNode(name string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}

// Newconfigmap is a helper function for creating configmaps for testing.
func NewPod(name, namespace, node string) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: v1.PodSpec{
			NodeName: node,
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

	return pod
}

func contains(node *v1.Node, nodes []*v1.Node) bool {
	for i := 0; i < len(nodes); i++ {
		if node.Name == nodes[i].Name {
			return true
		}
	}
	return false
}
func containPod(pod *v1.Pod, pods []*v1.Pod) bool {
	for i := 0; i < len(pods); i++ {
		if pod.Name == pods[i].Name {
			return true
		}
	}
	return false
}
func containConfig(node *v1.ConfigMap, nodes []*v1.ConfigMap) bool {
	for i := 0; i < len(nodes); i++ {
		if node.Name == nodes[i].Name {
			return true
		}
	}
	return false
}

// Newconfigmap is a helper function for creating configmap for testing.
func Newconfigmap(name string) *v1.ConfigMap {
	configmap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	return configmap
}

// NewNamespace is a helper function for creating Namespace for testing.
func NewNamespace(name string) *v1.Namespace {
	namespace := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	return namespace
}
