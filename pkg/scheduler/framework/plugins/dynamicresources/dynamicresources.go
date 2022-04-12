/*
Copyright 2022 The Kubernetes Authors.

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

package dynamicresources

import (
	"context"
	"errors"
	"fmt"
	"sync"

	cdiv1alpha1 "k8s.io/api/cdi/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	cdiv1alpha1listers "k8s.io/client-go/listers/cdi/v1alpha1"
	"k8s.io/component-helpers/cdi/resourceclaim"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name = names.DynamicResources

	stateKey framework.StateKey = Name
)

// The state is initialized in PreFilter phase. Because we save the pointer in
// framework.CycleState, in the later phases we don't need to call Write method
// to update the value
type stateData struct {
	clientset kubernetes.Interface

	// A copy of all claims for the Pod, initially with the
	// status from the start of the scheduling cycle. Each claim
	// instance is read-only because it might come from the informer
	// cache. The instances get replaced when the plugin itself
	// successfully does an Update.
	//
	// Empty if the Pod has no claims.
	claims []*cdiv1alpha1.ResourceClaim

	// The indices of all claims that:
	// - are allocated
	// - use delayed allocation
	// - were not available on at least one node
	//
	// Set in parallel during Filter, so write access there must be
	// protected by the mutex. Used by PostFilter.
	unavailableClaims sets.Int

	mutex sync.Mutex
}

func (d *stateData) Clone() framework.StateData {
	return d
}

func (d *stateData) updateClaimStatus(ctx context.Context, index int, claim *cdiv1alpha1.ResourceClaim) error {
	// TODO (?): replace with patch operation. Beware that patching must only succeed if the
	// object has not been modified in parallel by someone else.
	claim, err := d.clientset.CdiV1alpha1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
	// TODO: metric for update results, with the operation ("set selected
	// node", "set PotentialNodes", etc.) as one dimension.
	if err != nil {
		return fmt.Errorf("update resource claim: %w", err)
	}

	// Remember the new instance. This is relevant when the plugin must
	// update the same claim multiple times (for example, first set
	// PotentialNodes, then SelectedNode), because otherwise the second
	// update would fail with a "was modified" error.
	d.claims[index] = claim

	return nil
}

// dynamicResources is a plugin that ensures that ResourceClaims are allocated.
type dynamicResources struct {
	clientset   kubernetes.Interface
	claimLister cdiv1alpha1listers.ResourceClaimLister
}

// New initializes a new plugin and returns it.
func New(plArgs runtime.Object, fh framework.Handle) (framework.Plugin, error) {
	return &dynamicResources{
		clientset:   fh.ClientSet(),
		claimLister: fh.SharedInformerFactory().Cdi().V1alpha1().ResourceClaims().Lister(),
	}, nil
}

var _ framework.PreFilterPlugin = &dynamicResources{}
var _ framework.FilterPlugin = &dynamicResources{}
var _ framework.PostFilterPlugin = &dynamicResources{}
var _ framework.PreScorePlugin = &dynamicResources{}
var _ framework.ReservePlugin = &dynamicResources{}
var _ framework.EnqueueExtensions = &dynamicResources{}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *dynamicResources) Name() string {
	return Name
}

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
func (pl *dynamicResources) EventsToRegister() []framework.ClusterEvent {
	events := []framework.ClusterEvent{
		// Allocation is tracked in ResourceClaims, so any changes may make the pods schedulable.
		//
		// This isn't perfect: when we trigger delayed allocation by setting SelectedNode,
		// kube-scheduler will almost immediately get that updated object back via the
		// informer and try to schedule the pod again although nothing has changed:
		//
		//  Type     Reason            Age   From               Message
		//  ----     ------            ----  ----               -------
		//  Warning  FailedScheduling  95s   default-scheduler  0/1 nodes are available: 1 waiting for dynamic resource controller to create the resourceclaim "pause-resource".
		//  Warning  FailedScheduling  94s   default-scheduler  running Reserve plugin "DynamicResources": waiting for resource driver to allocate resource claim
		//  Warning  FailedScheduling  91s   default-scheduler  running Reserve plugin "DynamicResources": waiting for resource driver to allocate resource claim
		//
		// TODO (?): filter out Update events where the object's Generation matches a "known"
		// generation. Keeping track of such known objects must use a structure with
		// a bounded amount of memory because we cannot allow kube-scheduler's memory consumption
		// to grow over time.
		{Resource: "resourceclaims.v1alpha1.cdi.k8s.io", ActionType: framework.Add | framework.Update},
		// A resource might depend on node labels for topology filtering.
		// A new or updated node may make pods schedulable.
		{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeLabel},
	}
	return events
}

// podHasClaims returns the ResourceClaims for all pod.Spec.PodResourceClaims.
func (pl *dynamicResources) podHasClaims(pod *v1.Pod) ([]*cdiv1alpha1.ResourceClaim, error) {
	claims := make([]*cdiv1alpha1.ResourceClaim, 0, len(pod.Spec.ResourceClaims))
	for _, resource := range pod.Spec.ResourceClaims {
		claimName := resourceclaim.Name(pod, &resource)
		isEphemeral := resource.ResourceClaimName == nil
		claim, err := pl.claimLister.ResourceClaims(pod.Namespace).Get(claimName)
		if err != nil {
			// The error usually has already enough context ("resourcevolumeclaim "myclaim" not found"),
			// but we can do better for generic ephemeral inline volumes where that situation
			// is normal directly after creating a pod.
			if isEphemeral && apierrors.IsNotFound(err) {
				err = fmt.Errorf("waiting for dynamic resource controller to create the resourceclaim %q", claimName)
			}
			return nil, err
		}

		if claim.DeletionTimestamp != nil {
			return nil, fmt.Errorf("resourceclaim %q is being deleted", claim.Name)
		}

		if isEphemeral {
			if err := resourceclaim.IsForPod(pod, claim); err != nil {
				return nil, err
			}
		}
		// We store the pointer as returned by the lister. The
		// assumption is that if a claim gets modified while our code
		// runs, the cache will store a new pointer, not mutate the
		// existing object that we point to here.
		claims = append(claims, claim)
	}
	return claims, nil
}

// PreFilter invoked at the prefilter extension point to check if pod has all
// immediate claims bound. UnschedulableAndUnresolvable is returned if
// the pod cannot be scheduled at the moment on any node.
func (pl *dynamicResources) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	logger := klog.FromContext(ctx)

	// If pod does not reference any claim, we don't need to do anything.
	claims, err := pl.podHasClaims(pod)
	logger.V(5).Info("pod resource claims", "pod", klog.KObj(pod), "resourceclaims", klog.KObjs(claims), "err", err)
	if err != nil {
		return nil, statusUnschedulable(logger, err.Error())
	}
	if len(claims) == 0 {
		state.Write(stateKey, &stateData{})
		return nil, nil
	}

	for _, claim := range claims {
		if claim.Spec.AllocationMode == v1.AllocationModeImmediate &&
			claim.Status.Phase != cdiv1alpha1.ResourceClaimAllocated {
			// This will get resolved by the resource driver.
			return nil, statusUnschedulable(logger, "unallocated immediate resourceclaim", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}
		if claim.Status.Phase == cdiv1alpha1.ResourceClaimReallocate {
			// Same here
			return nil, statusUnschedulable(logger, "resourceclaim must be reallocated", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}
		if claim.Status.Phase == cdiv1alpha1.ResourceClaimAllocated &&
			!canBeReserved(claim) &&
			!isReservedForPod(pod, claim) {
			// Resource is in use. The pod has to wait.
			return nil, statusUnschedulable(logger, "resourceclaim in use", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}
	}

	state.Write(stateKey, &stateData{clientset: pl.clientset, claims: claims})
	return nil, nil
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *dynamicResources) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func getStateData(cs *framework.CycleState) (*stateData, error) {
	state, err := cs.Read(stateKey)
	if err != nil {
		return nil, err
	}
	s, ok := state.(*stateData)
	if !ok {
		return nil, errors.New("unable to convert state into stateData")
	}
	return s, nil
}

// Filter invoked at the filter extension point.
// It evaluates if a pod can fit due to the resources it requests,
// for both allocated and unallocated claims.
//
// For claims that are bound, then it checks that the node affinity is
// satisfied by the given node.
//
// For claims that are unbound, it checks whether the claim might get allocated
// for the node.
func (pl *dynamicResources) Filter(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	state, err := getStateData(cs)
	if err != nil {
		return framework.AsStatus(err)
	}
	if len(state.claims) == 0 {
		return nil
	}

	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found")
	}

	logger := klog.FromContext(ctx)
	// We bail out early here as soon as we know that the pod is unschedulable.
	// We could also gather all reasons and then report all of them, but that is
	// more complex.
	var unavailableClaims []int
	for index, claim := range state.claims {
		logger.V(5).Info("filter", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim), "phase", claim.Status.Phase)
		switch claim.Status.Phase {
		case cdiv1alpha1.ResourceClaimAllocated:
			if claim.Status.Allocation.AvailableOnNodes != nil {
				nodeSelector, err := nodeaffinity.NewNodeSelector(claim.Status.Allocation.AvailableOnNodes)
				if err != nil {
					return framework.AsStatus(err)
				}
				if !nodeSelector.Match(node) {
					logger.V(5).Info("AvailableOnNodes does not match", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
					unavailableClaims = append(unavailableClaims, index)
				}
			}
		case cdiv1alpha1.ResourceClaimReallocate:
			// We shouldn't get here. PreFilter already checked this.
			return statusUnschedulable(logger, "resourceclaim must be reallocated", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
		case cdiv1alpha1.ResourceClaimPending:
			// This must be delayed allocation. Immediate
			// allocation was already checked for in PreFilter.
			if claim.Status.Scheduling.Driver.SuitableNodes != nil {
				nodeSelector, err := nodeaffinity.NewNodeSelector(claim.Status.Scheduling.Driver.SuitableNodes)
				if err != nil {
					return framework.AsStatus(err)
				}
				if !nodeSelector.Match(node) {
					return statusUnschedulable(logger, "resourceclaim cannot be allocated for the node (not suitable)", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim), "suitablenodes", claim.Status.Scheduling.Driver.SuitableNodes)
				}
			}
			for _, unsuitableNode := range claim.Status.Scheduling.Driver.UnsuitableNodes {
				if node.Name == unsuitableNode {
					return statusUnschedulable(logger, "resourceclaim cannot be allocated for the node (unsuitable)", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim), "unsuitablenodes", claim.Status.Scheduling.Driver.UnsuitableNodes)
				}
			}
		default:
			return statusUnschedulable(logger, "resourceclaim with unknown phase", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim), "phase", claim.Status.Phase)
		}
	}

	if len(unavailableClaims) > 0 {
		state.mutex.Lock()
		defer state.mutex.Unlock()
		state.unavailableClaims.Insert(unavailableClaims...)
		return statusUnschedulable(logger, "resourceclaim not available on the node", "pod", klog.KObj(pod))
	}

	return nil
}

// PostFilter checks whether freeing an allocated claim might help to get a Pod
// schedulable. This only gets called when filtering found no suitable node.
func (pl *dynamicResources) PostFilter(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	state, err := getStateData(cs)
	if err != nil {
		return nil, framework.AsStatus(err)
	}
	if len(state.claims) == 0 {
		return nil, nil
	}

	// Iterating over a map is random. This is intentional here, we want to
	// pick one claim randomly because there is no better heuristic.
	for index := range state.unavailableClaims {
		claim := state.claims[index]
		if len(claim.Status.ReservedFor) == 0 ||
			len(claim.Status.ReservedFor) == 1 && claim.Status.ReservedFor[0].UID == pod.UID {
			claim := state.claims[index].DeepCopy()
			claim.Status.Phase = cdiv1alpha1.ResourceClaimReallocate
			claim.Status.ReservedFor = nil
			klog.FromContext(ctx).V(5).Info("reallocate", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
			if err := state.updateClaimStatus(ctx, index, claim); err != nil {
				return nil, framework.AsStatus(err)
			}
			break
		}
	}
	return nil, nil
}

// PreScore is passed a list of all nodes that would fit the pod. Not all
// claims are necessarily allocated yet, so here we can set the SuitableNodes
// field for those which are pending.
func (pl *dynamicResources) PreScore(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	state, err := getStateData(cs)
	if err != nil {
		return framework.AsStatus(err)
	}
	if len(state.claims) == 0 {
		return nil
	}

	logger := klog.FromContext(ctx)
	for index, claim := range state.claims {
		if claim.Status.Phase == cdiv1alpha1.ResourceClaimPending {
			// Must be delayed allocation. We change the
			// SuitableNodes field if some new node became a
			// candidate. This means the list may contain nodes
			// that are not actually viable candidates right now,
			// but it's okay to not remove them and it may help to
			// avoid apiserver traffic.
			if haveAllNodes(claim.Status.Scheduling.Scheduler.PotentialNodes, nodes) {
				logger.V(5).Info("no need to update potential nodes", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim), "potentialnodes", nodes)
				continue
			}
			claim := claim.DeepCopy()
			claim.Status.Scheduling.Scheduler.PotentialNodes = make([]string, 0, len(nodes))
			for _, node := range nodes {
				claim.Status.Scheduling.Scheduler.PotentialNodes = append(claim.Status.Scheduling.Scheduler.PotentialNodes, node.Name)
			}
			logger.V(5).Info("update potential nodes", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim), "potentialnodes", nodes)
			if err := state.updateClaimStatus(ctx, index, claim); err != nil {
				return framework.AsStatus(err)
			}
		}
	}

	return nil
}

func haveAllNodes(nodeNames []string, nodes []*v1.Node) bool {
	for _, node := range nodes {
		if !haveNode(nodeNames, node.Name) {
			return false
		}
	}
	return true
}

func haveNode(nodeNames []string, nodeName string) bool {
	for _, n := range nodeNames {
		if n == nodeName {
			return true
		}
	}
	return false
}

// Reserve reserves claims for the pod.
func (pl *dynamicResources) Reserve(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	state, err := getStateData(cs)
	if err != nil {
		return framework.AsStatus(err)
	}
	if len(state.claims) == 0 {
		return nil
	}

	pending := false
	logger := klog.FromContext(ctx)
	for index, claim := range state.claims {
		switch claim.Status.Phase {
		case cdiv1alpha1.ResourceClaimAllocated:
			// Allocated, but perhaps not reserved yet.
			if isReservedForPod(pod, claim) {
				klog.V(5).Info("is reserved", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName}, "resourceclaim", klog.KObj(claim))
				continue
			}
			claim := claim.DeepCopy()
			claim.Status.ReservedFor = append(claim.Status.ReservedFor,
				metav1.OwnerReference{
					APIVersion: "v1",
					Kind:       "pod",
					Name:       pod.Name,
					UID:        pod.UID,
				})
			klog.V(5).Info("reserve", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName}, "resourceclaim", klog.KObj(claim))
			_, err := pl.clientset.CdiV1alpha1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
			// TODO: metric for update errors.
			if err != nil {
				return framework.AsStatus(err)
			}
			// If we get here, we know that reserving the claim for
			// the pod worked and we can proceed with scheduling
			// it.
		case cdiv1alpha1.ResourceClaimPending:
			// Must be delayed allocation. Tell driver to start allocation.
			pending = true
			claim.Status.Scheduling.Scheduler.SelectedNode = nodeName
			klog.V(5).Info("start allocation", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName}, "resourceclaim", klog.KObj(claim))
			if err := state.updateClaimStatus(ctx, index, claim); err != nil {
				// We bail out early here instead of continuing with
				// other claims because:
				// - its simpler
				// - it should be rare that a pod has multiple
				//   claims and updating one of them fails
				//
				// One potential error here is that the resource driver
				// refreshed the UnsuitableNodes, which currently
				// causes our update to fail.
				//
				// TODO: investigate how likely that is and whether
				// we can patch the selected node instead.
				return framework.AsStatus(err)
			}
		}
	}

	if pending {
		return statusUnschedulable(logger, "waiting for resource driver to allocate resource claim", "pod", klog.KObj(pod), "node", klog.ObjectRef{Name: nodeName})
	}

	return nil
}

func isReservedForPod(pod *v1.Pod, claim *cdiv1alpha1.ResourceClaim) bool {
	for _, reserved := range claim.Status.ReservedFor {
		if reserved.UID == pod.UID {
			return true
		}
	}
	return false
}

func canBeReserved(claim *cdiv1alpha1.ResourceClaim) bool {
	return claim.Status.Allocation.UserLimit == -1 ||
		claim.Status.Allocation.UserLimit > int64(len(claim.Status.ReservedFor))
}

// Unreserve clears the ReservedFor field for all claims.
// It's idempotent, and does nothing if no state found for the given pod.
func (pl *dynamicResources) Unreserve(ctx context.Context, cs *framework.CycleState, pod *v1.Pod, nodeName string) {
	state, err := getStateData(cs)
	if err != nil {
		return
	}
	if len(state.claims) == 0 {
		return
	}

	logger := klog.FromContext(ctx)
	for index, claim := range state.claims {
		if claim.Status.Phase == cdiv1alpha1.ResourceClaimAllocated &&
			isReservedForPod(pod, claim) {
			// Remove pod from ReservedFor.
			claim := claim.DeepCopy()
			reservedFor := make([]metav1.OwnerReference, 0, len(claim.Status.ReservedFor)-1)
			for _, reserved := range claim.Status.ReservedFor {
				if reserved.UID != pod.UID {
					reservedFor = append(reservedFor, reserved)
				}
			}
			claim.Status.ReservedFor = reservedFor
			logger.V(5).Info("unreserve", "resourceclaim", klog.KObj(claim))
			if err := state.updateClaimStatus(ctx, index, claim); err != nil {
				// We will get here again when pod scheduling
				// is retried.
				klog.ErrorS(err, "unreserve", "resourceclaim", klog.KObj(claim))
			}
		}
	}
	return
}

func statusUnschedulable(logger klog.Logger, reason string, kv ...interface{}) *framework.Status {
	if loggerV := logger.V(5); loggerV.Enabled() {
		helper, loggerV := loggerV.WithCallStackHelper()
		helper()
		kv = append(kv, "reason", reason)
		loggerV.Info("pod unschedulable", kv...)
	}
	return framework.NewStatus(framework.UnschedulableAndUnresolvable, reason)
}
