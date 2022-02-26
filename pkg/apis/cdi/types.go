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

package cdi

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClass is used by administrators to influence how resources
// are allocated.
type ResourceClass struct {
	metav1.TypeMeta
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta

	// DriverName determines which resource driver is to be used for
	// allocation of a ResourceClaim that uses this class.
	//
	// Resource drivers have a unique name in reverse domain order
	// (acme.example.com).
	DriverName string

	// Parameters holds arbitrary values that will be available to the
	// driver when allocating a resource that uses this class. The driver
	// will be able to distinguish between parameters stored here and and
	// those stored in ResourceClaimSpec. These parameters here can only be
	// set by cluster administrators.
	Parameters runtime.RawExtension
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClassList is a collection of resource classes.
type ResourceClassList struct {
	metav1.TypeMeta
	// Standard list metadata
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ListMeta

	// Items is the list of resource classes.
	Items []ResourceClass
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClaim is created by users to describe which resources they need.
// Its status tracks whether the resource has been allocated and what the
// resulting attributes are.
type ResourceClaim struct {
	metav1.TypeMeta

	// The driver must set a finalizer here before it attempts to allocate
	// the resource. It removes the finalizer again when a) the allocation
	// attempt has definitely failed or b) when the allocated resource was
	// freed. This ensures that resources are not leaked.
	//
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta

	// Spec describes the desired attributes of a resource that then needs
	// to be allocated. It can only be set once when creating the
	// ResourceClaim.
	Spec core.ResourceClaimSpec

	// Status describes whether the resource is available and with which
	// attributes.
	Status ResourceClaimStatus
}

// ResourceClaimStatus tracks whether the resource has been allocated and what
// the resulting attributes are.
type ResourceClaimStatus struct {
	// Phase explains what the current status of the claim is
	// and determines which component needs to do something.
	Phase ResourceClaimPhase

	// DriverName is a copy of the driver name from the ResourceClass at
	// the time when allocation started. It's necessary to support
	// deallocation when the class gets deleted before a claim.
	DriverName string

	// Scheduling contains information that is only relevant while the
	// scheduler and the resource driver are in the process of selecting a
	// node for a Pod and the allocation mode is AllocationModeDelayed. The
	// resource driver should unset this when it has successfully allocated
	// the resource.
	Scheduling SchedulingStatus

	// Allocation is set by the resource driver once a resource has been
	// allocated succesfully.
	Allocation AllocationResult

	// ReservedFor indicates which entities are currently allowed to use
	// the resource.  Usually those are Pods, but any other object that
	// currently exists is also possible.
	//
	// A scheduler must add a Pod that it is scheduling. This must be done
	// in an atomic ResourceClaim update because there might be multiple
	// schedulers working on different Pods that compete for access to the
	// same ResourceClaim.
	//
	// kubelet will check this before allowing a Pod to run because a
	// scheduler might have missed that step, for example because it
	// doesn't support dynamic resource allocation or the feature was
	// disabled.
	ReservedFor []metav1.OwnerReference

	// UsedOnNodes is a list of nodes where the ResourceClaim is or is
	// going to be used. This must be set by the scheduler after scheduling
	// a Pod onto a node.
	//
	// List/watch requests for ResourceClaims can filter on this field
	// using a "status.usedOnNodes.<entry>=1" fieldSelector. kubelet uses
	// this to limit which ResourceClaims it receives from the apiserver.
	UsedOnNodes []string
}

// ResourceClaimPhase determines whether a ResourceClaim is currently pending
// (ResourceClaimPending), allocated (ResourceClaimAllocated) or needs to be
// reallocated (ResourceClaimReallocate). Other phases may get added in the
// future.
type ResourceClaimPhase string

const (
	// The claim is waiting for allocation by the driver.
	//
	// For delayed allocation, the driver will wait for a selected node
	// before it starts an allocation attempt.
	ResourceClaimPending ResourceClaimPhase = "Pending"

	// Set by the driver once the resource has been successfully
	// allocated. The scheduler waits for all resources used by
	// a Pod to be in this phase.
	ResourceClaimAllocated ResourceClaimPhase = "Allocated"

	// It can happen that a resource got allocated for a Pod and then the
	// Pod cannot be scheduled onto the nodes where the allocated resource
	// is available. The scheduler detects this and then sets the
	// “reallocate” phase to tell the driver that it must free the
	// resource. The driver does that and resets the ResourceClaimPhase
	// back to "Pending".
	ResourceClaimReallocate ResourceClaimPhase = "Reallocate"
)

// SchedulingStatus is used while handling delayed allocation.
type SchedulingStatus struct {
	// Scheduler contains information provided by the scheduler.
	Scheduler SchedulerSchedulingStatus

	// DriverStatus contains information provided by the resource driver.
	Driver DriverSchedulingStatus
}

// SchedulerSchedulingStatus contains information provided by the scheduler
// while handling delayed allocation.
type SchedulerSchedulingStatus struct {
	// When allocation is delayed, the scheduler must set
	// the node for which it wants the resource to be allocated
	// before the driver proceeds with allocation.
	//
	// For immediate allocation, the scheduler will not set
	// this field. The resource driver controller may
	// set it to trigger allocation on a specific node if the
	// resources are local to nodes.
	//
	// List/watch requests for ResourceClaims can filter on this field
	// using a "status.scheduling.scheduler.selectedNode=NAME"
	// fieldSelector.
	SelectedNode string

	// When allocation is delayed, and the scheduler needs to
	// decide on which node a Pod should run, it will
	// ask the driver on which nodes the resource might be
	// made available. To trigger that check, the scheduler
	// provides the names of nodes which might be suitable
	// for the Pod. Will be updated periodically until
	// the claim is allocated.
	PotentialNodes []string
}

// DriverSchedulingStatus contains information provided by the resource driver
// while handling delayed allocation.
type DriverSchedulingStatus struct {
	// Only nodes matching the selector will be considered by the scheduler
	// when trying to find a Node that fits a Pod. A resource driver can
	// set this immediately when a ResourceClaim gets created and, for
	// example, provide a static selector that uses labels.
	//
	// Setting this field is optional. If nil, all nodes are candidates.
	SuitableNodes *core.NodeSelector

	// A change of the PotentialNodes field triggers a check in the driver
	// on which of those nodes the resource might be made available. It
	// then excludes nodes by listing those where that is not the case in
	// UnsuitableNodes.
	//
	// Unsuitable nodes will be ignored by the scheduler when selecting a
	// node for a Pod. All other nodes are potential candidates, either
	// because no information is available yet or because allocation might
	// succeed.
	//
	// This can change, so the driver must refresh this information
	// periodically and/or after changing resource allocation for some
	// other ResourceClaim until a node gets selected by the scheduler.
	UnsuitableNodes []string
}

// AllocationResult contains attributed of an allocated resource.
type AllocationResult struct {
	// Attributes contains arbitrary data returned by the driver after a
	// successful allocation.  This data is passed to the driver for all
	// operations involving the allocated resource. This is opaque for
	// Kubernetes.  Driver documentation may explain to users how to
	// interpret this data if needed.
	//
	// The attributes must be sufficient to deallocate the resource because
	// the ResourceClass might not be available anymore when deallocation
	// starts.
	Attributes map[string]string

	// This field will get set by the resource driver after it has
	// allocated the resource driver to inform the scheduler where it can
	// schedule Pods using the ResourceClaim.
	//
	// A resource driver may already set this before the resource is
	// allocated. The scheduler will then check this field in addition to
	// UnsuitableNodes to filter out nodes where the resource cannot be
	// allocated.
	//
	// Setting this field is optional. If nil, the resource is available
	// everywhere.
	AvailableOnNodes *core.NodeSelector

	// UserLimit determines how many entities are allowed to use this
	// resource at the same time. The default is 1. -1 enables the usage by
	// an unlimited number of users. Individual containers in a pod are not
	// counted as users, only the Pod is.
	UserLimit int
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClaimList is a collection of resource classes.
type ResourceClaimList struct {
	metav1.TypeMeta
	// Standard list metadata
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ListMeta

	// Items is the list of resource claims.
	Items []ResourceClaim
}
