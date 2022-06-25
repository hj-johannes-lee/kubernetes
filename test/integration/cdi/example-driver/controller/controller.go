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

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	cdiv1alpha1 "k8s.io/api/cdi/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	cdiv1alpha1listers "k8s.io/client-go/listers/cdi/v1alpha1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-helpers/cdi/resourceclaim"
	"k8s.io/klog/v2"
	schedulerapi "k8s.io/kube-scheduler/extender/v1"
)

// Controller watches ResourceClaims and triggers allocation and deallocation
// as needed.
type Controller interface {
	// Run starts the controller.
	Run(ctx context.Context, workers int)

	// Filter implements the scheduler extender filter verb.
	Filter(http.ResponseWriter, *http.Request)
}

var ErrReschedule = errors.New("reschedule")

// Driver provides the actual allocation and deallocation operations.
type Driver interface {
	// Allocate gets called when a ResourceClaim is ready to be allocated.
	// The SelectedNodeName is empty for ResourceClaims with immediate
	// allocation, in which case the resource driver decides itself where
	// to allocate.
	//
	// If SelectedNodeName is set, the driver must attempt to allocated for
	// that node. If that is not possible, it must return RescheduleErr.
	//
	// The objects are read-only and must not be modified. This call
	// must be idempotent.
	Allocate(ctx context.Context, claim *cdiv1alpha1.ResourceClaim, class *cdiv1alpha1.ResourceClass) (*cdiv1alpha1.AllocationResult, error)

	// StopAllocation must check whether there is an ongoing allocation for
	// the ResourceClaim. It gets called when the controller is unsure
	// if Allocate was called before without recording the outcome
	// and now the ResourceClaim is marked for deletion.
	//
	// The resource driver then must abort the ongoing allocation
	// (if there is one). It may return an error to indicate that
	// StopAllocation must be called again at a later time.
	// It must return nil when it is okay to proceed with deleting
	// the claim.
	StopAllocation(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error

	// Deallocate gets called when a ResourceClaim is ready to be
	// freed.
	//
	// The claim is read-only and must not be modified. This call
	// must be idempotent.
	Deallocate(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error

	// SuitableNodes returns a node filter that will get copied into
	// Scheduling.Driver.SuitableNodes. It gets called periodically
	// for pending resource claims with delayed allocation. Ideally it
	// should return a static filter by label which then only needs to be
	// set once. Filtering by node name should be done in UnsuitableNodes,
	// after the scheduler has reduced the set of suitable nodes.
	//
	// Returning nil indicates that all nodes are potential candidates.
	SuitableNodes(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) (*v1.NodeSelector, error)

	// UnsuitableNodes returns a list of unsuitable node names for
	// Scheduling.Driver.UnsuitableNodes. It gets called only if the
	// scheduler has already set Scheduling.Scheduler.PotentialNodes. The
	// driver should focus on those nodes in its response.
	UnsuitableNodes(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) (sets.String, error)
}

type controller struct {
	name          string
	finalizer     string
	driver        Driver
	kubeClient    kubernetes.Interface
	claimQueue    workqueue.RateLimitingInterface
	eventRecorder record.EventRecorder
	rcLister      cdiv1alpha1listers.ResourceClassLister
	rcSynced      cache.InformerSynced
	claimLister   cdiv1alpha1listers.ResourceClaimLister
	claimSynced   cache.InformerSynced
}

// TODO: make it configurable
var recheckDelay = 30 * time.Second

// New creates a new controller.
func New(
	name string,
	driver Driver,
	kubeClient kubernetes.Interface,
	informerFactory informers.SharedInformerFactory) Controller {
	rcInformer := informerFactory.Cdi().V1alpha1().ResourceClasses()
	claimInformer := informerFactory.Cdi().V1alpha1().ResourceClaims()

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events(v1.NamespaceAll)})
	eventRecorder := eventBroadcaster.NewRecorder(scheme.Scheme,
		v1.EventSource{Component: fmt.Sprintf("resource driver %s", name)})

	claimQueue := workqueue.NewNamedRateLimitingQueue(
		workqueue.DefaultControllerRateLimiter(), fmt.Sprintf("%s-claims", name))

	ctrl := &controller{
		name:          name,
		finalizer:     name + "/deletion-protection",
		driver:        driver,
		kubeClient:    kubeClient,
		rcLister:      rcInformer.Lister(),
		rcSynced:      rcInformer.Informer().HasSynced,
		claimLister:   claimInformer.Lister(),
		claimSynced:   claimInformer.Informer().HasSynced,
		claimQueue:    claimQueue,
		eventRecorder: eventRecorder,
	}

	claimInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.addClaim,
		UpdateFunc: ctrl.updateClaim,
		DeleteFunc: ctrl.deleteClaim,
	})

	return ctrl
}

func (ctrl *controller) addClaim(obj interface{}) {
	objKey, err := getClaimKey(obj)
	if err != nil {
		return
	}
	ctrl.claimQueue.Add(objKey)
}

func (ctrl *controller) updateClaim(oldObj, newObj interface{}) {
	ctrl.addClaim(newObj)
}

func (ctrl *controller) deleteClaim(obj interface{}) {
	objKey, err := getClaimKey(obj)
	if err != nil {
		return
	}
	ctrl.claimQueue.Forget(objKey)
}

func getClaimKey(obj interface{}) (string, error) {
	objKey, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("Failed to get key from %T object %v: %v", obj, obj, err)
		return "", err
	}
	return objKey, nil
}

// Run starts the controller.
func (ctrl *controller) Run(ctx context.Context, workers int) {
	defer ctrl.claimQueue.ShutDown()

	klog.Infof("Starting resource driver %s", ctrl.name)
	defer klog.Infof("Shutting down resource driver %s", ctrl.name)

	stopCh := ctx.Done()

	if !cache.WaitForCacheSync(stopCh, ctrl.rcSynced, ctrl.claimSynced) {
		klog.ErrorS(nil, "Cannot sync caches")
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(func() { ctrl.syncClaims(ctx) }, 0, stopCh)
	}

	<-stopCh
}

// Filter implements the scheduler extender filter verb.
func (ctrl *controller) Filter(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// From https://github.com/Huang-Wei/sample-scheduler-extender/blob/047fdd5ae8b1a6d7fdc0e6d20ce4d70a1d6e7178/routers.go#L19-L39
	var args schedulerapi.ExtenderArgs
	var result *schedulerapi.ExtenderFilterResult
	err := json.NewDecoder(r.Body).Decode(&args)
	if err == nil {
		result, err = ctrl.doFilter(ctx, args)
	}

	// Always try to write a resonable response.
	if result == nil && err != nil {
		result = &schedulerapi.ExtenderFilterResult{
			Error: err.Error(),
		}
	}
	if response, err := json.Marshal(result); err != nil {
		klog.ErrorS(err, "JSON encoding")
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(response)
	}
}

func (ctrl *controller) doFilter(ctx context.Context, args schedulerapi.ExtenderArgs) (*schedulerapi.ExtenderFilterResult, error) {
	if args.Pod == nil ||
		args.Pod.Name == "" ||
		(args.NodeNames == nil && args.Nodes == nil) {
		return nil, errors.New("incomplete parameters")
	}
	pod := args.Pod

	// TODO: contextual logging: log := s.log.WithValues("pod", pod.Name)
	klog.V(5).InfoS("node filter", "request", args)

	potentialNodes := sets.NewString()
	if args.NodeNames != nil {
		potentialNodes.Insert((*args.NodeNames)...)
	} else {
		// Fallback for Extender.NodeCacheCapable == false:
		// not recommended, but may be used by users anyway.
		klog.InfoS("NodeCacheCapable is false in Extender configuration, should be set to true.")
		for _, node := range args.Nodes.Items {
			potentialNodes.Insert(node.Name)
		}
	}

	// We need to check all claims that this driver would allocate via
	// delayed allocation. We don't need to check allocated claims or
	// claims with immediate allocation because those have enough
	// information to be handled by the builtin dynamic resources plugin.
	failedNodes := map[string][]string{}
	for _, resource := range pod.Spec.ResourceClaims {
		claimName := resourceclaim.Name(pod, &resource)
		isEphemeral := resource.ResourceClaimName == nil
		claim, err := ctrl.claimLister.ResourceClaims(pod.Namespace).Get(claimName)
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

		if claim.Status.Phase != cdiv1alpha1.ResourceClaimPending ||
			claim.Spec.AllocationMode != v1.AllocationModeDelayed {
			continue
		}

		class, err := ctrl.rcLister.Get(claim.Spec.ResourceClassName)
		if err != nil {
			return nil, err
		}
		if class.DriverName != ctrl.name {
			continue
		}

		// Inject the current list of potential nodes, i.e. pretend that
		// the scheduler used communication through the API server.
		claim = claim.DeepCopy()
		claim.Status.Scheduling.Scheduler.PotentialNodes = potentialNodes.List()
		unsuitableNodes, err := ctrl.driver.UnsuitableNodes(ctx, claim)
		if err != nil {
			return nil, fmt.Errorf("checking for unsuitable nodes: %v", err)
		}

		// Rememeber for which nodes the claim wasn't suitable
		// and continue without those.
		for _, unsuitableNode := range unsuitableNodes.List() {
			potentialNodes.Delete(unsuitableNode)
			claims := failedNodes[unsuitableNode]
			claims = append(claims, claim.Name)
			failedNodes[unsuitableNode] = claims
		}
	}

	response := &schedulerapi.ExtenderFilterResult{
		FailedNodes: schedulerapi.FailedNodesMap{},
	}
	for nodeName, claimNames := range failedNodes {
		response.FailedNodes[nodeName] = "unsuitable for claim(s) " + strings.Join(claimNames, ", ")
	}
	if args.NodeNames != nil {
		remainingNodes := potentialNodes.List()
		response.NodeNames = &remainingNodes
	} else {
		// fallback response...
		response.Nodes = &v1.NodeList{}
		for nodeName := range potentialNodes {
			response.Nodes.Items = append(response.Nodes.Items, getNode(args.Nodes.Items, nodeName))
		}
	}
	klog.V(5).InfoS("node filter", "response", response)
	return response, nil
}

func getNode(nodes []v1.Node, nodeName string) v1.Node {
	for _, node := range nodes {
		if node.Name == nodeName {
			return node
		}
	}
	return v1.Node{}
}

// errRequeue is a special error instance that functions can return
// to request silent requeueing (not logged as error, no event).
// Uses exponential backoff.
var errRequeue = errors.New("requeue")

// errPeriodic is a special error instance that functions can return
// to request silent instance that functions can return
// to request silent retrying at a fixed rate.
var errPeriodic = errors.New("periodic")

// syncClaims is the main worker.
func (ctrl *controller) syncClaims(ctx context.Context) {
	key, quit := ctrl.claimQueue.Get()
	if quit {
		return
	}
	defer ctrl.claimQueue.Done(key)

	if err := ctrl.syncKey(ctx, key.(string)); err != nil {
		// Put ResourceClaim back to the queue so that we can retry later.
		switch err {
		case errRequeue:
			klog.V(5).InfoS("requeue", "claimKey", key)
			ctrl.claimQueue.AddRateLimited(key)
		case errPeriodic:
			klog.V(5).InfoS("recheck periodically", "claimKey", key)
			ctrl.claimQueue.AddAfter(key, recheckDelay)
		default:
			klog.ErrorS(err, "processing failed", "claimKey", key)
			ctrl.claimQueue.AddRateLimited(key)
		}
	} else {
		ctrl.claimQueue.Forget(key)
	}
}

// syncKey looks up a ResourceClaim by its key and processes it.
func (ctrl *controller) syncKey(ctx context.Context, key string) error {
	klog.V(4).Infof("Started ResourceClaim processing %q", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	claim, err := ctrl.claimLister.ResourceClaims(namespace).Get(name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			klog.V(4).Infof("ResourceClaim %s/%s is deleted, no need to process it", namespace, name)
			return nil
		}
		return err
	}
	if err := ctrl.syncClaim(ctx, claim); err != nil {
		if err != errRequeue && err != errPeriodic {
			// TODO: We don't know here *what* failed. Determine based on error?
			ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, "Failed", err.Error())
		}
		return err
	}
	return nil
}

// syncClaim determines which next action may be needed for a ResourceClaim
// and does it.
func (ctrl *controller) syncClaim(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	switch claim.Status.Phase {
	case cdiv1alpha1.ResourceClaimPending:
		return ctrl.syncPendingClaim(ctx, claim)
	case cdiv1alpha1.ResourceClaimAllocated:
		return ctrl.syncAllocatedClaim(ctx, claim)
	case cdiv1alpha1.ResourceClaimReallocate:
		return ctrl.deallocateClaim(ctx, claim)
	default:
		return fmt.Errorf("unsupported ResourceClaim.Status.Phase: %v", claim.Status.Phase)
	}

	return nil
}

func (ctrl *controller) syncPendingClaim(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	if claim.DeletionTimestamp != nil {
		// Marked for deletion. We might have our finalizer set. The
		// finalizer is specific to the driver, therefore we know that
		// this claim is "ours" when the finalizer is set.
		if ctrl.hasFinalizer(claim) {
			// We need to remove it. We can only do that when there
			// is no ongoing allocation.
			if err := ctrl.driver.StopAllocation(ctx, claim); err != nil {
				return fmt.Errorf("stop allocation: %v", err)
			}
			claim = claim.DeepCopy()
			claim.Finalizers = ctrl.removeFinalizer(claim.Finalizers)
			if _, err := ctrl.kubeClient.CdiV1alpha1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("remove finalizer: %v", err)
			}
		}

		// Nothing to do. The apiserver should remove it shortly.
		return nil
	}

	// We need the ResourceClass to determine whether we should work on it.
	class, err := ctrl.rcLister.Get(claim.Spec.ResourceClassName)
	if err != nil {
		return err
	}
	if class.DriverName != ctrl.name {
		// Not ours *at the moment*. This can change, so requeue and
		// check again. We could trigger a faster check when the
		// ResourceClass changes, but that shouldn't occur much in
		// practice and thus isn't worth the effort.
		//
		// We use exponential backoff because it is unlikely that
		// the ResourceClass changes much.
		return errRequeue
	}

	// Ready for allocation?
	if claim.Spec.AllocationMode == v1.AllocationModeImmediate ||
		claim.Spec.AllocationMode == v1.AllocationModeDelayed &&
			claim.Status.Scheduling.Scheduler.SelectedNode != "" {
		if !ctrl.hasFinalizer(claim) {
			// Set finalizer before doing anything.
			claim = claim.DeepCopy()
			claim.Finalizers = append(claim.Finalizers, ctrl.finalizer)
			if _, err := ctrl.kubeClient.CdiV1alpha1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("add finalizer: %v", err)
			}
			return nil
		}

		allocation, err := ctrl.driver.Allocate(ctx, claim, class)
		if err != nil && err != ErrReschedule {
			return fmt.Errorf("allocate: %v", err)
		}
		if err == nil {
			claim = claim.DeepCopy()
			claim.Status.Allocation = *allocation
			claim.Status.Scheduling = cdiv1alpha1.SchedulingStatus{}
			claim.Status.Phase = cdiv1alpha1.ResourceClaimAllocated
			claim.Status.DriverName = class.DriverName
			if _, err := ctrl.kubeClient.CdiV1alpha1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("add allocation: %v", err)
			}
			return nil
		}
		// Fall through to potentially updating claim.Status.Scheduling.
	}

	if claim.Spec.AllocationMode == v1.AllocationModeImmediate {
		// Nothing to do until the claim gets updated.
		return nil
	}

	// Update scheduling information.
	claim = claim.DeepCopy()
	modified := false
	if claim.Status.Scheduling.Scheduler.SelectedNode != "" {
		// The allocation attempt above must have failed with ErrReschedule.
		claim.Status.Scheduling.Scheduler.SelectedNode = ""
		modified = true
	}
	suitableNodes, err := ctrl.driver.SuitableNodes(ctx, claim)
	if err != nil {
		return fmt.Errorf("checking for suitable nodes: %v", err)
	}
	if suitableNodes == nil && claim.Status.Scheduling.Driver.SuitableNodes != nil ||
		suitableNodes != nil && claim.Status.Scheduling.Driver.SuitableNodes == nil ||
		suitableNodes.String() != claim.Status.Scheduling.Driver.SuitableNodes.String() {
		claim.Status.Scheduling.Driver.SuitableNodes = suitableNodes
		modified = true
	}
	if len(claim.Status.Scheduling.Scheduler.PotentialNodes) > 0 {
		// This is expected produce a potentially long list, therefore this
		// is only done when scheduling of a pod has started.
		unsuitableNodes, err := ctrl.driver.UnsuitableNodes(ctx, claim)
		if err != nil {
			return fmt.Errorf("checking for unsuitable nodes: %v", err)
		}
		sorted := unsuitableNodes.List()
		if !stringsEqual(sorted, claim.Status.Scheduling.Driver.UnsuitableNodes) {
			claim.Status.Scheduling.Driver.UnsuitableNodes = sorted
			modified = true
		}
	}
	if modified {
		if _, err := ctrl.kubeClient.CdiV1alpha1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update driver scheduling status: %v", err)
		}
	}

	// We must keep the claim in our queue and keep updating the
	// UnsuitableNodes field.
	return errPeriodic
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (ctrl *controller) hasFinalizer(claim *cdiv1alpha1.ResourceClaim) bool {
	for _, finalizer := range claim.Finalizers {
		if finalizer == ctrl.finalizer {
			return true
		}
	}
	return false
}

func (ctrl *controller) removeFinalizer(in []string) []string {
	out := make([]string, 0, len(in))
	for _, finalizer := range in {
		if finalizer != ctrl.finalizer {
			out = append(out, finalizer)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (ctrl *controller) syncAllocatedClaim(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	// Ready for deallocation?
	if claim.DeletionTimestamp != nil {
		return ctrl.deallocateClaim(ctx, claim)
	}
	return nil
}

func (ctrl *controller) deallocateClaim(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	if err := ctrl.driver.Deallocate(ctx, claim); err != nil {
		return fmt.Errorf("deallocate: %v", err)
	}
	claim = claim.DeepCopy()
	claim.Finalizers = ctrl.removeFinalizer(claim.Finalizers)
	claim.Status.Allocation = cdiv1alpha1.AllocationResult{}
	claim.Status.Phase = cdiv1alpha1.ResourceClaimPending
	if _, err := ctrl.kubeClient.CdiV1alpha1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("remove allocation: %v", err)
	}
	if claim.Spec.AllocationMode == v1.AllocationModeImmediate {
		// Nothing to do until the claim gets updated.
		return nil
	}
	return errPeriodic
}
