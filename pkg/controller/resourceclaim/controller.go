/*
Copyright 2020 The Kubernetes Authors.

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

package resourceclaim

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	cdiv1alpha1 "k8s.io/api/cdi/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	cdiv1alpha1informers "k8s.io/client-go/informers/cdi/v1alpha1"
	corev1informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	cdiv1alpha1listers "k8s.io/client-go/listers/cdi/v1alpha1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-helpers/cdi/resourceclaim"
	"k8s.io/kubernetes/pkg/controller/resourceclaim/metrics"
)

const (
	// podResourceClaimIndex is the lookup name for the index function which indexes by pod ResourceClaim templates.
	podResourceClaimIndex = "pod-resource-claim-index"
)

// Controller creates ResourceClaims for ResourceClaimTemplates in a pod spec.
type Controller interface {
	Run(ctx context.Context, workers int)
}

type resourceClaimController struct {
	// kubeClient is the kube API client used by volumehost to communicate with
	// the API server.
	kubeClient clientset.Interface

	// claimLister is the shared ResourceClaim lister used to fetch and store ResourceClaim
	// objects from the API server. It is shared with other controllers and
	// therefore the ResourceClaim objects in its store should be treated as immutable.
	claimLister  cdiv1alpha1listers.ResourceClaimLister
	claimsSynced kcache.InformerSynced

	// podLister is the shared Pod lister used to fetch Pod
	// objects from the API server. It is shared with other controllers and
	// therefore the Pod objects in its store should be treated as immutable.
	podLister corev1listers.PodLister
	podSynced kcache.InformerSynced

	// podIndexer has the common PodResourceClaim indexer indexer installed To
	// limit iteration over pods to those of interest.
	podIndexer cache.Indexer

	// recorder is used to record events in the API server
	recorder record.EventRecorder

	queue workqueue.RateLimitingInterface
}

// NewController creates a ResourceClaim controller.
func NewController(
	kubeClient clientset.Interface,
	podInformer corev1informers.PodInformer,
	claimInformer cdiv1alpha1informers.ResourceClaimInformer) (Controller, error) {

	ec := &resourceClaimController{
		kubeClient:   kubeClient,
		podLister:    podInformer.Lister(),
		podIndexer:   podInformer.Informer().GetIndexer(),
		podSynced:    podInformer.Informer().HasSynced,
		claimLister:  claimInformer.Lister(),
		claimsSynced: claimInformer.Informer().HasSynced,
		queue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "resource_claim"),
	}

	metrics.RegisterMetrics()

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	ec.recorder = eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "resource_claim"})

	podInformer.Informer().AddEventHandler(kcache.ResourceEventHandlerFuncs{
		AddFunc: ec.enqueuePod,
		// The pod spec is immutable. Therefore the controller can ignore pod updates
		// because there cannot be any changes that have to be copied into the generated
		// ResourceClaim.
		// Deletion of the ResourceClaim is handled through the owner reference and garbage collection.
		// Therefore pod deletions also can be ignored.
	})
	claimInformer.Informer().AddEventHandler(kcache.ResourceEventHandlerFuncs{
		DeleteFunc: ec.onResourceClaimDelete,
	})
	if err := ec.podIndexer.AddIndexers(cache.Indexers{podResourceClaimIndex: podResourceClaimIndexFunc}); err != nil {
		return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
	}

	return ec, nil
}

func (ec *resourceClaimController) enqueuePod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}

	// Ignore pods which are already getting deleted.
	if pod.DeletionTimestamp != nil {
		return
	}

	for _, podClaim := range pod.Spec.ResourceClaims {
		if podClaim.Template != nil {
			// It has at least one inline resource, work on it.
			key, err := kcache.DeletionHandlingMetaNamespaceKeyFunc(pod)
			if err != nil {
				runtime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", pod, err))
				return
			}
			ec.queue.Add(key)
			break
		}
	}
}

func (ec *resourceClaimController) onResourceClaimDelete(obj interface{}) {
	claim, ok := obj.(*cdiv1alpha1.ResourceClaim)
	if !ok {
		return
	}

	// Someone deleted a ResourceClaim, either intentionally or
	// accidentally. If there is a pod referencing it because of
	// an inline resource, then we should re-create the ResourceClaim.
	// The common indexer does some prefiltering for us by
	// limiting the list to those pods which reference
	// the ResourceClaim.
	objs, err := ec.podIndexer.ByIndex(podResourceClaimIndex, fmt.Sprintf("%s/%s", claim.Namespace, claim.Name))
	if err != nil {
		runtime.HandleError(fmt.Errorf("listing pods from cache: %v", err))
		return
	}
	for _, obj := range objs {
		ec.enqueuePod(obj)
	}
}

func (ec *resourceClaimController) Run(ctx context.Context, workers int) {
	defer runtime.HandleCrash()
	defer ec.queue.ShutDown()

	klog.Infof("Starting ephemeral volume controller")
	defer klog.Infof("Shutting down ephemeral volume controller")

	if !cache.WaitForNamedCacheSync("ephemeral", ctx.Done(), ec.podSynced, ec.claimsSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, ec.runWorker, time.Second)
	}

	<-ctx.Done()
}

func (ec *resourceClaimController) runWorker(ctx context.Context) {
	for ec.processNextWorkItem(ctx) {
	}
}

func (ec *resourceClaimController) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := ec.queue.Get()
	if shutdown {
		return false
	}
	defer ec.queue.Done(key)

	err := ec.syncHandler(ctx, key.(string))
	if err == nil {
		ec.queue.Forget(key)
		return true
	}

	runtime.HandleError(fmt.Errorf("%v failed with: %v", key, err))
	ec.queue.AddRateLimited(key)

	return true
}

// syncHandler is invoked for each pod which might need to be processed.
// If an error is returned from this function, the pod will be requeued.
func (ec *resourceClaimController) syncHandler(ctx context.Context, key string) error {
	namespace, name, err := kcache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	pod, err := ec.podLister.Pods(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(5).Infof("nothing to do for pod %s, it is gone", key)
			return nil
		}
		klog.V(5).Infof("Error getting pod %s/%s (uid: %q) from informer : %v", pod.Namespace, pod.Name, pod.UID, err)
		return err
	}

	// Ignore pods which are already getting deleted.
	if pod.DeletionTimestamp != nil {
		klog.V(5).Infof("nothing to do for pod %s, it is marked for deletion", key)
		return nil
	}

	for _, podClaim := range pod.Spec.ResourceClaims {
		if err := ec.handleClaim(ctx, pod, podClaim); err != nil {
			ec.recorder.Event(pod, v1.EventTypeWarning, "ResourceClaimCreation", fmt.Sprintf("PodResourceClaim %s: %v", podClaim.Name, err))
			return fmt.Errorf("pod %s, PodResourceClaim %s: %v", key, podClaim.Name, err)
		}
	}

	return nil
}

// handleResourceClaim is invoked for each volume of a pod.
func (ec *resourceClaimController) handleClaim(ctx context.Context, pod *v1.Pod, podClaim v1.PodResourceClaim) error {
	klog.V(5).Infof("checking podClaim %s", podClaim.Name)
	if podClaim.Template == nil {
		return nil
	}

	claimName := resourceclaim.Name(pod, &podClaim)
	claim, err := ec.claimLister.ResourceClaims(pod.Namespace).Get(claimName)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if claim != nil {
		if err := resourceclaim.IsForPod(pod, claim); err != nil {
			return err
		}
		// Already created, nothing more to do.
		klog.V(5).Infof("podClaim %s: ResourceClaim %s already created", podClaim.Name, claimName)
		return nil
	}

	// Create the ResourceClaim with pod as owner.
	isTrue := true
	claim = &cdiv1alpha1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: claimName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "Pod",
					Name:               pod.Name,
					UID:                pod.UID,
					Controller:         &isTrue,
					BlockOwnerDeletion: &isTrue,
				},
			},
			Annotations: podClaim.Template.Annotations,
			Labels:      podClaim.Template.Labels,
		},
		Spec: podClaim.Template.Spec,
	}
	metrics.ResourceClaimCreateAttempts.Inc()
	_, err = ec.kubeClient.CdiV1alpha1().ResourceClaims(pod.Namespace).Create(ctx, claim, metav1.CreateOptions{})
	if err != nil {
		metrics.ResourceClaimCreateFailures.Inc()
		return fmt.Errorf("create ResourceClaim %s: %v", claimName, err)
	}
	return nil
}

// podResourceClaimIndexFunc id an index function that returns ResourceClaim keys (=
// namespace/name) for ResourceClaimTemplates in a given pod.
func podResourceClaimIndexFunc(obj interface{}) ([]string, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return []string{}, nil
	}
	keys := []string{}
	for _, podClaim := range pod.Spec.ResourceClaims {
		if podClaim.Template != nil {
			claimName := resourceclaim.Name(pod, &podClaim)
			keys = append(keys, fmt.Sprintf("%s/%s", pod.Namespace, claimName))
		}
	}
	return keys, nil
}
