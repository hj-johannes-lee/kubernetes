package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cdiv1alpha1 "k8s.io/api/cdi/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestController(t *testing.T) {
	claimKey := "default/claim"
	claimName := "claim"
	claimNamespace := "default"
	driverName := "mock-driver"
	className := "mock-class"
	otherDriverName := "other-driver"
	otherClassName := "other-class"
	ourFinalizer := driverName + "/deletion-protection"
	otherFinalizer := otherDriverName + "/deletion-protection"
	classes := []*cdiv1alpha1.ResourceClass{
		createClass(className, driverName),
		createClass(otherClassName, otherDriverName),
	}
	claim := createClaim(claimName, claimNamespace, className)
	otherClaim := createClaim(claimName, claimNamespace, otherClassName)
	delayedClaim := claim.DeepCopy()
	delayedClaim.Spec.AllocationMode = corev1.AllocationModeDelayed
	nodeName := "worker"
	otherNodeName := "worker-2"
	suitableNodes := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{
			{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{
						Key:      "name",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{nodeName},
					},
				},
			},
		},
	}
	unsuitableNodes := []string{nodeName}
	potentialNodes := []string{nodeName, otherNodeName}
	withDeletionTimestamp := func(claim *cdiv1alpha1.ResourceClaim) *cdiv1alpha1.ResourceClaim {
		var deleted metav1.Time
		claim = claim.DeepCopy()
		claim.DeletionTimestamp = &deleted
		return claim
	}
	withFinalizer := func(claim *cdiv1alpha1.ResourceClaim, finalizer string) *cdiv1alpha1.ResourceClaim {
		claim = claim.DeepCopy()
		claim.Finalizers = append(claim.Finalizers, finalizer)
		return claim
	}
	allocation := cdiv1alpha1.AllocationResult{
		UserLimit: 1,
	}
	withSelectedNode := func(claim *cdiv1alpha1.ResourceClaim) *cdiv1alpha1.ResourceClaim {
		claim = claim.DeepCopy()
		claim.Status.Scheduling.Scheduler.SelectedNode = nodeName
		return claim
	}
	withAllocate := func(claim *cdiv1alpha1.ResourceClaim) *cdiv1alpha1.ResourceClaim {
		// Any allocated claim must have our finalizer.
		claim = withFinalizer(claim, ourFinalizer)
		claim.Status.Allocation = allocation
		claim.Status.Phase = cdiv1alpha1.ResourceClaimAllocated
		return claim
	}
	withReallocate := func(claim *cdiv1alpha1.ResourceClaim) *cdiv1alpha1.ResourceClaim {
		claim = claim.DeepCopy()
		claim.Status.Phase = cdiv1alpha1.ResourceClaimReallocate
		return claim
	}
	withSuitableNodes := func(claim *cdiv1alpha1.ResourceClaim) *cdiv1alpha1.ResourceClaim {
		claim = claim.DeepCopy()
		claim.Status.Scheduling.Driver.SuitableNodes = suitableNodes
		return claim
	}
	withUnsuitableNodes := func(claim *cdiv1alpha1.ResourceClaim) *cdiv1alpha1.ResourceClaim {
		claim = claim.DeepCopy()
		claim.Status.Scheduling.Driver.UnsuitableNodes = unsuitableNodes
		return claim
	}
	withPotentialNodes := func(claim *cdiv1alpha1.ResourceClaim) *cdiv1alpha1.ResourceClaim {
		claim = claim.DeepCopy()
		claim.Status.Scheduling.Scheduler.PotentialNodes = potentialNodes
		return claim
	}

	var m mockDriver

	for name, test := range map[string]struct {
		key                  string
		driver               mockDriver
		classes              []*cdiv1alpha1.ResourceClass
		claim, expectedClaim *cdiv1alpha1.ResourceClaim
		expectedError        string
	}{
		"invalid-key": {
			key:           "x/y/z",
			expectedError: `unexpected key format: "x/y/z"`,
		},
		"not-found": {
			key: "default/claim",
		},
		"nop": {},
		"wrong-driver": {
			key:           claimKey,
			classes:       classes,
			claim:         otherClaim,
			expectedClaim: otherClaim,
			expectedError: errRequeue.Error(), // class might change
		},
		// Immediate allocation:
		// deletion time stamp set, our finalizer set, not allocated  -> remove finalizer
		"immediate-deleted-finalizer-removal": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withDeletionTimestamp(claim), ourFinalizer),
			driver:        m.expectStopAllocation(nil),
			expectedClaim: withDeletionTimestamp(claim),
		},
		// deletion time stamp set, our finalizer set, not allocated, stopping fails  -> requeue
		"immediate-deleted-finalizer-stop-failure": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withDeletionTimestamp(claim), ourFinalizer),
			driver:        m.expectStopAllocation(errors.New("fake error")),
			expectedClaim: withFinalizer(withDeletionTimestamp(claim), ourFinalizer),
			expectedError: "stop allocation: fake error",
		},
		// deletion time stamp set, other finalizer set, not allocated  -> do nothing
		"immediate-deleted-finalizer-no-removal": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withDeletionTimestamp(claim), otherFinalizer),
			expectedClaim: withFinalizer(withDeletionTimestamp(claim), otherFinalizer),
		},
		// deletion time stamp set, finalizer set, allocated  -> deallocate
		"immediate-deleted-allocated": {
			key:           claimKey,
			classes:       classes,
			claim:         withAllocate(withDeletionTimestamp(claim)),
			driver:        m.expectDeallocate(nil),
			expectedClaim: withDeletionTimestamp(claim),
		},
		// deletion time stamp set, finalizer set, allocated, deallocation fails  -> requeue
		"immediate-deleted-deallocate-failure": {
			key:           claimKey,
			classes:       classes,
			claim:         withAllocate(withDeletionTimestamp(claim)),
			driver:        m.expectDeallocate(errors.New("fake error")),
			expectedClaim: withAllocate(withDeletionTimestamp(claim)),
			expectedError: "deallocate: fake error",
		},
		// deletion time stamp set, finalizer not set -> do nothing
		"immediate-deleted-no-finalizer": {
			key:           claimKey,
			classes:       classes,
			claim:         withDeletionTimestamp(claim),
			expectedClaim: withDeletionTimestamp(claim),
		},
		// not deleted, not allocated, no finalizer -> add finalizer
		"immediate-prepare-allocation": {
			key:           claimKey,
			classes:       classes,
			claim:         claim,
			expectedClaim: withFinalizer(claim, ourFinalizer),
		},
		// not deleted, not allocated, finalizer -> allocate
		"immdiate-do-allocation": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(claim, ourFinalizer),
			driver:        m.expectAllocate(&allocation, nil),
			expectedClaim: withAllocate(claim),
		},
		// not deleted, not allocated, finalizer, fail allocation -> requeue
		"immediate-fail-allocation": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(claim, ourFinalizer),
			driver:        m.expectAllocate(nil, errors.New("fake error")),
			expectedClaim: withFinalizer(claim, ourFinalizer),
			expectedError: "allocate: fake error",
		},
		// not deleted, allocated -> do nothing
		"immediate-allocated-nop": {
			key:           claimKey,
			classes:       classes,
			claim:         withAllocate(claim),
			expectedClaim: withAllocate(claim),
		},
		// not deleted, reallocate -> deallocate
		"immediate-allocated-reallocate": {
			key:           claimKey,
			classes:       classes,
			claim:         withReallocate(withAllocate(claim)),
			driver:        m.expectDeallocate(nil),
			expectedClaim: claim,
		},
		// not deleted, reallocate, deallocate failure -> requeue
		"immediate-allocated-fail-deallocation-during-reallocate": {
			key:           claimKey,
			classes:       classes,
			claim:         withReallocate(withAllocate(claim)),
			driver:        m.expectDeallocate(errors.New("fake error")),
			expectedClaim: withReallocate(withAllocate(claim)),
			expectedError: "remove allocation: fake error",
		},

		// Delayed allocation is similar in some cases, but not quite
		// the same.
		// deletion time stamp set, our finalizer set, not allocated  -> remove finalizer
		"delayed-deleted-finalizer-removal": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withDeletionTimestamp(delayedClaim), ourFinalizer),
			driver:        m.expectStopAllocation(nil),
			expectedClaim: withDeletionTimestamp(delayedClaim),
		},
		// deletion time stamp set, our finalizer set, not allocated, stopping fails  -> requeue
		"delayed-deleted-finalizer-stop-failure": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withDeletionTimestamp(delayedClaim), ourFinalizer),
			driver:        m.expectStopAllocation(errors.New("fake error")),
			expectedClaim: withFinalizer(withDeletionTimestamp(delayedClaim), ourFinalizer),
			expectedError: "stop allocation: fake error",
		},
		// deletion time stamp set, other finalizer set, not allocated  -> do nothing
		"delayed-deleted-finalizer-no-removal": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withDeletionTimestamp(delayedClaim), otherFinalizer),
			expectedClaim: withFinalizer(withDeletionTimestamp(delayedClaim), otherFinalizer),
		},
		// deletion time stamp set, finalizer set, allocated  -> deallocate
		"delayed-deleted-allocated": {
			key:           claimKey,
			classes:       classes,
			claim:         withAllocate(withDeletionTimestamp(delayedClaim)),
			driver:        m.expectDeallocate(nil),
			expectedClaim: withDeletionTimestamp(delayedClaim),
			expectedError: errPeriodic.Error(),
		},
		// deletion time stamp set, finalizer set, allocated, deallocation fails  -> requeue
		"delayed-deleted-deallocate-failure": {
			key:           claimKey,
			classes:       classes,
			claim:         withAllocate(withDeletionTimestamp(delayedClaim)),
			driver:        m.expectDeallocate(errors.New("fake error")),
			expectedClaim: withAllocate(withDeletionTimestamp(delayedClaim)),
			expectedError: "deallocate: fake error",
		},
		// deletion time stamp set, finalizer not set -> do nothing
		"delayed-deleted-no-finalizer": {
			key:           claimKey,
			classes:       classes,
			claim:         withDeletionTimestamp(delayedClaim),
			expectedClaim: withDeletionTimestamp(delayedClaim),
		},
		// not deleted, not allocated, no selected node, no suitable node filter -> do nothing
		// TODO: check of UnsuitableNodes must be set or refreshed
		"delayed-nop": {
			key:           claimKey,
			classes:       classes,
			claim:         delayedClaim,
			driver:        m.expectSuitableNodes(nil, nil),
			expectedClaim: delayedClaim,
			expectedError: errPeriodic.Error(),
		},
		// not deleted, not allocated, no selected node, suitable node filter -> add it
		"delayed-suitable-nodes": {
			key:           claimKey,
			classes:       classes,
			claim:         delayedClaim,
			driver:        m.expectSuitableNodes(suitableNodes, nil),
			expectedClaim: withSuitableNodes(delayedClaim),
			expectedError: errPeriodic.Error(),
		},
		// not deleted, not allocated, no selected node, suitable node filter -> add it
		"delayed-suitable-nodes-fail": {
			key:           claimKey,
			classes:       classes,
			claim:         delayedClaim,
			driver:        m.expectSuitableNodes(nil, errors.New("fake error")),
			expectedClaim: delayedClaim,
			expectedError: "check for suitable nodes: fake error",
		},
		// not deleted, not allocated, no selected node, potential nodes, no unsuitable nodes -> do nothing
		"delayed-unsuitable-nodes-nop": {
			key:           claimKey,
			classes:       classes,
			claim:         withPotentialNodes(delayedClaim),
			driver:        m.expectUnsuitableNodes(nil, nil).expectSuitableNodes(nil, nil),
			expectedClaim: withPotentialNodes(delayedClaim),
			expectedError: errPeriodic.Error(),
		},
		// not deleted, not allocated, no selected node, potential nodes, unsuitable nodes -> add them
		"delayed-unsuitable-nodes-add": {
			key:           claimKey,
			classes:       classes,
			claim:         withPotentialNodes(delayedClaim),
			driver:        m.expectUnsuitableNodes(sets.NewString(unsuitableNodes...), nil).expectSuitableNodes(nil, nil),
			expectedClaim: withUnsuitableNodes(withPotentialNodes(delayedClaim)),
			expectedError: errPeriodic.Error(),
		},
		// not deleted, not allocated, no selected node, potential nodes, unsuitable nodes fails -> error
		"delayed-unsuitable-nodes-fail": {
			key:           claimKey,
			classes:       classes,
			claim:         withPotentialNodes(delayedClaim),
			driver:        m.expectUnsuitableNodes(nil, errors.New("fake error")).expectSuitableNodes(nil, nil),
			expectedClaim: withPotentialNodes(delayedClaim),
			expectedError: "check for unsuitable nodes: fake error",
		},
		// not deleted, not allocated, selected node, no finalizer -> add finalizer
		"delayed-prepare-allocation": {
			key:           claimKey,
			classes:       classes,
			claim:         withSelectedNode(delayedClaim),
			expectedClaim: withFinalizer(withSelectedNode(delayedClaim), ourFinalizer),
		},
		// not deleted, not allocated, selected node, finalizer -> allocate
		"delayed-do-allocation": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withSelectedNode(delayedClaim), ourFinalizer),
			driver:        m.expectAllocate(&allocation, nil),
			expectedClaim: withAllocate(delayedClaim),
		},
		// not deleted, not allocated, selected node, finalizer, fail allocation -> requeue
		"delayed-fail-allocation": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withSelectedNode(delayedClaim), ourFinalizer),
			driver:        m.expectAllocate(nil, errors.New("fake error")),
			expectedClaim: withFinalizer(withSelectedNode(delayedClaim), ourFinalizer),
			expectedError: "allocate: fake error",
		},
		// not deleted, allocated -> do nothing
		"delayed-allocated-nop": {
			key:           claimKey,
			classes:       classes,
			claim:         withAllocate(delayedClaim),
			expectedClaim: withAllocate(delayedClaim),
		},
		// not deleted, reallocate -> deallocate
		"delayed-allocated-reallocate": {
			key:           claimKey,
			classes:       classes,
			claim:         withReallocate(withAllocate(delayedClaim)),
			driver:        m.expectDeallocate(nil),
			expectedClaim: delayedClaim,
			expectedError: errPeriodic.Error(),
		},
		// not deleted, reallocate, deallocate failure -> requeue
		"delayed-allocated-fail-deallocation-during-reallocate": {
			key:           claimKey,
			classes:       classes,
			claim:         withReallocate(withAllocate(delayedClaim)),
			driver:        m.expectDeallocate(errors.New("fake error")),
			expectedClaim: withReallocate(withAllocate(delayedClaim)),
			expectedError: "remove allocation: fake error",
		},

		// unset selected node
		"delayed-unselect-node": {
			key:           claimKey,
			classes:       classes,
			claim:         withFinalizer(withSelectedNode(delayedClaim), ourFinalizer),
			driver:        m.expectAllocate(nil, ErrReschedule).expectSuitableNodes(suitableNodes, nil),
			expectedClaim: withSuitableNodes(withFinalizer(delayedClaim, ourFinalizer)),
			expectedError: errPeriodic.Error(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			initialObjects := []runtime.Object{}
			for _, class := range test.classes {
				initialObjects = append(initialObjects, class)
			}
			if test.claim != nil {
				initialObjects = append(initialObjects, test.claim)
			}
			kubeClient, informerFactory := fakeK8s(initialObjects)
			rcInformer := informerFactory.Cdi().V1alpha1().ResourceClasses()
			claimInformer := informerFactory.Cdi().V1alpha1().ResourceClaims()

			for _, obj := range initialObjects {
				switch obj.(type) {
				case *cdiv1alpha1.ResourceClass:
					rcInformer.Informer().GetStore().Add(obj)
				case *cdiv1alpha1.ResourceClaim:
					claimInformer.Informer().GetStore().Add(obj)
				default:
					t.Fatalf("unknown initalObject type: %+v", obj)
				}
			}

			driver := test.driver
			driver.t = t

			ctrl := New(driverName, driver, kubeClient, informerFactory)
			err := ctrl.(*controller).syncKey(context.TODO(), test.key)
			if err != nil && test.expectedError == "" {
				t.Fatalf("unexpected error: %v", err)
			}
			if err == nil && test.expectedError != "" {
				t.Fatalf("did not get expected error %q", test.expectedError)
			}
			claims, err := kubeClient.CdiV1alpha1().ResourceClaims("").List(ctx, metav1.ListOptions{})
			require.NoError(t, err, "list claims")
			var expectedClaims []cdiv1alpha1.ResourceClaim
			if test.expectedClaim != nil {
				expectedClaims = append(expectedClaims, *test.expectedClaim)
			}
			assert.Equal(t, expectedClaims, claims.Items)
		})
	}
}

func asClaim(t *testing.T, objs []interface{}) *cdiv1alpha1.ResourceClaim {
	switch len(objs) {
	case 0:
		return nil
	case 1:
		return objs[0].(*cdiv1alpha1.ResourceClaim)
	default:
		t.Helper()
		t.Fatalf("expected at most one ResourceClaim, got: %v", objs)
		return nil
	}
}

func (m mockDriver) expectAllocate(result *cdiv1alpha1.AllocationResult, err error) mockDriver {
	m.allocate = true
	m.allocResult = result
	m.allocErr = err
	return m
}

func (m mockDriver) expectStopAllocation(err error) mockDriver {
	m.stopAllocation = true
	m.stopAllocErr = err
	return m
}

func (m mockDriver) expectDeallocate(err error) mockDriver {
	m.deallocate = true
	m.deallocErr = err
	return m
}

func (m mockDriver) expectSuitableNodes(suitableNodes *v1.NodeSelector, err error) mockDriver {
	m.suitableNodes = true
	m.suitableNodesResult = suitableNodes
	m.suitableNodesErr = err
	return m
}

func (m mockDriver) expectUnsuitableNodes(unsuitableNodes sets.String, err error) mockDriver {
	m.unsuitableNodes = true
	m.unsuitableNodesResult = unsuitableNodes
	m.unsuitableNodesErr = err
	return m
}

type mockDriver struct {
	t *testing.T

	allocate    bool
	allocResult *cdiv1alpha1.AllocationResult
	allocErr    error

	deallocate bool
	deallocErr error

	stopAllocation bool
	stopAllocErr   error

	suitableNodes       bool
	suitableNodesResult *v1.NodeSelector
	suitableNodesErr    error

	unsuitableNodes       bool
	unsuitableNodesResult sets.String
	unsuitableNodesErr    error
}

func (m mockDriver) Allocate(ctx context.Context, claim *cdiv1alpha1.ResourceClaim, class *cdiv1alpha1.ResourceClass) (*cdiv1alpha1.AllocationResult, error) {
	m.t.Logf("Allocate(%s)", claim)
	if !m.allocate {
		m.t.Fatal("unexpected Allocate call")
	}
	return m.allocResult, m.allocErr
}

func (m mockDriver) StopAllocation(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	m.t.Logf("StopAllocation(%s)", claim)
	if !m.stopAllocation {
		m.t.Fatal("unexpected StopAllocation call")
	}
	return m.stopAllocErr
}

func (m mockDriver) Deallocate(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	m.t.Logf("Dellocate(%v)", claim)
	if !m.deallocate {
		m.t.Fatal("unexpected Deallocate call")
	}
	return m.deallocErr
}

func (m mockDriver) SuitableNodes(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) (*v1.NodeSelector, error) {
	m.t.Logf("SuitableNodes(%s)", claim)
	if !m.suitableNodes {
		m.t.Fatal("unexpected SuitableNodes call")
	}
	return m.suitableNodesResult, m.suitableNodesErr
}

func (m mockDriver) UnsuitableNodes(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) (sets.String, error) {
	m.t.Logf("UnsuitableNodes(%s)", claim)
	if !m.unsuitableNodes {
		m.t.Fatal("unexpected UnsuitableNodes call")
	}
	return m.unsuitableNodesResult, m.unsuitableNodesErr
}

func createClass(className, driverName string) *cdiv1alpha1.ResourceClass {
	return &cdiv1alpha1.ResourceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
		DriverName: driverName,
	}
}

func createClaim(claimName, claimNamespace, className string) *cdiv1alpha1.ResourceClaim {
	return &cdiv1alpha1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: claimNamespace,
		},
		Spec: corev1.ResourceClaimSpec{
			ResourceClassName: className,
			AllocationMode:    corev1.AllocationModeImmediate,
		},
		Status: cdiv1alpha1.ResourceClaimStatus{
			Phase: cdiv1alpha1.ResourceClaimPending,
		},
	}
}

func fakeK8s(objs []runtime.Object) (kubernetes.Interface, informers.SharedInformerFactory) {
	// This is a very simple replacement for a real apiserver. For example,
	// it doesn't do defaulting and accepts updates to the status in normal
	// Update calls. Therefore this test does not catch when we use Update
	// instead of UpdateStatus. Reactors could be used to catch that, but
	// that seems overkill because E2E tests will find that.
	//
	// Interactions with the fake apiserver also never fail. TODO:
	// simulate update errors.
	client := fake.NewSimpleClientset(objs...)
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	return client, informerFactory
}
