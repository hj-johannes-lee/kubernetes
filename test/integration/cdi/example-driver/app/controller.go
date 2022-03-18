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

// Package app does all of the work necessary to configure and run a
// Kubernetes app process.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	cdiv1alpha1 "k8s.io/api/cdi/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	"k8s.io/kubernetes/test/integration/cdi/example-driver/controller"
)

func runController(ctx context.Context, clientset kubernetes.Interface, driverName string, workers int) {
	driver := &exampleDriver{}
	informerFactory := informers.NewSharedInformerFactory(clientset, 0 /* resync period */)
	ctrl := controller.New(driverName, driver, clientset, informerFactory)
	informerFactory.Start(ctx.Done())
	ctrl.Run(ctx, workers)
}

type exampleDriver struct{}
type parameters map[string]string

var _ controller.Driver = exampleDriver{}

// Allocate simply copies parameters as env variables into the result.
func (e exampleDriver) Allocate(ctx context.Context, claim *cdiv1alpha1.ResourceClaim, class *cdiv1alpha1.ResourceClass) (*cdiv1alpha1.AllocationResult, error) {
	allocation := &cdiv1alpha1.AllocationResult{
		UserLimit: -1,
	}
	if err := toEnvVars("user", claim.Spec.Parameters, &allocation.Attributes); err != nil {
		return nil, fmt.Errorf("ResourceClaim.Spec.Parameters: %v", err)
	}
	if err := toEnvVars("admin", class.Parameters, &allocation.Attributes); err != nil {
		return nil, fmt.Errorf("%s: ResourceClass.Parameters: %v", class.Name, err)
	}
	return allocation, nil
}

func toEnvVars(what string, in runtime.RawExtension, out *map[string]string) error {
	var params parameters
	if err := json.Unmarshal(in.Raw, &params); err != nil {
		return fmt.Errorf("parse %s parameters: %v", what, err)
	}
	for key, value := range params {
		if *out == nil {
			*out = map[string]string{}
		}
		(*out)[what+"_"+strings.ToLower(key)] = value
	}
	return nil
}

func (e exampleDriver) StopAllocation(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	return nil
}

func (e exampleDriver) Deallocate(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) error {
	return nil
}

func (e exampleDriver) SuitableNodes(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) (*v1.NodeSelector, error) {
	return nil, nil
}

func (e exampleDriver) UnsuitableNodes(ctx context.Context, claim *cdiv1alpha1.ResourceClaim) (sets.String, error) {
	return nil, nil
}
