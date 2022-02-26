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

package fuzzer

import (
	"fmt"

	fuzz "github.com/google/gofuzz"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/cdi"
)

// Funcs returns the fuzzer functions for the cdi api group.
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(obj *cdi.ResourceClass, c fuzz.Continue) {
			c.FuzzNoCustom(obj) // fuzz self without calling this function again
		},
		func(obj *cdi.ResourceClaim, c fuzz.Continue) {
			c.FuzzNoCustom(obj) // fuzz self without calling this function again

			// Custom fuzzing for allocation mode.
			modes := []cdi.AllocationMode{
				cdi.AllocationMode(fmt.Sprintf("%d", c.Rand.Int31())),
				cdi.AllocationModeImmediate,
				cdi.AllocationModeDelayed,
			}
			obj.Spec.AllocationMode = modes[c.Rand.Intn(len(modes))]

			// Custom fuzzing for phase.
			phases := []cdi.ResourceClaimPhase{
				cdi.ResourceClaimPhase(fmt.Sprintf("%d", c.Rand.Int31())),
				cdi.ResourceClaimPending,
				cdi.ResourceClaimAllocated,
			}
			obj.Status.Phase = phases[c.Rand.Intn(len(phases))]
		},
	}
}
