/*
Copyright 2023 The Kubernetes Authors.

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

package apimachinery

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edebug "k8s.io/kubernetes/test/e2e/framework/debug"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = SIGDescribe("Pod exec", func() {

	f := framework.NewDefaultFramework("execwithoptions-stress")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var pod *v1.Pod
	//	var podYaml string

	ginkgo.BeforeEach(func(ctx context.Context) {
		ginkgo.By("creating a pod")

		// Create pod to attach Volume to Node
		var err error
		pod, err = e2epod.CreatePod(ctx, f.ClientSet, f.Namespace.Name, nil, nil, false, "")
		if err != nil {
			framework.Failf("unable to create pod: %v", err)
		}

		ginkgo.By("waiting for busybox's availability")
		err = e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
		if err != nil {
			e2edebug.DumpAllNamespaceInfo(ctx, f.ClientSet, f.Namespace.Name)
			framework.Failf("unable to wait for busybox pod to be running and ready: %v", err)
		}
	})

	ginkgo.It("works under load", func(ctx context.Context) {
		start := time.Now()
		duration := 3 * time.Minute
		workers := 20

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func(worker int) {
				ginkgo.By(fmt.Sprintf("Worker #%d started.", worker))
				defer wg.Done()
				defer ginkgo.GinkgoRecover()
				defer func() {
					// Here we detect failures, do
					// something, then pass that failure on
					// to GinkgoRecover.
					if r := recover(); r != nil {
						// Notify other workers that they can stop prematurely.
						cancel()
						ginkgo.By(fmt.Sprintf("Worker #%d failed.", worker))
						panic(r)
					}
					ginkgo.By(fmt.Sprintf("Worker #%d completed successfully.", worker))
				}()

				// Busy loop and check as often as possible
				// during the entire test runtime until the
				// time runs out, the test gets interrupted
				// (parent context), or some other worker fails
				// (our context).
				for i := 0; time.Now().Sub(start) <= duration && ctx.Err() == nil; i++ {
					data := generateRandomBytes(102400)
					stdout, _, err := e2epod.ExecWithOptions(f, e2epod.ExecOptions{
						Command:            []string{"cat", "-"},
						Namespace:          f.Namespace.Name,
						PodName:            pod.Name,
						ContainerName:      "write-pod",
						Stdin:              bytes.NewBuffer(data),
						CaptureStdout:      true,
						CaptureStderr:      true,
						PreserveWhitespace: false,
						Quiet:              true, // Avoid dumping this struct, which would include all of the stdin buffer..
					})
					if err != nil {
						framework.Failf("attempt #%d in worker #%d: error of ExecWithOptions: %v", i, worker, err)
					}
					stdout_bytes := []byte(stdout)
					res := bytes.Compare(data, stdout_bytes)
					if res != 0 {
						framework.Failf("attempt #%d in worker #%d: wrong stdout found:\nlen(data):\n%v\nlen(stdout):\n%v\n", i, worker, len(data), len(stdout_bytes))
					}
				}
			}(i)
		}

		// Wait for all workers to succeed or fail.
		wg.Wait()
	})
})

func generateRandomBytes(bytes int) []byte {
	buf := make([]byte, bytes)

	if _, err := rand.Read(buf); err != nil {
		framework.Failf("error while generating random bytes: %s", err)
	}

	return buf
}
