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
		isTimeout := false
		go func() {
			time.Sleep(time.Second * 20)
			isTimeout = true
		}()
		for {
			if isTimeout {
				break
			}
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
			})
			if err != nil {
				framework.Failf("error of ExecWithOptions: %v", err)
			}
			stdout_bytes := []byte(stdout)
			res := bytes.Compare(data, stdout_bytes)
			if res != 0 {
				framework.Failf("wrong stdout found:\nlen(data):\n%v\nlen(stdout):\n%v\n", len(data), len(stdout_bytes))
			}
		}
	})
})

func generateRandomBytes(bytes int) []byte {
	buf := make([]byte, bytes)

	if _, err := rand.Read(buf); err != nil {
		framework.Failf("error while generating random bytes: %s", err)
	}

	return buf
}
