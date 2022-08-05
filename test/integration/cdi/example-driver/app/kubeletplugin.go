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
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	cdiapi "github.com/container-orchestrated-devices/container-device-interface/pkg/cdi"
	specs "github.com/container-orchestrated-devices/container-device-interface/specs-go"
	"k8s.io/klog/v2"
	drapbv1 "k8s.io/kubernetes/pkg/kubelet/apis/dra/v1alpha1"
	plugin "k8s.io/kubernetes/test/integration/cdi/example-driver/kubeletplugin"
)

type pluginConfig struct {
	driverName             string
	draAddress             string
	pluginRegistrationPath string
}

type examplePlugin struct {
	config pluginConfig
}

// newExamplePlugin returns an initialized examplePlugin instance
func newExampleDriver(config pluginConfig) (*examplePlugin, error) {

	return (&examplePlugin{
		config: config,
	}), nil
}

var cdiVersion = "0.2.0"
var kind = "vendor.com/device"
var deviceName = "example"

func getEnvsSlice(str string) []string {
	// "var=val var2=val2" => [var=val var2=val2]
	if len(str) == 0 {
		return nil
	}
	return strings.Split(str, " ")
}

func getJsonFilePath(claimName string) string {
	return "/etc/cdi/" + claimName + "-vendor.json"
}

func validateResourceHandle(rh map[string]string) error {
	for key := range rh {
		if key != "user_env" {
			return fmt.Errorf("invalid key for ResourceHandle")
		}
	}
	return nil
}

func decodeResourceHandle(rh string) (map[string]string, error) {
	var decodedResourceHandle map[string]string
	if err := json.Unmarshal([]byte(rh), &decodedResourceHandle); err != nil {
		return nil, fmt.Errorf("failed to unmarshal resourceHandle: %v", err)
	}

	if err := validateResourceHandle(decodedResourceHandle); err != nil {
		return nil, fmt.Errorf("failed to validate resourceHandle: %v", err)
	}
	return decodedResourceHandle, nil
}

func (ex *examplePlugin) NodePrepareResource(ctx context.Context, req *drapbv1.NodePrepareResourceRequest) (*drapbv1.NodePrepareResourceResponse, error) {
	klog.Infof("NodePrepareResource is called: request: %+v", req)
	// create CDI Files

	// create spec
	klog.Infof("Creating CDI Files")

	// resourceHandle in the form of map[string]string
	decodedResourceHandle, err := decodeResourceHandle(req.ResourceHandle)
	if err != nil {
		klog.Infof(err.Error())
		return nil, fmt.Errorf("failed to decode: %v", err)
	}

	containerEditsInDevices := (&cdiapi.ContainerEdits{}).Append(&cdiapi.ContainerEdits{ContainerEdits: &specs.ContainerEdits{}})
	if env := getEnvsSlice(decodedResourceHandle["user_env"]); env != nil {
		containerEditsInDevices.Append(&cdiapi.ContainerEdits{ContainerEdits: &specs.ContainerEdits{
			Env: env,
		}})
	}

	containerEdits := (&cdiapi.ContainerEdits{}).Append(&cdiapi.ContainerEdits{ContainerEdits: &specs.ContainerEdits{}})

	spec, err := cdiapi.NewSpec(
		&specs.Spec{
			Version: cdiVersion,
			Kind:    kind,
			Devices: []specs.Device{{
				Name:           deviceName,
				ContainerEdits: *containerEditsInDevices.ContainerEdits,
			}},
			ContainerEdits: *containerEdits.ContainerEdits,
		},
		getJsonFilePath(req.ClaimName), 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create new spec: %v", err)
	}
	klog.V(5).InfoS("CDI spec is validated")

	// create json file from spec
	doc, err := json.Marshal(*spec)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal: %v", err)
	}

	if err := ioutil.WriteFile(getJsonFilePath(req.ClaimName), doc, os.FileMode(0644)); err != nil {
		return nil, fmt.Errorf("failed to write file %v", err)
	}
	klog.Infof("Json file is created at " + getJsonFilePath(req.ClaimName))

	//return qualified name of the cdi device
	dev := spec.GetDevice(deviceName).GetQualifiedName()
	klog.Infof("Return: " + dev)
	return &drapbv1.NodePrepareResourceResponse{CdiDevice: []string{dev}}, nil
}

func (ex *examplePlugin) NodeUnprepareResource(ctx context.Context, req *drapbv1.NodeUnprepareResourceRequest) (*drapbv1.NodeUnprepareResourceResponse, error) {
	klog.Infof("NodeUnprepareResource is called: request: %+v", req)
	if _, err := os.Stat(getJsonFilePath(req.ClaimName)); err == nil {
		// jsonFile exists
		os.Remove(getJsonFilePath(req.ClaimName))
		klog.Infof("Json file " + getJsonFilePath(req.ClaimName) + " is removed")
	} else if errors.Is(err, os.ErrNotExist) {
		// jsonFile does not exist
		klog.Infof("Json file " + getJsonFilePath(req.ClaimName) + " is already deleted")
	} else {
		return &drapbv1.NodeUnprepareResourceResponse{}, fmt.Errorf("unexpected error: %v", err)
	}

	return &drapbv1.NodeUnprepareResourceResponse{}, nil
}

func (ex examplePlugin) startPlugin() error {
	klog.Infof("Starting kubelet plugin")

	if err := os.MkdirAll(filepath.Dir(ex.config.draAddress), 0750); err != nil {
		return fmt.Errorf("failed to create socket directory: %v", err)
	}

	// Run a grpc server for the driver, that listens on draAddress
	if err := plugin.StartNonblockingGrpcServer(ex.config.draAddress, &ex); err != nil {
		return fmt.Errorf("failed to start a nonblocking grpc server: %v", err)
	}

	// Run a registration server and registers plugin with kubelet
	if err := plugin.StartRegistrar(ex.config.driverName, ex.config.draAddress, ex.config.pluginRegistrationPath); err != nil {
		return fmt.Errorf("failed to start registrar: %v", err)
	}

	return nil

}
