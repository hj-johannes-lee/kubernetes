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
package kubeletplugin

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

type nodeRegistrarConfig struct {
	draDriverName          string
	draAddress             string
	pluginRegistrationPath string
}
type nodeRegistrar struct {
	config nodeRegistrarConfig
}

func buildSocketPath(cdiDriverName string, pluginRegistrationPath string) string {
	return fmt.Sprintf("%s/%s-reg.sock", pluginRegistrationPath, cdiDriverName)
}

func removeRegSocket(cdiDriverName, pluginRegistrationPath string) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM)
	<-sigc
	socketPath := buildSocketPath(cdiDriverName, pluginRegistrationPath)
	err := os.Remove(socketPath)
	if err != nil && !os.IsNotExist(err) {
		klog.Errorf("failed to remove socket: %s with error: %+v", socketPath, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func umask(mask int) (int, error) {
	return unix.Umask(mask), nil
}

func cleanupSocketFile(socketPath string) error {
	socketExists, err := doesSocketExist(socketPath)
	if err != nil {
		return err
	}
	if socketExists {
		if err := os.Remove(socketPath); err != nil {
			return fmt.Errorf("failed to remove stale socket %s with error: %+v", socketPath, err)
		}
	}
	return nil
}

func doesSocketExist(socketPath string) (bool, error) {
	fi, err := os.Stat(socketPath)
	if err == nil {
		if isSocket := (fi.Mode()&os.ModeSocket != 0); isSocket {
			return true, nil
		}
		return false, fmt.Errorf("file exists in socketPath %s but it's not a socket.: %+v", socketPath, fi)
	}
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("failed to stat the socket %s with error: %+v", socketPath, err)
	}
	return false, nil
}

// newRegistrar returns an initialized nodeRegistrar instance
func newRegistrar(config nodeRegistrarConfig) *nodeRegistrar {
	return &nodeRegistrar{
		config: config,
	}
}

// nodeRegister runs a registration server and registers plugin with kubelet
func (nr nodeRegistrar) nodeRegister() error {
	// Run a registration server
	registrationServer := newRegistrationServer(nr.config.draDriverName, nr.config.draAddress, []string{"1.0.0"})
	socketPath := buildSocketPath(nr.config.draDriverName, nr.config.pluginRegistrationPath)
	if err := cleanupSocketFile(socketPath); err != nil {
		return fmt.Errorf(err.Error())
	}

	var oldmask int
	if runtime.GOOS == "linux" {
		oldmask, _ = umask(0077)
	}

	klog.Infof("Starting Registration Server at: %s\n", socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on socket: %s with error: %+v", socketPath, err)
	}

	if runtime.GOOS == "linux" {
		umask(oldmask)
	}
	klog.Infof("Registration Server started at: %s\n", socketPath)
	grpcServer := grpc.NewServer()

	// Register kubelet plugin watcher api.
	registerapi.RegisterRegistrationServer(grpcServer, registrationServer)

	go removeRegSocket(nr.config.draDriverName, nr.config.pluginRegistrationPath)
	// Start service
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("registration server stopped serving: %v", err)
	}
	return nil
}

// StartRegistrar creates a new registrar, and run the function nodeRegister.
func StartRegistrar(driverName, draAddress, pluginRegistrationPath string) error {
	klog.Infof("Starting noderegistrar")
	registrarConfig := nodeRegistrarConfig{
		draDriverName:          driverName,
		draAddress:             draAddress,
		pluginRegistrationPath: pluginRegistrationPath,
	}
	registrar := newRegistrar(registrarConfig)
	if err := registrar.nodeRegister(); err != nil {
		return fmt.Errorf("failed to register node: %v", err)
	}

	return nil
}
