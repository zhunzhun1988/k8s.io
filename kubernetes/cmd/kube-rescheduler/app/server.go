/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/golang/glog"

	"github.com/prometheus/client_golang/prometheus"
	apiv1 "k8s.io/api/core/v1"
	clientv1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	kube_record "k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/cmd/kube-rescheduler/app/options"
	ca_simulator "k8s.io/kubernetes/cmd/kube-rescheduler/simulator"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controller"
	myrescheduler "k8s.io/kubernetes/pkg/rescheduler"
	rs_utils "k8s.io/kubernetes/pkg/rescheduler/utils"
)

type RescheduleServer struct {
	rsConfig         *options.ReschedulerServerConfig
	kubeClient       *clientset.Clientset
	recorder         kube_record.EventRecorder
	clientBuilder    controller.ControllerClientBuilder
	reschedulerCache *rs_utils.ReschedulerCache
}

func NewRescheduleServer(config *options.ReschedulerServerConfig) (*RescheduleServer, error) {
	kubeClient, clientBuilder, err := createKubeClientAndBuilder(config)
	if err != nil {
		glog.Fatalf("Failed to create kube client: %v", err)
		return nil, err
	}
	recorder := createEventRecorder(kubeClient)
	return &RescheduleServer{
		rsConfig:      config,
		kubeClient:    kubeClient,
		recorder:      recorder,
		clientBuilder: clientBuilder,
	}, nil
}

func (s *RescheduleServer) startRescheduler() error {
	s.reschedulerCache.Start()
	defer s.reschedulerCache.Stop()

	go func() {
		http.Handle("/metrics", prometheus.Handler())
		err := http.ListenAndServe(s.rsConfig.PrometheusListenAddr, nil)
		glog.Fatalf("Failed to start metrics: %v", err)
	}()

	if err := s.reschedulerCache.Wait(); err != nil {
		glog.Errorf("reschedulerCache wait err =%v\n", err)
		return fmt.Errorf("startRescheduler wait err:%v", err)
	}
	glog.Infof("reschedulerCache wait ok")
	stop := make(chan struct{})
	defer close(stop)
	predicateChecker, err := ca_simulator.NewPredicateChecker(s.kubeClient, s.rsConfig.ReschedulerPredicateProvider, stop)
	if err != nil {
		glog.Fatalf("Failed to create predicate checker: %v", err)
		return err
	}

	// TODO(piosz): consider reseting this set once every few hours.
	podsBeingProcessed := rs_utils.NewPodSet()
	apiClient := rs_utils.NewApiClient(s.kubeClient, s.reschedulerCache)

	releaseAllTaints(apiClient, podsBeingProcessed)
	glog.Infof("enableNSRescheduleDebug=%t, rescheduleDebugServerPort=%d", s.rsConfig.EnableNSRescheduleDebug, s.rsConfig.RescheduleDebugServerPort)

	nsRs := myrescheduler.NewNSReschedule(apiClient, predicateChecker, podsBeingProcessed,
		s.rsConfig.EnableNSRescheduleDebug, s.rsConfig.RescheduleDebugServerPort, s.rsConfig.PodScheduledTimeout)

	criticalRs := myrescheduler.NewCriticalRescheduler(apiClient, podsBeingProcessed,
		s.recorder, s.rsConfig.PodScheduledTimeout, predicateChecker)

	for {
		select {
		case <-time.After(s.rsConfig.HousekeepingInterval):
			{
				if nsRs != nil {
					nsRs.CheckAndReschedule()
				}
				if criticalRs != nil {
					if err := criticalRs.CheckAndReschedule(); err != nil {
						glog.Errorf("criticalRs.CheckAndReschedule() err =%v\n", err)
					}
				}
				releaseAllTaints(apiClient, podsBeingProcessed)
			}
		}
	}
	return nil
}

// Run runs the specified RescheduleServer.  This should never exit (unless CleanupAndExit is set).
func (s *RescheduleServer) Run() error {
	glog.Infof("Running Rescheduler")

	s.reschedulerCache = rs_utils.NewReschedulerCache(s.clientBuilder)
	// TODO(piosz): figure our a better way of verifying cluster stabilization here.
	time.Sleep(s.rsConfig.InitialDelay)

	run := func(stop <-chan struct{}) {
		err := s.startRescheduler()
		glog.Fatalf("error startRescheduler: %v", err)
		panic("unreachable")
	}
	if !s.rsConfig.LeaderElect {
		run(nil)
		panic("unreachable")
	}
	id := ""
	if s.rsConfig.LeaderId != "" {
		id = s.rsConfig.LeaderId
	} else {
		hostname, err := os.Hostname()
		if err != nil {
			glog.Errorf("Failed to get hostname %v", err)
			return err
		}
		id = hostname
	}

	// Lock required for leader election
	rl, err := resourcelock.New("endpoints",
		"kube-system",
		"kube-rescheduler",
		s.kubeClient.CoreV1(),
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: s.recorder,
		})
	if err != nil {
		glog.Fatalf("error creating lock: %v", err)
	}

	leaderelection.RunOrDie(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: s.rsConfig.LeaderElectLeaseDuration,
		RenewDeadline: s.rsConfig.LeaderElectRenewDeadline,
		RetryPeriod:   s.rsConfig.LeaderElectRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				glog.Fatalf("leaderelection lost")
			},
		},
	})

	return nil
}

func createKubeClientAndBuilder(config *options.ReschedulerServerConfig) (*clientset.Clientset, controller.ControllerClientBuilder, error) {
	kubeconfig, err := clientcmd.BuildConfigFromFlags(config.Master, config.Kubeconfig)
	if err != nil {
		glog.Errorf("unable to build config from flags: %v", err)
		return nil, nil, err
	}

	kubeconfig.ContentType = config.ContentType
	// Override kubeconfig qps/burst settings from flags
	kubeconfig.QPS = config.KubeAPIQPS
	kubeconfig.Burst = int(config.KubeAPIBurst)

	client, err := clientset.NewForConfig(restclient.AddUserAgent(kubeconfig, "rescheduler"))
	if err != nil {
		glog.Fatalf("Invalid API configuration: %v", err)
		return nil, nil, err
	}

	rootClientBuilder := controller.SimpleControllerClientBuilder{
		ClientConfig: kubeconfig,
	}
	return client, rootClientBuilder, nil
}

func createEventRecorder(client *clientset.Clientset) kube_record.EventRecorder {
	eventBroadcaster := kube_record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(client.Core().RESTClient()).Events("")})
	return eventBroadcaster.NewRecorder(legacyscheme.Scheme, clientv1.EventSource{Component: "rescheduler"})
}

func releaseAllTaints(client rs_utils.ApiClientInterface, podsBeingProcessed *rs_utils.PodSet) {
	nodes := client.ListReadyNodes()

	releaseTaintsOnNodes(client, nodes, podsBeingProcessed)
}

func releaseTaintsOnNodes(client rs_utils.ApiClientInterface, nodes []*apiv1.Node, podsBeingProcessed *rs_utils.PodSet) {
	for _, node := range nodes {
		newTaints := make([]apiv1.Taint, 0)
		for _, taint := range node.Spec.Taints {
			if taint.Key == rs_utils.CriticalPodAnnotation && !podsBeingProcessed.HasId(taint.Value) {
				glog.Infof("Releasing taint %+v on node %v", taint, node.Name)
			} else {
				newTaints = append(newTaints, taint)
			}
		}

		if len(newTaints) != len(node.Spec.Taints) {
			node.Spec.Taints = newTaints
			_, err := client.UpdateNode(node)
			if err != nil {
				glog.Warningf("Error while releasing taints on node %v: %v", node.Name, err)
			} else {
				glog.Infof("Successfully released all taints on node %v", node.Name)
			}
		}
	}
}
