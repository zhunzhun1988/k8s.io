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

// Package options contains flags for initializing a rescheduler.
package options

import (
	"time"

	"github.com/spf13/pflag"
)

// ReschedulerServerConfig configures and runs a Kubernetes proxy server
type ReschedulerServerConfig struct {
	InitialDelay                 time.Duration
	HousekeepingInterval         time.Duration
	PodScheduledTimeout          time.Duration
	InCluster                    bool
	EnableNSRescheduleDebug      bool
	ContentType                  string
	SystemNamespace              string
	RescheduleDebugServerPort    int
	LeaderElect                  bool
	LeaderElectLeaseDuration     time.Duration
	LeaderElectRenewDeadline     time.Duration
	LeaderElectRetryPeriod       time.Duration
	LeaderId                     string
	Master                       string
	Kubeconfig                   string
	ReschedulerPredicateProvider string
	PrometheusListenAddr         string
	// kubeAPIQPS is the QPS to use while talking with kubernetes apiserver.
	KubeAPIQPS float32 `json:"kubeAPIQPS"`
	// kubeAPIBurst is the QPS burst to use while talking with kubernetes apiserver.
	KubeAPIBurst int32 `json:"kubeAPIBurst"`
}

func NewReschedulerConfig() *ReschedulerServerConfig {
	return &ReschedulerServerConfig{}
}

// AddFlags adds flags for a specific ProxyServer to the specified FlagSet
func (s *ReschedulerServerConfig) AddFlags(fs *pflag.FlagSet) {

	fs.IntVar(&s.RescheduleDebugServerPort, "reschedule-debug-port", 8888, "If enableNSRescheduleDebug=true, the debug server will wait at this port for clinet to connect")
	fs.BoolVar(&s.EnableNSRescheduleDebug, "enable-namespace-reschedule-debug", true, "If true will run a debug server allow client to connect and observe the reschedule process")
	fs.DurationVar(&s.InitialDelay, "initial-delay", 2*time.Minute, "How long should rescheduler wait after start to make sure all critical addons had a chance to start.")
	fs.DurationVar(&s.HousekeepingInterval, "housekeeping-interval", 10*time.Second, "How often rescheduler takes actions.")
	fs.DurationVar(&s.PodScheduledTimeout, "pod-scheduled-timeout", 10*time.Minute, "How long should rescheduler wait for pod to be scheduled after evicting pods to make a spot for it.")
	fs.StringVar(&s.ContentType, "kube-api-content-type", "application/vnd.kubernetes.protobuf", "Content type of requests sent to apiserver.")
	//fs.StringVar(&s.SystemNamespace, "system-namespace", kube_api.NamespaceSystem, "Namespace to watch for critical addons.")
	fs.BoolVar(&s.LeaderElect, "leader-elect", true, "Start a leader election client and gain leadership before executing the main loop. Enable this when running replicated components for high availability. (default true)")
	fs.DurationVar(&s.LeaderElectLeaseDuration, "leader-elect-lease-duration", 15*time.Second, "The duration that non-leader candidates will wait after observing a leadership renewal until attempting to acquire leadership of a led but unrenewed leader slot. This is effectively the maximum duration that a leader can be stopped before it is replaced by another candidate. This is only applicable if leader election is enabled. (default 15s)")
	fs.DurationVar(&s.LeaderElectRenewDeadline, "leader-elect-renew-deadline", 10*time.Second, "The interval between attempts by the acting master to renew a leadership slot before it stops leading. This must be less than or equal to the lease duration. This is only applicable if leader election is enabled. (default 10s)")
	fs.DurationVar(&s.LeaderElectRetryPeriod, "leader-elect-retry-period", 2*time.Second, "The duration the clients should wait between attempting acquisition and renewal of a leadership. This is only applicable if leader election is enabled. (default 2s)")
	fs.StringVar(&s.LeaderId, "leader-name", "", "leaderId to create endpoint holderIdentity if empty will use the hostname")
	fs.StringVar(&s.Master, "master", "127.0.0.1:8080", "The address of the Kubernetes API server (overrides any value in kubeconfig),Default 127.0.0.1:8080")
	fs.StringVar(&s.Kubeconfig, "kubeconfig", "", "Path to kubeconfig file with authorization and master location information.")
	fs.Float32Var(&s.KubeAPIQPS, "kube-api-qps", 5.0, "QPS to use while talking with kubernetes apiserver")
	fs.Int32Var(&s.KubeAPIBurst, "kube-api-burst", 10, "Burst to use while talking with kubernetes apiserver")
	fs.StringVar(&s.ReschedulerPredicateProvider, "predicate-provider", "NameSpacePriorityProvider", "Use the predicates in provider to check whether the node can run the pod")
	fs.StringVar(&s.PrometheusListenAddr, "prometheus-listenaddr", "localhost:9235", "Address to listen on for serving prometheus metrics")
}
