/*
Copyright 2018 The Kubernetes Authors.

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

package options

import (
	"github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/apis/componentconfig"
)

// HPAControllerOptions holds the HPAController options.
type FPSControllerOptions struct {
	FailPodScaleLimit int32
}

// AddFlags adds flags related to HPAController for controller manager to the specified FlagSet.
func (o *FPSControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.FailPodScaleLimit, "fail-pod-scale-down-limit", o.FailPodScaleLimit, "The pod crash count above the limit the app will be scale down to 0.")
}

// ApplyTo fills up HPAController config with options.
func (o *FPSControllerOptions) ApplyTo(cfg *componentconfig.FailPodScalerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.FailPodScaleLimit = o.FailPodScaleLimit

	return nil
}

// Validate checks validation of HPAControllerOptions.
func (o *FPSControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
