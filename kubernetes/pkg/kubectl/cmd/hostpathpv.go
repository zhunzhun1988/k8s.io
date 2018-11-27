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

package cmd

import (
	"io"

	"github.com/spf13/cobra"

	"k8s.io/kubernetes/pkg/kubectl/cmd/templates"
	cmdutil "k8s.io/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/kubernetes/pkg/kubectl/util/i18n"
)

func NewCmdHostPathPV(f cmdutil.Factory, out, err io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hostpathpv <command> --help for more information about a given command.",
		Short: i18n.T("Cluster hostpath pv controls"),
		Long:  templates.LongDesc(`kubectl controls the Kubernetes cluster hostpath pv.`),
		Run:   runHostpathPvHelp,
	}

	cmd.AddCommand(NewCmdHostPathPVGet(f, out, err))
	cmd.AddCommand(NewCmdHostPathPVDescribe(f, out, err))
	cmd.AddCommand(NewCmdHostPathPVDelete(f, out, err))
	cmd.AddCommand(NewCmdHostPathPVMove(f, out, err))
	cmd.AddCommand(NewCmdHostPathPVDisable(f, out, err))
	cmd.AddCommand(NewCmdHostPathPVAdd(f, out, err))
	cmd.AddCommand(NewCmdHostPathPVUpgrade(f, out, err))
	return cmd
}
func runHostpathPvHelp(cmd *cobra.Command, args []string) {
	cmd.Help()
}
