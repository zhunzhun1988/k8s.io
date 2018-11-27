// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Handler for "raw" containers.
package raw

import (
	"fmt"
	"path"

	"github.com/google/cadvisor/container"
	"github.com/google/cadvisor/container/common"
	"github.com/google/cadvisor/container/libcontainer"
	"github.com/google/cadvisor/fs"
	info "github.com/google/cadvisor/info/v1"
	"github.com/google/cadvisor/machine"

	"github.com/golang/glog"
	cgroupfs "github.com/opencontainers/runc/libcontainer/cgroups/fs"
	"github.com/opencontainers/runc/libcontainer/configs"
)

type rawContainerHandler struct {
	hostnameOverride string
	// Name of the container for this handler.
	name               string
	clusterName        string
	quotadiskpath      string
	machineInfoFactory info.MachineInfoFactory

	// Absolute path to the cgroup hierarchies of this container.
	// (e.g.: "cpu" -> "/sys/fs/cgroup/cpu/test")
	cgroupPaths map[string]string

	fsInfo         fs.FsInfo
	externalMounts []common.Mount

	libcontainerHandler *libcontainer.Handler
}

func isRootCgroup(name string) bool {
	return name == "/"
}

func newRawContainerHandler(hostnameOverride string, name string, clusterName, quotadiskpath string, cgroupSubsystems *libcontainer.CgroupSubsystems,
	machineInfoFactory info.MachineInfoFactory, fsInfo fs.FsInfo, watcher *common.InotifyWatcher, rootFs string,
	ignoreMetrics container.MetricSet) (container.ContainerHandler, error) {
	cgroupPaths := common.MakeCgroupPaths(cgroupSubsystems.MountPoints, name)

	cHints, err := common.GetContainerHintsFromFile(*common.ArgContainerHints)
	if err != nil {
		return nil, err
	}

	// Generate the equivalent cgroup manager for this container.
	cgroupManager := &cgroupfs.Manager{
		Cgroups: &configs.Cgroup{
			Name: name,
		},
		Paths: cgroupPaths,
	}

	var externalMounts []common.Mount
	for _, container := range cHints.AllHosts {
		if name == container.FullName {
			externalMounts = container.Mounts
			break
		}
	}

	pid := 0
	if isRootCgroup(name) {
		pid = 1
	}

	handler := libcontainer.NewHandler(cgroupManager, rootFs, pid, ignoreMetrics)

	return &rawContainerHandler{
		hostnameOverride:    hostnameOverride,
		clusterName:         clusterName,
		quotadiskpath:       quotadiskpath,
		name:                name,
		machineInfoFactory:  machineInfoFactory,
		cgroupPaths:         cgroupPaths,
		fsInfo:              fsInfo,
		externalMounts:      externalMounts,
		libcontainerHandler: handler,
	}, nil
}

func (self *rawContainerHandler) ContainerReference() (info.ContainerReference, error) {
	// We only know the container by its one name.
	return info.ContainerReference{
		Name:             self.name,
		HostnameOverride: self.hostnameOverride,
		ClusterName:      self.clusterName,
	}, nil
}

func (self *rawContainerHandler) GetRootNetworkDevices() ([]info.NetInfo, error) {
	nd := []info.NetInfo{}
	if isRootCgroup(self.name) {
		mi, err := self.machineInfoFactory.GetMachineInfo()
		if err != nil {
			return nd, err
		}
		return mi.NetworkDevices, nil
	}
	return nd, nil
}

// Nothing to start up.
func (self *rawContainerHandler) Start() {}

// Nothing to clean up.
func (self *rawContainerHandler) Cleanup() {}

func (self *rawContainerHandler) GetSpec() (info.ContainerSpec, error) {
	const hasNetwork = false
	hasFilesystem := isRootCgroup(self.name) || len(self.externalMounts) > 0
	spec, err := common.GetSpec(self.cgroupPaths, self.machineInfoFactory, hasNetwork, hasFilesystem)
	if err != nil {
		return spec, err
	}

	if isRootCgroup(self.name) {
		// Check physical network devices for root container.
		nd, err := self.GetRootNetworkDevices()
		if err != nil {
			return spec, err
		}
		spec.HasNetwork = spec.HasNetwork || len(nd) != 0

		// Get memory and swap limits of the running machine
		memLimit, err := machine.GetMachineMemoryCapacity()
		if err != nil {
			glog.Warningf("failed to obtain memory limit for machine container")
			spec.HasMemory = false
		} else {
			spec.Memory.Limit = uint64(memLimit)
			// Spec is marked to have memory only if the memory limit is set
			spec.HasMemory = true
		}

		swapLimit, err := machine.GetMachineSwapCapacity()
		if err != nil {
			glog.Warningf("failed to obtain swap limit for machine container")
		} else {
			spec.Memory.SwapLimit = uint64(swapLimit)
		}
	}

	return spec, nil
}

func fsToFsStats(fs *fs.Fs) info.FsStats {
	inodes := uint64(0)
	inodesFree := uint64(0)
	hasInodes := fs.InodesFree != nil
	if hasInodes {
		inodes = *fs.Inodes
		inodesFree = *fs.InodesFree
	}
	return info.FsStats{
		Device:          fs.Device,
		Type:            fs.Type.String(),
		Limit:           fs.Capacity,
		Usage:           fs.Capacity - fs.Free,
		HasInodes:       hasInodes,
		Inodes:          inodes,
		InodesFree:      inodesFree,
		Available:       fs.Available,
		ReadsCompleted:  fs.DiskStats.ReadsCompleted,
		ReadsMerged:     fs.DiskStats.ReadsMerged,
		SectorsRead:     fs.DiskStats.SectorsRead,
		ReadTime:        fs.DiskStats.ReadTime,
		WritesCompleted: fs.DiskStats.WritesCompleted,
		WritesMerged:    fs.DiskStats.WritesMerged,
		SectorsWritten:  fs.DiskStats.SectorsWritten,
		WriteTime:       fs.DiskStats.WriteTime,
		IoInProgress:    fs.DiskStats.IoInProgress,
		IoTime:          fs.DiskStats.IoTime,
		WeightedIoTime:  fs.DiskStats.WeightedIoTime,
	}
}

func (self *rawContainerHandler) getFsStats(stats *info.ContainerStats) error {
	var allFs []fs.Fs
	// Get Filesystem information only for the root cgroup.
	if isRootCgroup(self.name) {
		filesystems, err := self.fsInfo.GetGlobalFsInfo()
		if err != nil {
			return err
		}
		for i := range filesystems {
			fs := filesystems[i]
			stats.Filesystem = append(stats.Filesystem, fsToFsStats(&fs))
		}
		allFs = filesystems
	} else if len(self.externalMounts) > 0 {
		var mountSet map[string]struct{}
		mountSet = make(map[string]struct{})
		for _, mount := range self.externalMounts {
			mountSet[mount.HostDir] = struct{}{}
		}
		filesystems, err := self.fsInfo.GetFsInfoForPath(mountSet)
		if err != nil {
			return err
		}
		for i := range filesystems {
			fs := filesystems[i]
			stats.Filesystem = append(stats.Filesystem, fsToFsStats(&fs))
		}
		allFs = filesystems
	}

	common.AssignDeviceNamesToDiskStats(&fsNamer{fs: allFs, factory: self.machineInfoFactory}, &stats.DiskIo)
	return nil
}

func (self *rawContainerHandler) getGpuStats(stats *info.ContainerStats) error {
	// Get gpu information only for the root cgroup.
	stats.Gpu.NumOfGPU = 0
	stats.Gpu.Usages = []info.GpuUsage{}
	if isRootCgroup(self.name) {
		gpu, err := machine.GetGpuInfo()
		if err != nil {
			return nil // maybe has no gpu we should return error
		}
		stats.Gpu.NumOfGPU = uint(len(gpu))
		stats.Gpu.Usages = gpu
	}
	return nil
}

func (self *rawContainerHandler) getDiskQuotaStats(stats *info.ContainerStats) error {
	// Get gpu information only for the root cgroup.
	stats.DiskQuota.NumOfQuotaDisk = 0
	stats.DiskQuota.Capacity = 0
	stats.DiskQuota.CurUseSize = 0
	stats.DiskQuota.CurQuotaSize = 0
	stats.DiskQuota.AvaliableSize = ""
	stats.DiskQuota.DiskStatus = make([]info.QuotaStatus, 0)
	if isRootCgroup(self.name) {
		paths, capacity, usedSize, quotaSize, err := machine.GetDiskQuotaStatus(path.Join(self.quotadiskpath, "status"))

		if err != nil {
			return nil // maybe has no disk quota we should return error
		}
		if len(paths) != len(capacity) || len(paths) != len(usedSize) || len(paths) != len(quotaSize) {
			return fmt.Errorf("rawContainerHandler getDiskQuotaStats %s err", path.Join(self.quotadiskpath, "status"))
		}
		stats.DiskQuota.NumOfQuotaDisk = uint(len(paths))
		funcSizeToStr := func(size uint64) string {
			if size < 1024 {
				return fmt.Sprintf("%dB", size)
			} else if size < 1024*1024 {
				return fmt.Sprintf("%.2fKB", float64(size)/1024.0)
			} else if size < 1024*1024*1024 {
				return fmt.Sprintf("%.2fMB", float64(size)/(1024.0*1024.0))
			} else if size < 1024*1024*1024*1024 {
				return fmt.Sprintf("%.2fGB", float64(size)/(1024.0*1024.0*1024.0))
			} else {
				return fmt.Sprintf("%.2fTB", float64(size)/(1024.0*1024.0*1024.0*1024.0))
			}
		}
		for i, _ := range paths {
			var qs info.QuotaStatus
			qs.Capacity = capacity[i]
			qs.CurUseSize = usedSize[i]
			qs.CurQuotaSize = quotaSize[i]
			qs.AvaliableSize = fmt.Sprintf("availableSize:%s, availabeQuotaSize:%s", funcSizeToStr(capacity[i]-usedSize[i]), funcSizeToStr(capacity[i]-quotaSize[i]))
			qs.MountPath = paths[i]
			stats.DiskQuota.DiskStatus = append(stats.DiskQuota.DiskStatus, qs)
			stats.DiskQuota.Capacity += qs.Capacity
			stats.DiskQuota.CurUseSize += qs.CurUseSize
			stats.DiskQuota.CurQuotaSize += qs.CurQuotaSize
		}
		stats.DiskQuota.AvaliableSize = fmt.Sprintf("availableSize:%s, availabeQuotaSize:%s", funcSizeToStr(stats.DiskQuota.Capacity-stats.DiskQuota.CurUseSize),
			funcSizeToStr(stats.DiskQuota.Capacity-stats.DiskQuota.CurQuotaSize))

	}
	return nil
}

func (self *rawContainerHandler) GetStats() (*info.ContainerStats, error) {
	stats, err := self.libcontainerHandler.GetStats()
	if err != nil {
		return stats, err
	}
	stats.ClusterName = self.clusterName

	// Get filesystem stats.
	err = self.getFsStats(stats)
	if err != nil {
		return stats, err
	}

	// Get gpu stats.
	err = self.getGpuStats(stats)
	if err != nil {
		return stats, err
	}

	// Get disk quota stats.
	err = self.getDiskQuotaStats(stats)
	if err != nil {
		return stats, err
	}

	return stats, nil
}

func (self *rawContainerHandler) GetCgroupPath(resource string) (string, error) {
	path, ok := self.cgroupPaths[resource]
	if !ok {
		return "", fmt.Errorf("could not find path for resource %q for container %q\n", resource, self.name)
	}
	return path, nil
}

func (self *rawContainerHandler) GetContainerLabels() map[string]string {
	return map[string]string{}
}

func (self *rawContainerHandler) GetContainerIPAddress() string {
	// the IP address for the raw container corresponds to the system ip address.
	return "127.0.0.1"
}

func (self *rawContainerHandler) ListContainers(listType container.ListType) ([]info.ContainerReference, error) {
	return common.ListContainers(self.name, self.cgroupPaths, listType)
}

func (self *rawContainerHandler) ListProcesses(listType container.ListType) ([]int, error) {
	return self.libcontainerHandler.GetProcesses()
}

func (self *rawContainerHandler) Exists() bool {
	return common.CgroupExists(self.cgroupPaths)
}

func (self *rawContainerHandler) Type() container.ContainerType {
	return container.ContainerTypeRaw
}

type fsNamer struct {
	fs      []fs.Fs
	factory info.MachineInfoFactory
	info    common.DeviceNamer
}

func (n *fsNamer) DeviceName(major, minor uint64) (string, bool) {
	for _, info := range n.fs {
		if uint64(info.Major) == major && uint64(info.Minor) == minor {
			return info.Device, true
		}
	}
	if n.info == nil {
		mi, err := n.factory.GetMachineInfo()
		if err != nil {
			return "", false
		}
		n.info = (*common.MachineInfoNamer)(mi)
	}
	return n.info.DeviceName(major, minor)
}
