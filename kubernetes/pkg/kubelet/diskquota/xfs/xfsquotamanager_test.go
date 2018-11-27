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

package xfs

import (
	"path"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	utilequota "k8s.io/kubernetes/pkg/kubelet/diskquota/xfs/utils"
)

func newXFSQuotaManager(checkPath string, runner utilequota.Interface) DiskQuotaManager {
	quotaManager := &XFSQuotaManager{
		disks:            make([]DiskQuotaInfo, 0),
		projectGenerator: nil,
		checkPath:        "/xfs",
		runner:           runner,
		podKeepIdMap:     make(map[string]string),
	}
	quotaManager.initQuotaXFSDisk()
	return quotaManager
}

func getDiskInfo(diskpath string, projects []utilequota.FakeXFSQuotaProject) (hardQuotaSize, softQuotaSize, usedSize int64) {
	for _, project := range projects {
		if strings.HasPrefix(project.MountPath, diskpath) {
			hardQuotaSize += project.HardQuota
			softQuotaSize += project.SoftQuota
			usedSize += project.UsedSize
		}
	}
	return
}
func filterById(projects []utilequota.FakeXFSQuotaProject, id string) []utilequota.FakeXFSQuotaProject {
	ret := make([]utilequota.FakeXFSQuotaProject, 0, len(projects))
	for _, project := range projects {
		if project.Id == id {
			ret = append(ret, project)
		}
	}
	return ret
}

func getIds(projects []utilequota.FakeXFSQuotaProject) sets.String {
	ret := sets.NewString()
	for _, project := range projects {
		ret.Insert(project.Id)
	}
	return ret
}

func isProjectValid(project utilequota.FakeXFSQuotaProject, devices []utilequota.FakeXFSDevice) bool {
	for _, device := range devices {
		if device.ProjQuotaOn == true {
			if strings.HasPrefix(project.MountPath, device.MountPath) {
				return true
			}
		}
	}
	return true
}

func getQuotaDiskNum(devices []utilequota.FakeXFSDevice) int {
	var ret int
	for _, device := range devices {
		if device.ProjQuotaOn == true {
			ret++
		}
	}
	return ret
}

func TestXFSQuotaManagerGeneralTest(t *testing.T) {
	device1 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdb1",
		MountPath:    "/xfs/disk1",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	device2 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdc1",
		MountPath:    "/xfs/disk2",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	device3 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdd1",
		MountPath:    "/xfs/disk3",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  false,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	quotapaths := []string{
		"/xfs/disk1/" + xfsQuotaDirPrifix + "_testid1",
		"/xfs/disk2/" + xfsQuotaDirPrifix + "_testid2",
	}
	quotaProject1 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[0],
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    5 * 1024 * 1024,
		ProjectName: "k8spro1",
		ProjectId:   1,
		Id:          "testid1",
		SubId:       "",
	}
	quotaProject2 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[1],
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro2",
		ProjectId:   2,
		Id:          "testid2",
		SubId:       "",
	}
	quotaDevices := []utilequota.FakeXFSDevice{device1, device2, device3}
	quotaProjects := []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2}
	xfsQuotaRunner := utilequota.NewFakeXFSQuotaRunner(quotaDevices, quotaProjects, quotapaths, "")

	quotaManager := newXFSQuotaManager("/xfs", xfsQuotaRunner)
	if quotaManager.GetQuotaDiskNum() != getQuotaDiskNum(quotaDevices) {
		t.Errorf("GetQuotaDiskNum = %d not equal %d", quotaManager.GetQuotaDiskNum(), getQuotaDiskNum(quotaDevices))
	}
	diskInfo := quotaManager.GetQuotaDiskInfos()

	if len(diskInfo) != getQuotaDiskNum(quotaDevices) {
		t.Errorf("len(diskInfo) = %d not equal %d", len(diskInfo), getQuotaDiskNum(quotaDevices))
	} else {
		var nextIndex int
		for _, device := range quotaDevices {
			if device.ProjQuotaOn == true {
				if getStandardPathString(diskInfo[nextIndex].MountPath) != getStandardPathString(device.MountPath) {
					t.Errorf("diskInfo[%d].MountPath = %s not equal %s", nextIndex, getStandardPathString(diskInfo[nextIndex].MountPath), getStandardPathString(device.MountPath))
				}
				hq, _, us := getDiskInfo(diskInfo[nextIndex].MountPath, quotaProjects)
				if diskInfo[nextIndex].Capacity != device.Capacity || diskInfo[nextIndex].QuotaedSize != hq || diskInfo[nextIndex].UsedSize != us {
					t.Errorf("diskInfo[%d] MountPath:%s Capacity QuotaedSize UsedSize (%d,%d,%d)!=(%d,%d,%d)",
						nextIndex, diskInfo[nextIndex].MountPath, diskInfo[nextIndex].Capacity, diskInfo[nextIndex].QuotaedSize, diskInfo[nextIndex].UsedSize, device1.Capacity, hq, us)
				}
				nextIndex++
			}
		}
	}

	for _, project := range quotaProjects {
		valid := isProjectValid(project, quotaDevices)
		isQuota, info := quotaManager.IsPathXFSQuota(project.MountPath)
		isQuota2, info2 := quotaManager.IsIdXFSQuota(project.Id, project.SubId)
		if valid != isQuota || valid != isQuota2 {
			t.Errorf("project:%s valid=%t, isQuota = %t, isQuotaById=%t", project.MountPath, valid, isQuota, isQuota2)
		} else if valid {
			if info.HardQuota != project.HardQuota || info.SoftQuota != project.SoftQuota || info.UsedSize != project.UsedSize ||
				info.ProjectId != project.ProjectId || info.ProjectName != project.ProjectName || info.Path != project.MountPath {
				t.Errorf("project %v not equal quotaProject get by mountpath %v", project, info)
			}
			if info2.HardQuota != project.HardQuota || info2.SoftQuota != project.SoftQuota || info2.UsedSize != project.UsedSize ||
				info2.ProjectId != project.ProjectId || info2.ProjectName != project.ProjectName || info2.Path != project.MountPath {
				t.Errorf("project %v not equal quotaProject get by id %v", project, info2)
			}
		}
	}

	ids := getIds(quotaProjects)
	for id, _ := range ids {
		felterProjects := filterById(quotaProjects, id)
		quotaProjects := quotaManager.GetXFSQuotasById(id)
		if len(felterProjects) != len(quotaProjects) {
			t.Errorf("project id:%s len(felterProjects)=%d, len(quotaProjects)=%d", id, len(felterProjects), len(quotaProjects))
		}
	}

	addId := "testid3"
	ok, path, err := quotaManager.AddPathQuota("", addId, "", "", "", false, 5*1024*1024, 5*1024*1024, func(id, pvid string) bool { return true })
	if ok == false || err != nil {
		t.Errorf("add %s err=%v", addId, err)
	}
	exists, info := quotaManager.IsIdXFSQuota(addId, "")
	if exists == false {
		t.Errorf("can't get quotaproject id:%s after add", addId)
	} else {
		if info.HardQuota != 5*1024*1024 || info.SoftQuota != 5*1024*1024 || info.GetPath() != path {
			t.Errorf("quotaproject %s get is different from add", addId)
		}
	}

	changeOk, changeErr := quotaManager.ChangeQuota(addId, "", 10*1024*1024, 10*1024*1024)
	if changeOk != true || changeErr != nil {
		t.Errorf("change project err=%v\n", changeErr)
	}
	exists, info = quotaManager.IsIdXFSQuota(addId, "")
	if exists == false {
		t.Errorf("can't get quotaproject id:%s after add", addId)
	} else {
		if info.HardQuota != 10*1024*1024 || info.SoftQuota != 10*1024*1024 || info.GetPath() != path {
			t.Errorf("quotaproject %s get is different frome after change", addId)
		}
	}

	deleteOk, deleteErr := quotaManager.DeletePathQuota(addId, "")
	if deleteOk == false || deleteErr != nil {
		t.Errorf("delete path quota err=%v", deleteErr)
	}

	exists, _ = quotaManager.IsIdXFSQuota(addId, "")
	if exists == true {
		t.Errorf("%s is delete should not be get again", addId)
	}

}

func TestXFSQuotaManagerAddTest(t *testing.T) {
	device1 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdb1",
		MountPath:    "/xfs/disk1",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	device2 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdc1",
		MountPath:    "/xfs/disk2",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     150 * 1024 * 1024, // 100MB
	}
	device3 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdd1",
		MountPath:    "/xfs/disk3",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     200 * 1024 * 1024, // 100MB
	}
	quotapaths := []string{
		"/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1_testsubid1/" + xfsKeepForOnePodInnerDir,
		"/xfs/disk2/" + xfsQuotaDirPrifix + "_testid2_testsubid2/" + xfsKeepForOnePodInnerDir,
	}
	quotaProject1 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[0],
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    5 * 1024 * 1024,
		ProjectName: "k8spro1",
		ProjectId:   1,
		Id:          "testid1",
		SubId:       "testsubid1",
		OwnerId:     "testownerid1",
	}
	quotaProject2 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[1],
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro2",
		ProjectId:   2,
		Id:          "testid2",
		SubId:       "testsubid2",
		OwnerId:     "testownerid1",
	}
	addProjects := []struct {
		HardQuota         int64
		SoftQuota         int64
		CanAdd            bool
		TestName          string
		Id                string
		SubId             string
		OwnerId           string
		ShouldMountDevice *utilequota.FakeXFSDevice
		Devices           []utilequota.FakeXFSDevice
		ExistsProject     []utilequota.FakeXFSQuotaProject
	}{
		{
			HardQuota:         100 * 1024 * 1024,
			SoftQuota:         100 * 1024 * 1024,
			CanAdd:            true,
			Id:                "testaddid1",
			SubId:             "",
			OwnerId:           "testownerid1",
			ShouldMountDevice: &device3,
			Devices:           []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:     make([]utilequota.FakeXFSQuotaProject, 0),
		},
		{
			HardQuota:         220 * 1024 * 1024,
			SoftQuota:         220 * 1024 * 1024,
			CanAdd:            false,
			Id:                "testaddid1",
			SubId:             "",
			OwnerId:           "testownerid1",
			ShouldMountDevice: nil,
			Devices:           []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:     make([]utilequota.FakeXFSQuotaProject, 0),
		},
		{
			HardQuota:         20 * 1024 * 1024,
			SoftQuota:         20 * 1024 * 1024,
			CanAdd:            true,
			Id:                "testaddid1",
			SubId:             "testsubid1",
			OwnerId:           "testownerid1",
			ShouldMountDevice: &device1,
			Devices:           []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:     []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2},
		},
		{
			HardQuota:         20 * 1024 * 1024,
			SoftQuota:         20 * 1024 * 1024,
			CanAdd:            true,
			Id:                "testaddid1",
			SubId:             "testsubid1",
			OwnerId:           "testownerid1",
			ShouldMountDevice: &device2,
			Devices:           []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:     []utilequota.FakeXFSQuotaProject{quotaProject1},
		},
		{
			HardQuota:         20 * 1024 * 1024,
			SoftQuota:         20 * 1024 * 1024,
			CanAdd:            true,
			Id:                "testaddid2",
			SubId:             "testsubid2",
			OwnerId:           "testownerid2",
			ShouldMountDevice: &device3,
			Devices:           []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:     []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2},
		},
	}

	for _, project := range addProjects {

		xfsQuotaRunner := utilequota.NewFakeXFSQuotaRunner(project.Devices, []utilequota.FakeXFSQuotaProject{}, quotapaths, "")
		quotaManager := newXFSQuotaManager("/xfs", xfsQuotaRunner)

		for _, existsProject := range project.ExistsProject {
			ok, path, err := quotaManager.AddPathQuota(existsProject.OwnerId, existsProject.Id, existsProject.SubId, "", "", false,
				existsProject.SoftQuota, existsProject.HardQuota, func(id, pvid string) bool { return true })
			if ok == false || path != existsProject.MountPath {
				t.Errorf("existsProject add error ok=%t, path:%s, existsProject.MountPath:%s, err=%v", ok, path, existsProject.MountPath, err)
			}
		}
		ok, path, addErr := quotaManager.AddPathQuota(project.OwnerId, project.Id, project.SubId, "", "", false,
			project.SoftQuota, project.HardQuota, func(id, pvid string) bool { return true })
		if ok != project.CanAdd {
			t.Errorf("add project %s should add %t, but add %t, err=%v", project.Id, project.CanAdd, ok, addErr)
		} else if ok == true {
			if strings.HasPrefix(path, project.ShouldMountDevice.MountPath) == false {
				t.Errorf("should add to device path %s not to %s", project.ShouldMountDevice.MountPath, path)
			}
		}
	}
}

// test following case
// io.enndata.user/alpha-pvhostpathmountpolicy:keep
// io.enndata.kubelet/alpha-pvhostpathquotaforonepod:true
func TestXFSQuotaManagerAddKeepForOnePodTest(t *testing.T) {
	device1 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdb1",
		MountPath:    "/xfs/disk1",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	device2 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdc1",
		MountPath:    "/xfs/disk2",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     150 * 1024 * 1024, // 100MB
	}
	device3 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdd1",
		MountPath:    "/xfs/disk3",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     200 * 1024 * 1024, // 100MB
	}
	quotapaths := []string{
		"/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1_" + xfsKeepForOnePodFlag + "-testsubid1/",
		"/xfs/disk2/" + xfsQuotaDirPrifix + "_testid2_" + xfsKeepForOnePodFlag + "-testsubid2/",
		"/xfs/disk1/" + xfsQuotaDirPrifix + "_testid1_" + xfsKeepForOnePodFlag + "-testsubid3/",
	}
	quotaProject1 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[0] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    5 * 1024 * 1024,
		ProjectName: "k8spro1",
		ProjectId:   1,
		Id:          "testid1",
		KeepId:      xfsKeepForOnePodFlag + "-testsubid1/",
		SubId:       "testsubid1",
		OwnerId:     "testownerid1",
	}
	quotaProject2 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[1] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro2",
		ProjectId:   2,
		Id:          "testid2",
		KeepId:      xfsKeepForOnePodFlag + "-testsubid2/",
		SubId:       "testsubid2",
		OwnerId:     "testownerid1",
	}
	quotaProject3 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[2] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro3",
		ProjectId:   3,
		Id:          "testid1",
		KeepId:      xfsKeepForOnePodFlag + "-testsubid3/",
		SubId:       "testsubid3",
		OwnerId:     "testownerid1",
	}
	quotaProject4 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[2] + xfsKeepForOnePodInnerDir,
		SoftQuota:   100 * 1024 * 1024,
		HardQuota:   100 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro4",
		ProjectId:   4,
		Id:          "testid1",
		KeepId:      xfsKeepForOnePodFlag + "-testsubid1/",
		SubId:       "testsubid1",
		OwnerId:     "testownerid1",
	}
	getActiveFun := func(activeIds []string) ActiveFun {
		return func(id, pvid string) bool {
			for _, existid := range activeIds {
				if existid == id {
					return true
				}
			}
			return false
		}
	}
	addProjects := []struct {
		HardQuota       int64
		SoftQuota       int64
		CanAdd          bool
		TestName        string
		Id              string
		SubId           string
		OwnerId         string
		ShouldMountPath string
		Devices         []utilequota.FakeXFSDevice
		ExistsProject   []utilequota.FakeXFSQuotaProject
		ExistsPaths     map[string]string
		AcitvieSubids   []string
	}{
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testaddid1",
			SubId:           "testsubid1",
			OwnerId:         "testownerid1",
			ShouldMountPath: "/xfs/disk3/" + xfsQuotaDirPrifix + "_testaddid1_" + xfsKeepForOnePodFlag + "-testsubid1/" + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   make([]utilequota.FakeXFSQuotaProject, 0),
			ExistsPaths:     map[string]string{},
			AcitvieSubids:   []string{},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "testsubid1",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotaProject3.MountPath,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject3},
			ExistsPaths:     map[string]string{quotapaths[2]: "testsubid3"},
			AcitvieSubids:   []string{},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "testsubid1",
			OwnerId:         "testownerid1",
			ShouldMountPath: "/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1_" + xfsKeepForOnePodFlag + "-testsubid1/" + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject3},
			ExistsPaths:     map[string]string{quotapaths[2]: "testsubid3"},
			AcitvieSubids:   []string{"testsubid3"},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "testsubid4",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotaProject1.MountPath,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject3, quotaProject2, quotaProject1},
			ExistsPaths:     map[string]string{quotapaths[2]: "testsubid3", quotapaths[1]: "testsubid2", quotapaths[0]: "testsubid1"},
			AcitvieSubids:   []string{"testsubid3"},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "testsubid4",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotaProject3.MountPath,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject3, quotaProject2, quotaProject1},
			ExistsPaths:     map[string]string{quotapaths[2]: "testsubid3", quotapaths[1]: "testsubid2", quotapaths[0]: "testsubid1"},
			AcitvieSubids:   []string{"testsubid1"},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          false,
			Id:              "testid1",
			SubId:           "testsubid4",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotaProject4.MountPath,
			Devices:         []utilequota.FakeXFSDevice{device1},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject4},
			ExistsPaths:     map[string]string{quotapaths[2]: "testsubid1"},
			AcitvieSubids:   []string{"testsubid1"},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "testsubid4",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotaProject4.MountPath,
			Devices:         []utilequota.FakeXFSDevice{device1},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject4},
			ExistsPaths:     map[string]string{quotapaths[2]: "testsubid1"},
			AcitvieSubids:   []string{},
		},
	}

	for i, project := range addProjects {
		paths := make([]string, 0, len(project.ExistsPaths))
		for p, _ := range project.ExistsPaths {
			paths = append(paths, p)
		}
		xfsQuotaRunner := utilequota.NewFakeXFSQuotaRunner(project.Devices, []utilequota.FakeXFSQuotaProject{}, paths, "")
		for p, content := range project.ExistsPaths {
			xfsQuotaRunner.WriteFile(p, content)
		}
		quotaManager := newXFSQuotaManager("/xfs", xfsQuotaRunner)

		subids := make([]string, 0, len(project.ExistsProject))
		for _, existsProject := range project.ExistsProject {
			ok, path, err := quotaManager.AddPathQuota(existsProject.OwnerId, existsProject.Id, existsProject.SubId, "", "", true,
				existsProject.SoftQuota, existsProject.HardQuota, getActiveFun(subids))
			if ok == false || path != existsProject.MountPath {
				t.Errorf("Case[%d] existsProject add error ok=%t, path:%s, existsProject.MountPath:%s, err=%v", i, ok, path, existsProject.MountPath, err)
			}
			subids = append(subids, existsProject.SubId)
		}
		ok, p, addErr := quotaManager.AddPathQuota(project.OwnerId, project.Id, project.SubId, "", "", true,
			project.SoftQuota, project.HardQuota, getActiveFun(project.AcitvieSubids))
		if ok != project.CanAdd {
			t.Errorf("Case[%d] add project %s should add %t, but add %t, err=%v", i, project.Id, project.CanAdd, ok, addErr)
		} else if ok == true {
			if path.Clean(p) != path.Clean(project.ShouldMountPath) {
				t.Errorf("Case[%d] should add to path %s not to %s", i, path.Clean(project.ShouldMountPath), path.Clean(p))
			}
		}
	}

}

// test following case
// io.enndata.user/alpha-pvhostpathmountpolicy:keep
// io.enndata.kubelet/alpha-pvhostpathquotaforonepod:false
func TestXFSQuotaManagerAddKeepNotForOnePodTest(t *testing.T) {
	device1 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdb1",
		MountPath:    "/xfs/disk1",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	device2 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdc1",
		MountPath:    "/xfs/disk2",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     150 * 1024 * 1024, // 100MB
	}
	device3 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdd1",
		MountPath:    "/xfs/disk3",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     200 * 1024 * 1024, // 100MB
	}
	quotapaths := []string{
		"/xfs/disk1/" + xfsQuotaDirPrifix + "_testid1/",
		"/xfs/disk2/" + xfsQuotaDirPrifix + "_testid2/",
		"/xfs/disk3/" + xfsQuotaDirPrifix + "_testid3/",
	}
	quotaProject1 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[0] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    5 * 1024 * 1024,
		ProjectName: "k8spro1",
		ProjectId:   1,
		Id:          "testid1",
		KeepId:      xfsKeepForOnePodFlag + "-testsubid1/",
		SubId:       "",
		OwnerId:     "testownerid1",
	}
	quotaProject2 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[1] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro2",
		ProjectId:   2,
		Id:          "testid2",
		KeepId:      xfsKeepForOnePodFlag + "-testsubid2/",
		SubId:       "",
		OwnerId:     "testownerid1",
	}
	quotaProject3 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[2] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro3",
		ProjectId:   3,
		Id:          "testid3",
		KeepId:      xfsKeepForOnePodFlag + "-testsubid3/",
		SubId:       "",
		OwnerId:     "testownerid1",
	}
	getActiveFun := func(ret bool) ActiveFun {
		return func(id, pvid string) bool {
			return ret
		}
	}
	addProjects := []struct {
		HardQuota       int64
		SoftQuota       int64
		CanAdd          bool
		TestName        string
		Id              string
		SubId           string
		OwnerId         string
		ShouldMountPath string
		Devices         []utilequota.FakeXFSDevice
		ExistsProject   []utilequota.FakeXFSQuotaProject
		ExistsPaths     []string
	}{
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: "/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1/" + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   make([]utilequota.FakeXFSQuotaProject, 0),
			ExistsPaths:     []string{},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotapaths[0] + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject1},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid2",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotapaths[1] + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject2},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid3",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotapaths[2] + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject3},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
	}

	for _, project := range addProjects {

		xfsQuotaRunner := utilequota.NewFakeXFSQuotaRunner(project.Devices, []utilequota.FakeXFSQuotaProject{}, project.ExistsPaths, "")

		quotaManager := newXFSQuotaManager("/xfs", xfsQuotaRunner)

		for _, existsProject := range project.ExistsProject {
			ok, p, err := quotaManager.AddPathQuota(existsProject.OwnerId, existsProject.Id, existsProject.SubId, "", "", false,
				existsProject.SoftQuota, existsProject.HardQuota, getActiveFun(true))
			if ok == false || path.Clean(p) != path.Clean(existsProject.MountPath) {
				t.Errorf("existsProject add error ok=%t, path:%s, existsProject.MountPath:%s, err=%v",
					ok, path.Clean(p), path.Clean(existsProject.MountPath), err)
			}
		}
		ok, p, addErr := quotaManager.AddPathQuota(project.OwnerId, project.Id, project.SubId, "", "", false,
			project.SoftQuota, project.HardQuota, getActiveFun(true))
		if ok != project.CanAdd {
			t.Errorf("add project %s should add %t, but add %t, err=%v", project.Id, project.CanAdd, ok, addErr)
		} else if ok == true {
			if path.Clean(p) != path.Clean(project.ShouldMountPath) {
				t.Errorf("should add to path %s not to %s", path.Clean(project.ShouldMountPath), path.Clean(p))
			}
		}
	}
}

// test following case
// io.enndata.user/alpha-pvhostpathmountpolicy:none
// io.enndata.kubelet/alpha-pvhostpathquotaforonepod:true
func TestXFSQuotaManagerAddNoneForOnePodTest(t *testing.T) {
	device1 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdb1",
		MountPath:    "/xfs/disk1",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	device2 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdc1",
		MountPath:    "/xfs/disk2",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     150 * 1024 * 1024, // 100MB
	}
	device3 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdd1",
		MountPath:    "/xfs/disk3",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     200 * 1024 * 1024, // 100MB
	}
	quotapaths := []string{
		"/xfs/disk1/" + xfsQuotaDirPrifix + "_testid1_testsubid1/",
		"/xfs/disk2/" + xfsQuotaDirPrifix + "_testid2_testsubid2/",
		"/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1_testsubid3/",
	}
	quotaProject1 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[0] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    5 * 1024 * 1024,
		ProjectName: "k8spro1",
		ProjectId:   1,
		Id:          "testid1",
		SubId:       "testsubid1",
		OwnerId:     "testownerid1",
	}
	quotaProject2 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[1] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro2",
		ProjectId:   2,
		Id:          "testid2",
		SubId:       "testsubid2",
		OwnerId:     "testownerid1",
	}
	quotaProject3 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[2] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro3",
		ProjectId:   3,
		Id:          "testid1",
		SubId:       "testsubid3",
		OwnerId:     "testownerid1",
	}
	getActiveFun := func(ret bool) ActiveFun {
		return func(id, pvid string) bool {
			return ret
		}
	}
	addProjects := []struct {
		HardQuota       int64
		SoftQuota       int64
		CanAdd          bool
		TestName        string
		Id              string
		SubId           string
		OwnerId         string
		ShouldMountPath string
		Devices         []utilequota.FakeXFSDevice
		ExistsProject   []utilequota.FakeXFSQuotaProject
		ExistsPaths     []string
	}{
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "testsubid1",
			OwnerId:         "testownerid1",
			ShouldMountPath: "/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1_testsubid1/" + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   make([]utilequota.FakeXFSQuotaProject, 0),
			ExistsPaths:     []string{},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "testsubid4",
			OwnerId:         "testownerid1",
			ShouldMountPath: "/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1_testsubid4/" + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2, quotaProject3},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
	}

	for _, project := range addProjects {

		xfsQuotaRunner := utilequota.NewFakeXFSQuotaRunner(project.Devices, []utilequota.FakeXFSQuotaProject{}, project.ExistsPaths, "")

		quotaManager := newXFSQuotaManager("/xfs", xfsQuotaRunner)

		for _, existsProject := range project.ExistsProject {
			ok, p, err := quotaManager.AddPathQuota(existsProject.OwnerId, existsProject.Id, existsProject.SubId, "", "", false,
				existsProject.SoftQuota, existsProject.HardQuota, getActiveFun(true))
			if ok == false || path.Clean(p) != path.Clean(existsProject.MountPath) {
				t.Errorf("existsProject add error ok=%t, path:%s, existsProject.MountPath:%s, err=%v",
					ok, path.Clean(p), path.Clean(existsProject.MountPath), err)
			}
		}
		ok, p, addErr := quotaManager.AddPathQuota(project.OwnerId, project.Id, project.SubId, "", "", false,
			project.SoftQuota, project.HardQuota, getActiveFun(true))
		if ok != project.CanAdd {
			t.Errorf("add project %s should add %t, but add %t, err=%v", project.Id, project.CanAdd, ok, addErr)
		} else if ok == true {
			if path.Clean(p) != path.Clean(project.ShouldMountPath) {
				t.Errorf("should add to path %s not to %s", path.Clean(project.ShouldMountPath), path.Clean(p))
			}
		}
	}
}

// test following case
// io.enndata.user/alpha-pvhostpathmountpolicy:none
// io.enndata.kubelet/alpha-pvhostpathquotaforonepod:false
func TestXFSQuotaManagerAddNoneNotForOnePodTest(t *testing.T) {
	device1 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdb1",
		MountPath:    "/xfs/disk1",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     100 * 1024 * 1024, // 100MB
	}
	device2 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdc1",
		MountPath:    "/xfs/disk2",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     150 * 1024 * 1024, // 100MB
	}
	device3 := utilequota.FakeXFSDevice{
		Device:       "/dev/sdd1",
		MountPath:    "/xfs/disk3",
		UserQuotaOn:  false,
		GroupQuotaOn: false,
		ProjQuotaOn:  true,
		Capacity:     200 * 1024 * 1024, // 100MB
	}
	quotapaths := []string{
		"/xfs/disk1/" + xfsQuotaDirPrifix + "_testid1/",
		"/xfs/disk2/" + xfsQuotaDirPrifix + "_testid2/",
		"/xfs/disk3/" + xfsQuotaDirPrifix + "_testid3/",
	}
	quotaProject1 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[0] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    5 * 1024 * 1024,
		ProjectName: "k8spro1",
		ProjectId:   1,
		Id:          "testid1",
		SubId:       "",
		OwnerId:     "testownerid1",
	}
	quotaProject2 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[1] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro2",
		ProjectId:   2,
		Id:          "testid2",
		SubId:       "",
		OwnerId:     "testownerid1",
	}
	quotaProject3 := utilequota.FakeXFSQuotaProject{
		MountPath:   quotapaths[2] + xfsKeepForOnePodInnerDir,
		SoftQuota:   10 * 1024 * 1024,
		HardQuota:   10 * 1024 * 1024,
		UsedSize:    10 * 1024 * 1024,
		ProjectName: "k8spro3",
		ProjectId:   3,
		Id:          "testid3",
		SubId:       "",
		OwnerId:     "testownerid1",
	}
	getActiveFun := func(ret bool) ActiveFun {
		return func(id, pvid string) bool {
			return ret
		}
	}
	addProjects := []struct {
		HardQuota       int64
		SoftQuota       int64
		CanAdd          bool
		TestName        string
		Id              string
		SubId           string
		OwnerId         string
		ShouldMountPath string
		Devices         []utilequota.FakeXFSDevice
		ExistsProject   []utilequota.FakeXFSQuotaProject
		ExistsPaths     []string
	}{
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: "/xfs/disk3/" + xfsQuotaDirPrifix + "_testid1/" + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   make([]utilequota.FakeXFSQuotaProject, 0),
			ExistsPaths:     []string{},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid4",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: "/xfs/disk3/" + xfsQuotaDirPrifix + "_testid4/" + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2, quotaProject3},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid1",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotapaths[0] + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2, quotaProject3},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid2",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotapaths[1] + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2, quotaProject3},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
		{
			HardQuota:       100 * 1024 * 1024,
			SoftQuota:       100 * 1024 * 1024,
			CanAdd:          true,
			Id:              "testid3",
			SubId:           "",
			OwnerId:         "testownerid1",
			ShouldMountPath: quotapaths[2] + xfsKeepForOnePodInnerDir,
			Devices:         []utilequota.FakeXFSDevice{device1, device2, device3},
			ExistsProject:   []utilequota.FakeXFSQuotaProject{quotaProject1, quotaProject2, quotaProject3},
			ExistsPaths:     []string{quotapaths[0], quotapaths[1], quotapaths[2]},
		},
	}

	for _, project := range addProjects {

		xfsQuotaRunner := utilequota.NewFakeXFSQuotaRunner(project.Devices, []utilequota.FakeXFSQuotaProject{}, project.ExistsPaths, "")

		quotaManager := newXFSQuotaManager("/xfs", xfsQuotaRunner)

		for _, existsProject := range project.ExistsProject {
			ok, p, err := quotaManager.AddPathQuota(existsProject.OwnerId, existsProject.Id, existsProject.SubId, "", "", false,
				existsProject.SoftQuota, existsProject.HardQuota, getActiveFun(true))
			if ok == false || path.Clean(p) != path.Clean(existsProject.MountPath) {
				t.Errorf("existsProject add error ok=%t, path:%s, existsProject.MountPath:%s, err=%v",
					ok, path.Clean(p), path.Clean(existsProject.MountPath), err)
			}
		}
		ok, p, addErr := quotaManager.AddPathQuota(project.OwnerId, project.Id, project.SubId, "", "", false,
			project.SoftQuota, project.HardQuota, getActiveFun(true))
		if ok != project.CanAdd {
			t.Errorf("add project %s should add %t, but add %t, err=%v", project.Id, project.CanAdd, ok, addErr)
		} else if ok == true {
			if path.Clean(p) != path.Clean(project.ShouldMountPath) {
				t.Errorf("should add to path %s not to %s", path.Clean(project.ShouldMountPath), path.Clean(p))
			}
		}
	}
}
