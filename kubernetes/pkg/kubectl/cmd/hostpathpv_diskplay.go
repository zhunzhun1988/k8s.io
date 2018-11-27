/*Copyright 2017 The Kubernetes Authors.

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
	"fmt"
)

type Displayer struct {
	titles       []string
	content      [][]string
	space        string
	prefix       string
	maxColumeLen []int
}

func NewDisplayer(space, prefix string, titles ...string) *Displayer {
	maxColumeLen := make([]int, 0, len(titles))
	for _, title := range titles {
		maxColumeLen = append(maxColumeLen, len(title))
	}
	return &Displayer{
		space:        space,
		prefix:       prefix,
		titles:       titles[0:],
		content:      make([][]string, 0),
		maxColumeLen: maxColumeLen,
	}
}

func (d *Displayer) AddLine(strs ...string) error {
	if len(strs) != len(d.titles) {
		return fmt.Errorf("content len err")
	}
	d.content = append(d.content, strs[0:])
	for i, content := range strs {
		l := len(content)
		if l > d.maxColumeLen[i] {
			d.maxColumeLen[i] = l
		}
	}
	return nil
}
func (d *Displayer) AddCutOffLine(str string) {
	strs := make([]string, len(d.titles))
	for i, title := range d.titles {
		strs[i] = getNumSpace(len(title), str)
	}
	d.AddLine(strs...)
}

func SyncDisplayersColumeLen(displayers []*Displayer) {
	syncMaxColumeLen := make([]int, 100)
	var columeNum int
	for c := 0; c < 100; c++ {
		needBreak := true
		for i := 0; i < len(displayers); i++ {
			if len(displayers[i].maxColumeLen) > c {
				syncMaxColumeLen[c] = maxint(syncMaxColumeLen[c], displayers[i].maxColumeLen[c])
				needBreak = false
			}
		}
		if needBreak {
			columeNum = c
			break
		}
	}
	for c := 0; c < columeNum; c++ {
		for i := 0; i < len(displayers); i++ {
			if len(displayers[i].maxColumeLen) > c {
				displayers[i].maxColumeLen[c] = syncMaxColumeLen[c]
			}
		}
	}
}

func (d *Displayer) Print(needUnderLine bool) {
	for i, title := range d.titles {
		fmt.Printf("%s%s%s%s", d.prefix, title, getNumSpace(d.maxColumeLen[i]-len(title), " "), d.space)
	}
	fmt.Printf("\n")
	if needUnderLine {
		for i := 0; i < len(d.titles); i++ {
			fmt.Printf("%s%s%s", d.prefix, getNumSpace(d.maxColumeLen[i], "-"), d.space)
		}
		fmt.Printf("\n")
	}

	for _, lineContent := range d.content {
		for i, content := range lineContent {
			fmt.Printf("%s%s%s%s", d.prefix, content, getNumSpace(d.maxColumeLen[i]-len(content), " "), d.space)
		}
		fmt.Printf("\n")
	}
	/*if needUnderLine {
		for i := 0; i < len(d.titles); i++ {
			fmt.Printf("%s%s%s", d.prefix, getNumSpace(d.maxColumeLen[i], "-"), d.space)
		}
		fmt.Printf("\n")
	}*/
}

func getNumSpace(num int, space string) string {
	ret := ""
	for i := 0; i < num; i++ {
		ret += space
	}
	return ret
}
