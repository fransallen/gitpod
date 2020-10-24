// Copyright (c) 2020 TypeFox GmbH. All rights reserved.
// Licensed under the GNU Affero General Public License (AGPL).
// See License-AGPL.txt in the project root for license information.

package ports

import (
	"context"
	"io"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/gitpod-io/gitpod/supervisor/api"
	"github.com/gitpod-io/gitpod/supervisor/pkg/gitpod"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestPortsUpdateState(t *testing.T) {
	type ExposureExpectation []ExposedPort
	type UpdateExpectation []*Diff
	type Change struct {
		Config     *gitpod.GitpodConfig
		Served     []ServedPort
		Exposed    []ExposedPort
		ConfigErr  error
		ServedErr  error
		ExposedErr error
	}
	tests := []struct {
		Desc             string
		InternalPorts    []uint32
		WorkspacePorts   []*gitpod.PortConfig
		Changes          []Change
		ExpectedExposure ExposureExpectation
		ExpectedUpdates  UpdateExpectation
	}{
		{
			Desc: "basic locally served",
			Changes: []Change{
				{Served: []ServedPort{{8080, true}}},
				{Exposed: []ExposedPort{{LocalPort: 8080, GlobalPort: 60000}}},
				{Served: []ServedPort{{8080, true}, {60000, false}}},
				{Served: []ServedPort{{60000, false}}},
				{Served: []ServedPort{}},
			},
			ExpectedExposure: []ExposedPort{
				{LocalPort: 8080, GlobalPort: 60000},
			},
			ExpectedUpdates: UpdateExpectation{
				{Added: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 60000, Served: true}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 60000, Served: true, Exposed: &api.PortsStatus_ExposedPortInfo{OnExposed: api.PortsStatus_ExposedPortInfo_notify_private}}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 60000, Served: false, Exposed: &api.PortsStatus_ExposedPortInfo{OnExposed: api.PortsStatus_ExposedPortInfo_notify_private}}}},
			},
		},
		{
			Desc: "basic globally served",
			Changes: []Change{
				{Served: []ServedPort{{8080, false}}},
				{Served: []ServedPort{}},
			},
			ExpectedExposure: []ExposedPort{
				{LocalPort: 8080, GlobalPort: 8080},
			},
			ExpectedUpdates: UpdateExpectation{
				{Added: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 8080, Served: true}}},
				{Removed: []uint32{8080}},
			},
		},
		{
			Desc: "basic port publically exposed",
			Changes: []Change{
				{Exposed: []ExposedPort{{LocalPort: 8080, GlobalPort: 8080, Public: false, URL: "foobar"}}},
				{Exposed: []ExposedPort{{LocalPort: 8080, GlobalPort: 8080, Public: true, URL: "foobar"}}},
				{Served: []ServedPort{{Port: 8080}}},
			},
			ExpectedUpdates: UpdateExpectation{
				{Added: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 8080, Exposed: &api.PortsStatus_ExposedPortInfo{Public: false, Url: "foobar", OnExposed: api.PortsStatus_ExposedPortInfo_notify_private}}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 8080, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, Url: "foobar", OnExposed: api.PortsStatus_ExposedPortInfo_notify_private}}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 8080, Served: true, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, Url: "foobar", OnExposed: api.PortsStatus_ExposedPortInfo_notify_private}}}},
			},
		},
		{
			Desc:          "internal ports served",
			InternalPorts: []uint32{8080},
			Changes: []Change{
				{Served: []ServedPort{}},
				{Served: []ServedPort{{8080, false}}},
			},

			ExpectedExposure: ExposureExpectation(nil),
			ExpectedUpdates:  UpdateExpectation(nil),
		},
		{
			Desc: "serving configured workspace port",
			WorkspacePorts: []*gitpod.PortConfig{
				{Port: 8080, OnOpen: "open-browser"},
				{Port: 9229, OnOpen: "ignore", Visibility: "private"},
			},
			Changes: []Change{
				{
					Exposed: []ExposedPort{
						{LocalPort: 8080, GlobalPort: 8080, Public: true, URL: "8080-foobar"},
						{LocalPort: 9229, GlobalPort: 9229, Public: false, URL: "9229-foobar"},
					},
				},
				{
					Served: []ServedPort{
						{8080, false},
						{9229, true},
					},
				},
			},
			ExpectedExposure: []ExposedPort{
				{LocalPort: 8080, Public: true},
				{LocalPort: 9229},
				{LocalPort: 9229, GlobalPort: 60000},
			},
			ExpectedUpdates: UpdateExpectation{
				{Added: []*api.PortsStatus{{LocalPort: 8080}, {LocalPort: 9229}}},
				{Updated: []*api.PortsStatus{
					{LocalPort: 8080, GlobalPort: 8080, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, Url: "8080-foobar", OnExposed: api.PortsStatus_ExposedPortInfo_open_browser}},
					{LocalPort: 9229, GlobalPort: 9229, Exposed: &api.PortsStatus_ExposedPortInfo{Public: false, Url: "9229-foobar", OnExposed: api.PortsStatus_ExposedPortInfo_ignore}},
				}},
				{Updated: []*api.PortsStatus{
					{LocalPort: 8080, GlobalPort: 8080, Served: true, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, Url: "8080-foobar", OnExposed: api.PortsStatus_ExposedPortInfo_open_browser}},
					{LocalPort: 9229, GlobalPort: 60000, Served: true, Exposed: &api.PortsStatus_ExposedPortInfo{Public: false, Url: "9229-foobar", OnExposed: api.PortsStatus_ExposedPortInfo_ignore}},
				}},
			},
		},
		{
			Desc: "filter out irrelevan configuration changes",
			Changes: []Change{
				{
					Config: &gitpod.GitpodConfig{
						Ide:   "theia",
						Ports: []*gitpod.PortsItems{{Port: 8080}},
					},
				},
				{
					Config: &gitpod.GitpodConfig{
						Ide:   "code",
						Ports: []*gitpod.PortsItems{{Port: 8080}},
					},
				},
			},
			ExpectedExposure: []ExposedPort{
				{LocalPort: 8080, Public: true},
			},
			ExpectedUpdates: UpdateExpectation{
				{Added: []*api.PortsStatus{{LocalPort: 8080}}},
			},
		},
		{
			Desc:           "auto expose configured ports",
			WorkspacePorts: []*gitpod.PortConfig{{Port: 8080, Visibility: "private"}},
			Changes: []Change{
				{
					Exposed: []ExposedPort{{LocalPort: 8080, GlobalPort: 8080, Public: false, URL: "foobar"}},
				},
				{
					Exposed: []ExposedPort{{LocalPort: 8080, GlobalPort: 8080, Public: true, URL: "foobar"}},
				},
				{
					Served: []ServedPort{{8080, true}},
				},
				{
					Exposed: []ExposedPort{{LocalPort: 8080, GlobalPort: 60000, Public: true, URL: "foobar"}},
				},
				{
					Served: []ServedPort{{8080, true}, {60000, false}},
				},
				{
					Served: []ServedPort{{60000, false}},
				},
				{
					Served: []ServedPort{},
				},
				{
					Served: []ServedPort{{8080, false}},
				},
			},
			ExpectedExposure: []ExposedPort{
				{LocalPort: 8080, Public: false},
				{LocalPort: 8080, GlobalPort: 60000, Public: true},
				{LocalPort: 8080, GlobalPort: 8080, Public: true},
			},
			ExpectedUpdates: UpdateExpectation{
				{Added: []*api.PortsStatus{{LocalPort: 8080}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 8080, Exposed: &api.PortsStatus_ExposedPortInfo{Public: false, OnExposed: api.PortsStatus_ExposedPortInfo_notify, Url: "foobar"}}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 8080, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, OnExposed: api.PortsStatus_ExposedPortInfo_notify, Url: "foobar"}}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 60000, Served: true, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, OnExposed: api.PortsStatus_ExposedPortInfo_notify, Url: "foobar"}}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 60000, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, OnExposed: api.PortsStatus_ExposedPortInfo_notify, Url: "foobar"}}}},
				{Updated: []*api.PortsStatus{{LocalPort: 8080, GlobalPort: 8080, Served: true, Exposed: &api.PortsStatus_ExposedPortInfo{Public: true, OnExposed: api.PortsStatus_ExposedPortInfo_notify, Url: "foobar"}}}},
			},
		},
		{
			Desc: "starting multiple proxies for the same served event",
			Changes: []Change{
				{
					Served: []ServedPort{{8080, true}, {3000, true}},
				},
			},
			ExpectedExposure: []ExposedPort{
				{LocalPort: 8080, GlobalPort: 60000},
				{LocalPort: 3000, GlobalPort: 59999},
			},
			ExpectedUpdates: UpdateExpectation{
				{Added: []*api.PortsStatus{
					{LocalPort: 8080, GlobalPort: 60000, Served: true},
					{LocalPort: 3000, GlobalPort: 59999, Served: true},
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			var (
				exposed = &testExposedPorts{
					Changes: make(chan []ExposedPort),
					Error:   make(chan error),
				}
				served = &testServedPorts{
					Changes: make(chan []ServedPort),
					Error:   make(chan error),
				}

				context, cancel = context.WithCancel(context.Background())
				configService   = &testGitpodConfigService{
					configs: make(chan *gitpod.GitpodConfig),
					errors:  make(chan error),
				}
				ctrl        = gomock.NewController(t)
				gitpodAPI   = gitpod.NewMockAPIInterface(ctrl)
				workspaceID = "test"
				config      = NewConfigService(workspaceID, configService, gitpodAPI)

				pm    = NewManager(exposed, served, config, test.InternalPorts...)
				updts []*Diff
			)
			gitpodAPI.EXPECT().GetWorkspace(context, workspaceID).Times(1).Return(&gitpod.WorkspaceInfo{
				Workspace: &gitpod.Workspace{
					Config: &gitpod.WorkspaceConfig{
						Ports: test.WorkspacePorts,
					},
				},
			}, nil)
			pm.proxyStarter = func(localPort uint32, globalPort uint32) (io.Closer, error) {
				return ioutil.NopCloser(nil), nil
			}

			var wg sync.WaitGroup
			wg.Add(3)
			go func() {
				defer wg.Done()
				pm.Run()
			}()
			go func() {
				defer wg.Done()
				defer cancel()
				defer close(configService.configs)
				defer close(configService.errors)
				defer close(served.Error)
				defer close(served.Changes)
				defer close(exposed.Error)
				defer close(exposed.Changes)

				configService.configs <- nil
				for _, c := range test.Changes {
					if c.Config != nil {
						configService.configs <- c.Config
					} else if c.ConfigErr != nil {
						configService.errors <- c.ConfigErr
					} else if c.Served != nil {
						served.Changes <- c.Served
					} else if c.ServedErr != nil {
						served.Error <- c.ServedErr
					} else if c.Exposed != nil {
						exposed.Changes <- c.Exposed
					} else if c.ExposedErr != nil {
						exposed.Error <- c.ExposedErr
					}
				}
			}()
			go func() {
				defer wg.Done()

				sub := pm.Subscribe()
				defer sub.Close()

				for up := range sub.Updates() {
					updts = append(updts, up)
				}
			}()

			wg.Wait()

			sortExposed := cmpopts.SortSlices(func(x, y ExposedPort) bool { return x.LocalPort < y.LocalPort })
			if diff := cmp.Diff(test.ExpectedExposure, ExposureExpectation(exposed.Exposures), sortExposed); diff != "" {
				t.Errorf("unexpected exposures (-want +got):\n%s", diff)
			}

			sorPorts := cmpopts.SortSlices(func(x, y uint32) bool { return x < y })
			sortPortStatus := cmpopts.SortSlices(func(x, y *api.PortsStatus) bool { return x.LocalPort < y.LocalPort })
			if diff := cmp.Diff(test.ExpectedUpdates, UpdateExpectation(updts), sorPorts, sortPortStatus); diff != "" {
				t.Errorf("unexpected updates (-want +got):\n%s", diff)
			}
		})
	}
}

type testExposedPorts struct {
	Changes chan []ExposedPort
	Error   chan error

	Exposures []ExposedPort
	mu        sync.Mutex
}

func (tep *testExposedPorts) Observe(ctx context.Context) (<-chan []ExposedPort, <-chan error) {
	return tep.Changes, tep.Error
}

func (tep *testExposedPorts) Expose(ctx context.Context, local, global uint32, public bool) error {
	tep.mu.Lock()
	defer tep.mu.Unlock()

	tep.Exposures = append(tep.Exposures, ExposedPort{
		GlobalPort: global,
		LocalPort:  local,
		Public:     public,
	})
	return nil
}

type testServedPorts struct {
	Changes chan []ServedPort
	Error   chan error
}

func (tps *testServedPorts) Observe(ctx context.Context) (<-chan []ServedPort, <-chan error) {
	return tps.Changes, tps.Error
}
