// Copyright (c) 2020 TypeFox GmbH. All rights reserved.
// Licensed under the GNU Affero General Public License (AGPL).
// See License-AGPL.txt in the project root for license information.

package ports

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/gitpod-io/gitpod/common-go/log"
	"github.com/gitpod-io/gitpod/supervisor/api"
	"github.com/gitpod-io/gitpod/supervisor/pkg/gitpod"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/xerrors"
)

const (
	// proxyPortRange is the port range in which we'll try to find
	// ports for proxying localhost-only services.
	proxyPortRangeLo uint32 = 50000
	proxyPortRangeHi uint32 = 60000
)

// NewManager creates a new port manager
func NewManager(exposed ExposedPortsInterface, served ServedPortsObserver, config *ConfigService, internalPorts ...uint32) *Manager {
	state := make(map[uint32]*managedPort)
	internal := make(map[uint32]struct{})
	for _, p := range internalPorts {
		internal[p] = struct{}{}
	}

	return &Manager{
		E: exposed,
		S: served,
		C: config,

		internal: internal,
		proxies:  make(map[uint32]*localhostProxy),

		state:         state,
		subscriptions: make(map[*Subscription]struct{}),
		proxyStarter:  startLocalhostProxy,
	}
}

type localhostProxy struct {
	io.Closer
	proxyPort uint32
}

// Manager brings together served and exposed ports. It keeps track of which port is exposed, which one is served,
// auto-exposes ports and proxies ports served on localhost only.
type Manager struct {
	E ExposedPortsInterface
	S ServedPortsObserver
	C *ConfigService

	internal     map[uint32]struct{}
	proxies      map[uint32]*localhostProxy
	proxyStarter func(LocalhostPort uint32, GlobalPort uint32) (proxy io.Closer, err error)

	configs *Configs
	exposed []ExposedPort
	served  []ServedPort

	state         map[uint32]*managedPort
	subscriptions map[*Subscription]struct{}
	mu            sync.RWMutex
}

type managedPort struct {
	Served    bool
	Exposed   bool
	Public    bool
	URL       string
	OnExposed api.PortsStatus_ExposedPortInfo_OnPortExposed

	LocalhostPort uint32
	GlobalPort    uint32
}

// Diff provides the diff against previous state
type Diff struct {
	Added   []*api.PortsStatus
	Updated []*api.PortsStatus
	Removed []uint32
}

// Subscription is a Subscription to status updates
type Subscription struct {
	updates chan *Diff
	Close   func() error
}

// Updates returns the updates channel
func (s *Subscription) Updates() <-chan *Diff {
	return s.updates
}

// Run starts the port manager which keeps running until one of its observers stops.
func (pm *Manager) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		// We copy the subscriptions to a list prior to closing them, to prevent a data race
		// between the map iteration and entry removal when closing the subscription.
		pm.mu.RLock()
		subs := make([]*Subscription, 0, len(pm.subscriptions))
		for s := range pm.subscriptions {
			subs = append(subs, s)
		}
		pm.mu.RUnlock()

		for _, s := range subs {
			s.Close()
		}
	}()
	defer cancel()

	exposedUpdates, exposedErrors := pm.E.Observe(ctx)
	servedUpdates, servedErrors := pm.S.Observe(ctx)
	configUpdates, configErrors := pm.C.Observe(ctx)
	for {
		select {
		case exposed := <-exposedUpdates:
			if exposed == nil {
				log.Error("exposed ports observer stopped")
				return
			}
			pm.mu.Lock()
			if !cmp.Equal(pm.exposed, exposed) {
				pm.exposed = exposed
				pm.updateState()
			}
			pm.mu.Unlock()
		case served := <-servedUpdates:
			if served == nil {
				log.Error("served ports observer stopped")
				return
			}
			pm.mu.Lock()
			if !cmp.Equal(pm.served, served) {
				pm.served = served
				pm.updateProxies()
				pm.updateState()
			}
			pm.mu.Unlock()
		case configs := <-configUpdates:
			if configs == nil {
				log.Error("configured ports observer stopped")
				return
			}
			pm.mu.Lock()
			pm.configs = configs
			pm.updateState()
			pm.mu.Unlock()
		case err := <-exposedErrors:
			if err == nil {
				log.Error("exposed ports observer stopped")
				return
			}
			log.WithError(err).Warn("error while observing exposed ports")
		case err := <-servedErrors:
			if err == nil {
				log.Error("served ports observer stopped")
				return
			}
			log.WithError(err).Warn("error while observing served ports")
		case err := <-configErrors:
			if err == nil {
				log.Error("port configs observer stopped")
				return
			}
			log.WithError(err).Warn("error while observing served port configs")
		}
	}
}

// Status provides the current port status
func (pm *Manager) Status() []*api.PortsStatus {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return pm.getStatus()
}

func (pm *Manager) updateProxies() {
	opened := make(map[uint32]struct{}, len(pm.served))
	for _, p := range pm.served {
		opened[p.Port] = struct{}{}
	}

	for localPort, proxy := range pm.proxies {
		globalPort := proxy.proxyPort
		_, exists := opened[globalPort]
		if exists {
			continue
		}

		err := proxy.Close()
		if err != nil {
			log.WithError(err).WithField("port", globalPort).Warn("cannot stop localhost proxy")
		}
		delete(pm.proxies, localPort)
		delete(pm.internal, globalPort)
	}

	for _, served := range pm.served {
		localPort := served.Port
		_, exists := pm.proxies[localPort]
		if exists || !served.BoundToLocalhost {
			continue
		}

		var globalPort uint32
		for port := proxyPortRangeHi; port >= proxyPortRangeLo; port-- {
			if _, used := opened[port]; used {
				continue
			}
			if _, used := pm.internal[port]; used {
				continue
			}

			globalPort = port
			break
		}
		if globalPort == 0 {
			log.WithField("port", localPort).Error("cannot find a free proxy port")
			continue
		}

		proxy, err := pm.proxyStarter(localPort, globalPort)
		if err != nil {
			log.WithError(err).WithField("port", served.Port).Warn("cannot start localhost proxy")
			continue
		}

		pm.internal[globalPort] = struct{}{}
		pm.proxies[localPort] = &localhostProxy{
			Closer:    proxy,
			proxyPort: globalPort,
		}
	}
}

func (pm *Manager) updateState() {
	var added, updated, removed []uint32
	defer func() {
		pm.publishStatus(added, updated, removed)
	}()

	newState := pm.nextState()
	for port := range newState {
		_, exists := pm.state[port]
		if !exists {
			added = append(added, port)
		}
	}
	for port, mp := range pm.state {
		newMp, exists := newState[port]
		if !exists {
			removed = append(removed, port)
			continue
		}
		if cmp.Equal(newMp, mp) {
			continue
		}
		updated = append(updated, port)
	}
	pm.state = newState
}

func (pm *Manager) nextState() map[uint32]*managedPort {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := make(map[uint32]*managedPort)

	// 1. capture exposed since we don't want to auto expose configured again
	for _, exposed := range pm.exposed {
		port := exposed.LocalPort
		if pm.boundInternally(port) {
			continue
		}

		state[port] = &managedPort{
			LocalhostPort: port,
			GlobalPort:    exposed.GlobalPort,
			Exposed:       true,
			Public:        exposed.Public,
			URL:           exposed.URL,
			OnExposed:     getOnExposed(nil, port),
		}
	}

	// 2. capture all directly configured ports
	if pm.configs != nil {
		pm.configs.ForEach(func(port uint32, config *gitpod.PortConfig) {
			if pm.boundInternally(port) {
				return
			}

			mp, exists := state[port]
			if !exists {
				mp = &managedPort{}
				state[port] = mp
			}

			mp.LocalhostPort = port
			mp.OnExposed = getOnExposed(config, port)
			if mp.Exposed {
				return
			}
			mp.Public = config.Visibility != "private"
			err := pm.E.Expose(ctx, mp.LocalhostPort, mp.GlobalPort, mp.Public)
			if err != nil {
				log.WithError(err).WithField("port", *mp).Warn("cannot auto-expose port")
				return
			}
			log.WithField("port", *mp).Warn("auto-expose port")
		})
	}

	// 3. capture served ports and auto expose ports as needed
	for _, served := range pm.served {
		port := served.Port
		if pm.boundInternally(port) {
			continue
		}

		mp, exists := state[port]
		if !exists {
			mp = &managedPort{}
			state[port] = mp
		}

		mp.LocalhostPort = port
		mp.Served = true

		exposedGlobalPort := mp.GlobalPort
		if served.BoundToLocalhost {
			proxy, exists := pm.proxies[port]
			if exists {
				mp.GlobalPort = proxy.proxyPort
			} else {
				mp.GlobalPort = 0
			}
		} else {
			// we don't need a proxy - the port is globally bound
			mp.GlobalPort = port
		}

		if mp.GlobalPort == 0 || (mp.Exposed && mp.GlobalPort == exposedGlobalPort) {
			continue
		}

		var public bool
		_, configured := pm.configs.Get(mp.LocalhostPort)
		if mp.Exposed || configured {
			public = mp.Public
		} else {
			config, exists := pm.configs.GetFromRange(port)
			public = exists && config.Visibility != "private"
		}

		err := pm.E.Expose(ctx, mp.LocalhostPort, mp.GlobalPort, public)
		if err != nil {
			log.WithError(err).WithField("port", *mp).Warn("cannot auto-expose port")
			continue
		}
		log.WithField("port", *mp).Warn("auto-expose port")
	}
	return state
}

func getOnExposed(config *gitpod.PortConfig, port uint32) api.PortsStatus_ExposedPortInfo_OnPortExposed {
	if config == nil {
		// anything above 32767 seems odd (e.g. used by language servers)
		unusualRange := !(0 < port && port < 32767)
		wellKnown := port <= 10000
		if unusualRange || !wellKnown {
			return api.PortsStatus_ExposedPortInfo_ignore
		}
		return api.PortsStatus_ExposedPortInfo_notify_private
	}
	if config.OnOpen == "ignore" {
		return api.PortsStatus_ExposedPortInfo_ignore
	}
	if config.OnOpen == "open-browser" {
		return api.PortsStatus_ExposedPortInfo_open_browser
	}
	if config.OnOpen == "open-preview" {
		return api.PortsStatus_ExposedPortInfo_open_preview
	}
	return api.PortsStatus_ExposedPortInfo_notify
}

func (pm *Manager) boundInternally(port uint32) bool {
	_, exists := pm.internal[port]
	return exists
}

// Expose exposes a port
func (pm *Manager) Expose(port uint32, targetPort uint32) string {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	mp, ok := pm.state[port]
	if ok {
		if mp.Exposed {
			return ""
		}
		if pm.boundInternally(port) {
			return "internal service cannot be exposed"
		}
	}

	if pm.configs != nil {
		_, exists := pm.configs.Get(port)
		if exists {
			// will be auto-exposed
			return ""
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	global := targetPort
	if global == 0 {
		global = port
	}
	config, exists := pm.configs.GetFromRange(port)
	public := exists && config.Visibility != "private"
	err := pm.E.Expose(ctx, port, global, public)
	if err == nil {
		return ""
	}
	log.WithError(err).WithField("port", port).WithField("targetPort", targetPort).Error("cannot expose port")
	return err.Error()
}

// Subscribe subscribes for status updates
func (pm *Manager) Subscribe() *Subscription {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if len(pm.subscriptions) > maxSubscriptions {
		return nil
	}

	sub := &Subscription{updates: make(chan *Diff, 5)}
	var once sync.Once
	sub.Close = func() error {
		pm.mu.Lock()
		defer pm.mu.Unlock()

		once.Do(func() { close(sub.updates) })
		delete(pm.subscriptions, sub)

		return nil
	}
	pm.subscriptions[sub] = struct{}{}

	return sub
}

// publishStatus pushes status updates to all subscribers.
// Callers are expected to hold mu.
func (pm *Manager) publishStatus(added []uint32, updated []uint32, removed []uint32) {
	if len(added) == 0 && len(updated) == 0 && len(removed) == 0 {
		return
	}

	diff := &Diff{Removed: removed}
	for _, port := range added {
		diff.Added = append(diff.Added, pm.getPortStatus(port))
	}
	for _, port := range updated {
		diff.Updated = append(diff.Updated, pm.getPortStatus(port))
	}

	log.WithField("ports", fmt.Sprintf("%+v", diff)).Debug("ports changed")

	for sub := range pm.subscriptions {
		select {
		case sub.updates <- diff:
		default:
			log.Warn("cannot to push ports update to a subscriber")
		}
	}
}

// getStatus produces an API compatible port status list.
// Callers are expected to hold mu.
func (pm *Manager) getStatus() []*api.PortsStatus {
	res := make([]*api.PortsStatus, 0, len(pm.state))
	for port := range pm.state {
		res = append(res, pm.getPortStatus(port))
	}
	return res
}

func (pm *Manager) getPortStatus(port uint32) *api.PortsStatus {
	mp := pm.state[port]
	ps := &api.PortsStatus{
		GlobalPort: mp.GlobalPort,
		LocalPort:  mp.LocalhostPort,
		Served:     mp.Served,
	}
	if mp.Exposed {
		ps.Exposed = &api.PortsStatus_ExposedPortInfo{
			Public:    mp.Public,
			Url:       mp.URL,
			OnExposed: mp.OnExposed,
		}
	}
	return ps
}

func startLocalhostProxy(localPort uint32, globalPort uint32) (io.Closer, error) {
	host := fmt.Sprintf("localhost:%d", localPort)
	dsturl, err := url.Parse("http://" + host)
	if err != nil {
		return nil, xerrors.Errorf("cannot produce proxy destination URL: %w", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(dsturl)
	od := proxy.Director
	proxy.Director = func(req *http.Request) {
		req.Host = host
		od(req)
	}
	proxyAddr := fmt.Sprintf(":%d", globalPort)
	lis, err := net.Listen("tcp", proxyAddr)
	if err != nil {
		return nil, xerrors.Errorf("cannot listen on proxy port %d: %w", globalPort, err)
	}

	srv := &http.Server{
		Addr:    proxyAddr,
		Handler: proxy,
	}
	go func() {
		err := srv.Serve(lis)
		if err != nil {
			log.WithError(err).WithField("local-port", localPort).Error("localhost proxy failed")
		}
	}()

	return srv, nil
}
