package outbound

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	N "github.com/sagernet/sing/common/network"
)

var _ adapter.OutboundManager = (*Manager)(nil)

type Manager struct {
	registry                adapter.OutboundRegistry
	endpoint                adapter.EndpointManager
	defaultTag              string
	access                  sync.RWMutex
	outbounds               []adapter.Outbound
	outboundByTag           map[string]adapter.Outbound
	defaultOutbound         adapter.Outbound
	defaultOutboundFallback func() (adapter.Outbound, error)

	scope   *adapter.Scope
	started bool
	// isInternalByTag keeps track of which outbounds are internal.
	// In other words, internal outbounds are those managed by the "outbounds" section of configuration,
	// they are created before starting the manager.
	isInternalByTag map[string]struct{}
	confByTag       map[string]*confItem
}

type confItem struct {
	typ     string
	options any
}

func NewManager(registry adapter.OutboundRegistry, endpoint adapter.EndpointManager, defaultTag string) *Manager {
	return &Manager{
		registry:      registry,
		endpoint:      endpoint,
		defaultTag:    defaultTag,
		outboundByTag: make(map[string]adapter.Outbound),

		isInternalByTag: make(map[string]struct{}),
		confByTag:       make(map[string]*confItem),
	}
}

func (m *Manager) Initialize(defaultOutboundFallback func() (adapter.Outbound, error)) {
	m.defaultOutboundFallback = defaultOutboundFallback
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.access.Lock()
	if stage == adapter.StartStateInitialize {
		m.scope = scope
		if m.defaultTag != "" && m.defaultOutbound == nil {
			defaultEndpoint, loaded := m.endpoint.Get(m.defaultTag)
			if !loaded {
				m.access.Unlock()
				return E.New("default outbound not found: ", m.defaultTag)
			}
			m.defaultOutbound = defaultEndpoint
		}
		if m.defaultOutbound == nil {
			directOutbound, err := m.defaultOutboundFallback()
			if err != nil {
				m.access.Unlock()
				return E.Cause(err, "create direct outbound for fallback")
			}
			m.outbounds = append(m.outbounds, directOutbound)
			m.outboundByTag[directOutbound.Tag()] = directOutbound
			// The fallback outbound is created before the manager starts, so it is internal
			// as well and is started stage by stage below.
			m.isInternalByTag[directOutbound.Tag()] = struct{}{}
			m.defaultOutbound = directOutbound
		}
	}
	if stage == adapter.StartStateStart {
		m.started = true
	}
	outbounds := m.outbounds
	m.access.Unlock()
	if stage == adapter.StartStateStart {
		return m.startOutbounds(scope, append(outbounds, common.Map(m.endpoint.Endpoints(), func(it adapter.Endpoint) adapter.Outbound { return it })...))
	}
	// Outbounds created at runtime are already started by Create, which replays every stage
	// in their own child scope. Starting them again here would double-start them and register
	// duplicate cleanups, so this loop only covers internal outbounds.
	for _, outbound := range outbounds {
		if !m.isInternal(outbound.Tag()) {
			continue
		}
		lifecycle, isLifecycle := outbound.(adapter.Lifecycle)
		if !isLifecycle {
			continue
		}
		name := "outbound/" + outbound.Type() + "[" + outbound.Tag() + "]"
		err := scope.Start(name, lifecycle, stage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) startOutbounds(scope *adapter.Scope, outbounds []adapter.Outbound) error {
	started := make(map[string]bool)
	for {
		canContinue := false
	startOne:
		for _, outboundToStart := range outbounds {
			outboundTag := outboundToStart.Tag()
			if started[outboundTag] {
				continue
			}
			dependencies := outboundToStart.Dependencies()
			for _, dependency := range dependencies {
				if !started[dependency] {
					continue startOne
				}
			}
			started[outboundTag] = true
			canContinue = true
			if endpoint, isEndpoint := outboundToStart.(adapter.Endpoint); isEndpoint {
				// Upstream's adapter.Endpoint is a structural interface, so every new-style
				// outbound satisfies it. Only the objects actually registered in the endpoint
				// manager should be started through it, the rest are plain outbounds below.
				if registered, loaded := m.endpoint.Get(outboundTag); loaded && registered == endpoint {
					err := m.endpoint.StartEndpoint(endpoint)
					if err != nil {
						return err
					}
					continue
				}
			}
			lifecycle, isLifecycle := outboundToStart.(adapter.Lifecycle)
			if !isLifecycle {
				continue
			}
			name := "outbound/" + outboundToStart.Type() + "[" + outboundTag + "]"
			err := scope.Start(name, lifecycle, adapter.StartStateStart)
			if err != nil {
				return err
			}
		}
		if len(started) == len(outbounds) {
			break
		}
		if canContinue {
			continue
		}
		currentOutbound := common.Find(outbounds, func(it adapter.Outbound) bool {
			return !started[it.Tag()]
		})
		var lintOutbound func(oTree []string, oCurrent adapter.Outbound) error
		lintOutbound = func(oTree []string, oCurrent adapter.Outbound) error {
			problemOutboundTag := common.Find(oCurrent.Dependencies(), func(it string) bool {
				return !started[it]
			})
			if common.Contains(oTree, problemOutboundTag) {
				return E.New("circular outbound dependency: ", strings.Join(oTree, " -> "), " -> ", problemOutboundTag)
			}
			problemOutbound := common.Find(outbounds, func(it adapter.Outbound) bool {
				return it.Tag() == problemOutboundTag
			})
			if problemOutbound == nil {
				return E.New("dependency[", problemOutboundTag, "] not found for outbound[", oCurrent.Tag(), "]")
			}
			return lintOutbound(append(oTree, problemOutboundTag), problemOutbound)
		}
		return lintOutbound([]string{currentOutbound.Tag()}, currentOutbound)
	}
	return nil
}

func (m *Manager) Outbounds() []adapter.Outbound {
	m.access.RLock()
	defer m.access.RUnlock()
	return m.outbounds
}

func (m *Manager) Outbound(tag string) (adapter.Outbound, bool) {
	m.access.RLock()
	outbound, found := m.outboundByTag[tag]
	m.access.RUnlock()
	if found {
		return outbound, true
	}
	return m.endpoint.Get(tag)
}

func (m *Manager) Default() adapter.Outbound {
	m.access.RLock()
	defer m.access.RUnlock()
	return m.defaultOutbound
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, outboundType string, options any) error {
	if tag == "" {
		return os.ErrInvalid
	}
	outbound, err := m.registry.CreateOutbound(ctx, router, logger, tag, outboundType, options)
	if err != nil {
		return err
	}
	m.access.Lock()
	if !m.started {
		m.isInternalByTag[tag] = struct{}{}
		err = m.register(tag, outbound, outboundType, options)
		m.access.Unlock()
		return err
	}
	if m.isInternal(tag) {
		m.access.Unlock()
		return E.New("cannot create outbound with a internal tag ", tag, " after manager has started")
	}
	err = m.remove(tag)
	m.access.Unlock()
	if err != nil && err != os.ErrInvalid {
		return err
	}
	// Outbounds created at runtime are children of the manager's own scope: Remove(tag)
	// detaches them individually, while the scope tree still closes any leftover on shutdown.
	// Note that m.access must not be held while starting components, they may call back into
	// the manager.
	if lifecycle, isLifecycle := outbound.(adapter.Lifecycle); isLifecycle {
		name := "outbound/" + outbound.Type() + "[" + tag + "]"
		for _, stage := range adapter.ListStartStages {
			err = m.scope.Start(name, lifecycle, stage)
			if err != nil {
				_ = m.scope.Remove(lifecycle)
				return err
			}
		}
	}
	m.access.Lock()
	defer m.access.Unlock()
	return m.register(tag, outbound, outboundType, options)
}

func (m *Manager) Remove(tag string) error {
	m.access.Lock()
	defer m.access.Unlock()
	return m.remove(tag)
}

func (m *Manager) remove(tag string) error {
	outbound, found := m.outboundByTag[tag]
	if !found {
		return os.ErrInvalid
	}
	if m.isInternal(tag) {
		return E.New("cannot remove internal outbound with tag ", tag)
	}
	delete(m.outboundByTag, tag)
	index := common.Index(m.outbounds, func(it adapter.Outbound) bool {
		return it == outbound
	})
	if index == -1 {
		panic("invalid inbound index")
	}
	m.outbounds = append(m.outbounds[:index], m.outbounds[index+1:]...)
	if !m.started {
		return nil
	}
	var err error
	// Runtime objects live in the manager's scope: removing one cancels its context and runs
	// its cleanups in reverse order.
	if lifecycle, isLifecycle := outbound.(adapter.Lifecycle); isLifecycle {
		err = m.scope.Remove(lifecycle)
	}
	return E.Append(err, common.Close(outbound), func(err error) error {
		return E.Cause(err, "close outbound [", tag, "]")
	})
}

// register records a created (and already started) outbound, the caller must hold m.access.
func (m *Manager) register(tag string, outbound adapter.Outbound, outboundType string, options any) error {
	_, loaded := m.outboundByTag[tag]
	if loaded {
		return E.New("duplicate outbound tag: ", tag)
	}
	m.outbounds = append(m.outbounds, outbound)
	m.outboundByTag[tag] = outbound
	if tag == m.defaultTag || (m.defaultTag == "" && m.defaultOutbound == nil) {
		m.defaultOutbound = outbound
	}
	m.confByTag[tag] = &confItem{
		typ:     outboundType,
		options: options,
	}
	return nil
}

func (m *Manager) isInternal(tag string) bool {
	_, internal := m.isInternalByTag[tag]
	return internal
}

// DupOverrideDetour duplicates the outbound with the specified tag and sets the override and detour for the duplicated outbound.
// The original outbound is not affected.
// The duplicated outbound starts in the given scope and lives until that scope is closed.
func (m *Manager) DupOverrideDetour(ctx context.Context, scope *adapter.Scope, router adapter.Router, tag string, logger log.ContextLogger, detour N.Dialer) (adapter.Outbound, error) {
	m.access.RLock()
	conf, found := m.confByTag[tag]
	m.access.RUnlock()
	if !found {
		return nil, os.ErrInvalid
	}
	// It's hacky here, works only if all outbound creations invoke dialer.New()
	ctx, used := dialer.ContextWithDetourOverride(ctx, detour)
	outbound, err := m.registry.CreateOutbound(ctx, router, logger, tag, conf.typ, conf.options)
	if err != nil {
		return nil, err
	}
	if !used() {
		return nil, E.New("[" + tag + "] detour not overridable")
	}
	// The duplicated outbound is not managed by the manager: it lives in the scope provided
	// by the caller, which is responsible for closing it.
	if lifecycle, isLifecycle := outbound.(adapter.Lifecycle); isLifecycle {
		name := "outbound/" + outbound.Type() + "[" + tag + "]"
		for _, stage := range adapter.ListStartStages {
			err = scope.Start(name, lifecycle, stage)
			if err != nil {
				_ = scope.Remove(lifecycle)
				return nil, err
			}
		}
	}
	return outbound, nil
}
