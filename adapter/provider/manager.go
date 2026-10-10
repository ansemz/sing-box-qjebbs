package provider

import (
	"context"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
)

var _ adapter.ProviderManager = (*Manager)(nil)

type Manager struct {
	registry      adapter.ProviderRegistry
	access        sync.Mutex
	started       bool
	stage         adapter.StartStage
	scope         *adapter.Scope
	providers     []adapter.Provider
	providerByTag map[string]adapter.Provider
}

func NewManager(registry adapter.ProviderRegistry) *Manager {
	return &Manager{
		registry:      registry,
		providerByTag: make(map[string]adapter.Provider),
	}
}

func providerName(providerItem adapter.Provider) string {
	return "provider/" + providerItem.Type() + "[" + providerItem.Tag() + "]"
}

func (m *Manager) Start(stage adapter.StartStage, scope *adapter.Scope) error {
	m.access.Lock()
	if m.started && m.stage >= stage {
		panic("already started")
	}
	m.started = true
	m.stage = stage
	m.scope = scope
	providers := m.providers
	m.access.Unlock()

	started := make(map[string]bool)
	for _, providerToStart := range providers {
		providerTag := providerToStart.Tag()
		if started[providerTag] {
			continue
		}
		started[providerTag] = true
		lifecycle, isLifecycle := providerToStart.(adapter.Lifecycle)
		if isLifecycle {
			err := m.scope.Start(providerName(providerToStart), lifecycle, stage)
			if err != nil {
				return E.Cause(err, "start provider", "[", providerTag, "]")
			}
		}
	}
	return nil
}

func (m *Manager) Providers() []adapter.Provider {
	m.access.Lock()
	defer m.access.Unlock()
	return m.providers
}

func (m *Manager) Provider(tag string) (adapter.Provider, bool) {
	m.access.Lock()
	provider, found := m.providerByTag[tag]
	m.access.Unlock()
	return provider, found
}

func (m *Manager) Remove(tag string) error {
	m.access.Lock()
	provider, found := m.take(tag)
	started := m.started
	m.access.Unlock()
	if !found {
		return os.ErrInvalid
	}
	if !started {
		return nil
	}
	// The provider lives in the manager's scope: removing it cancels its context and runs
	// its cleanups in reverse order, so there is no separate Close to call here.
	//
	// This is done outside of m.access on purpose: the cleanups may block for seconds (closing
	// transports, waiting for goroutines) and may call back into this manager, and m.access is
	// not reentrant.
	lifecycle, isLifecycle := provider.(adapter.Lifecycle)
	if !isLifecycle {
		return nil
	}
	if err := m.scope.Remove(lifecycle); err != nil {
		return E.Cause(err, "close provider [", tag, "]")
	}
	return nil
}

// take detaches a provider from the registry, the caller must hold m.access.
func (m *Manager) take(tag string) (adapter.Provider, bool) {
	provider, found := m.providerByTag[tag]
	if !found {
		return nil, false
	}
	delete(m.providerByTag, tag)
	index := common.Index(m.providers, func(it adapter.Provider) bool {
		return it == provider
	})
	if index == -1 {
		panic("invalid provider index")
	}
	m.providers = append(m.providers[:index], m.providers[index+1:]...)
	return provider, true
}

// register records a provider, the caller must hold m.access.
func (m *Manager) register(tag string, provider adapter.Provider) {
	m.providers = append(m.providers, provider)
	m.providerByTag[tag] = provider
}

func (m *Manager) Create(ctx context.Context, router adapter.Router, logFactory log.Factory, tag string, providerType string, options any) error {
	if tag == "" {
		return os.ErrInvalid
	}
	provider, err := m.registry.CreateProvider(ctx, router, logFactory, tag, providerType, options)
	if err != nil {
		return err
	}
	m.access.Lock()
	if !m.started {
		// Created before the manager starts: Start will start it stage by stage.
		m.take(tag)
		m.register(tag, provider)
		m.access.Unlock()
		return nil
	}
	m.access.Unlock()
	// Providers created at runtime are children of the manager's own scope: Remove(tag)
	// detaches them individually, while the scope tree still closes any leftover on shutdown.
	// This runs outside of m.access on purpose: Start may call back into the manager or block
	// on other locks of the box, and m.access is not reentrant.
	lifecycle, isLifecycle := provider.(adapter.Lifecycle)
	if isLifecycle {
		for _, stage := range adapter.ListStartStages {
			err = m.scope.Start(providerName(provider), lifecycle, stage)
			if err != nil {
				_ = m.scope.Remove(lifecycle)
				return E.Cause(err, stage, " provider/", "[", provider.Tag(), "]")
			}
		}
	}
	// Replace the provider with the same tag, if any. The old one is detached before the new
	// one is registered, and its teardown is delayed until m.access is released, see below.
	m.access.Lock()
	existsProvider, replaced := m.take(tag)
	m.register(tag, provider)
	m.access.Unlock()
	if !replaced {
		return nil
	}
	// The replaced provider is torn down outside of m.access, for the same reason the startup
	// above runs unlocked: the cleanups may block for seconds and may call back into the manager.
	existsLifecycle, isLifecycle := existsProvider.(adapter.Lifecycle)
	if !isLifecycle {
		return nil
	}
	err = m.scope.Remove(existsLifecycle)
	if err != nil {
		return E.Cause(err, "close provider [", tag, "]")
	}
	return nil
}
