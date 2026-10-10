package provider

import (
	"context"
	"io"
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

func (m *Manager) Initialize() {
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
		err := m.startProvider(providerToStart, stage)
		if err != nil {
			return E.Cause(err, "start provider", "[", providerTag, "]")
		}
	}
	return nil
}

// startProvider hands the provider over to the scope: the scope lazily creates its child
// scope and owns the stage timing and the automatic cleanup.
func (m *Manager) startProvider(providerToStart adapter.Provider, stage adapter.StartStage) error {
	lifecycle, isLifecycle := providerToStart.(adapter.Lifecycle)
	if !isLifecycle {
		return E.New("provider does not implement adapter.Lifecycle: ", providerName(providerToStart))
	}
	return m.scope.Start(providerName(providerToStart), lifecycle, stage)
}

func (m *Manager) Close() error {
	m.access.Lock()
	if !m.started {
		m.access.Unlock()
		return nil
	}
	m.started = false
	providers := m.providers
	m.providers = nil
	m.access.Unlock()
	var err error
	for _, provider := range providers {
		if closer, isCloser := provider.(io.Closer); isCloser {
			err = E.Append(err, closer.Close(), func(err error) error {
				return E.Cause(err, "close provider/", "[", provider.Tag(), "]")
			})
		}
	}
	return err
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
	provider, found := m.providerByTag[tag]
	if !found {
		m.access.Unlock()
		return os.ErrInvalid
	}
	delete(m.providerByTag, tag)
	index := common.Index(m.providers, func(it adapter.Provider) bool {
		return it == provider
	})
	if index == -1 {
		panic("invalid provider index")
	}
	m.providers = append(m.providers[:index], m.providers[index+1:]...)
	started := m.started
	m.access.Unlock()
	if started {
		return common.Close(provider)
	}
	return nil
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
	defer m.access.Unlock()
	if m.started {
		for _, stage := range adapter.ListStartStages {
			err = m.startProvider(provider, stage)
			if err != nil {
				return E.Cause(err, stage, " provider/", "[", provider.Tag(), "]")
			}
		}
	}
	if existsProvider, loaded := m.providerByTag[tag]; loaded {
		if m.started {
			err = common.Close(existsProvider)
			if err != nil {
				return E.Cause(err, "close provider", "[", existsProvider.Tag(), "]")
			}
		}
		existsIndex := common.Index(m.providers, func(it adapter.Provider) bool {
			return it == existsProvider
		})
		if existsIndex == -1 {
			panic("invalid provider index")
		}
		m.providers = append(m.providers[:existsIndex], m.providers[existsIndex+1:]...)
	}
	m.providers = append(m.providers, provider)
	m.providerByTag[tag] = provider
	return nil
}
