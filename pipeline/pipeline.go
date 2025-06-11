package pipeline

import (
	"context"
	"sync"
)

// Node представляет элементарную операцию обработки данных.
type Node interface {
	// Run запускает обработку ноды.
	Run(ctx context.Context) error
	// In возвращает входные каналы.
	In() []chan interface{}
	// Out возвращает выходные каналы.
	Out() []chan interface{}
	// Stop останавливает работу ноды.
	Stop()
}

// Pipeline реализует связку нод и управляет их запуском/остановкой.
type Pipeline struct {
	nodes  []Node
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// New создает новый pipeline из набора нод.
func New(nodes ...Node) *Pipeline {
	return &Pipeline{nodes: nodes}
}

// Start запускает все ноды параллельно.
func (p *Pipeline) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	for _, n := range p.nodes {
		p.wg.Add(1)
		go func(n Node) {
			defer p.wg.Done()
			_ = n.Run(p.ctx)
		}(n)
	}
}

// Stop останавливает pipeline и дожидается завершения всех нод.
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	for _, n := range p.nodes {
		n.Stop()
	}
	p.wg.Wait()
}
