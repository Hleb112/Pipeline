package pipeline

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// FilePathSource генерирует пути файлов из директории.
type FilePathSource struct {
	Dir  string
	out  chan interface{}
	stop chan struct{}
}

// NewFilePathSource создает новый источник файлов.
func NewFilePathSource(dir string) *FilePathSource {
	return &FilePathSource{
		Dir:  dir,
		out:  make(chan interface{}, 100),
		stop: make(chan struct{}),
	}
}

func (n *FilePathSource) Run(ctx context.Context) error {
	fmt.Println("FilePathSource started")
	fmt.Println("FilePathSource: Dir =", n.Dir)
	files, err := ioutil.ReadDir(n.Dir)
	if err != nil {
		fmt.Println("ReadDir error:", err)
		close(n.out)
		return err
	}
	for _, f := range files {
		fullPath := filepath.Join(n.Dir, f.Name())
		select {
		case n.out <- fullPath:
			fmt.Println("FilePathSource отправил:", fullPath)
		case <-ctx.Done():
			fmt.Println("FilePathSource: ctx.Done")
			close(n.out)
			return nil
		}
	}
	fmt.Println("FilePathSource: Закрываю out")
	close(n.out)
	return nil
}

func (n *FilePathSource) In() []chan interface{} {
	return []chan interface{}{}
}

func (n *FilePathSource) Out() []chan interface{} {
	return []chan interface{}{n.out}
}

func (n *FilePathSource) Stop() {
	close(n.stop)
}

// MD5Worker считает MD5 файлов.
type MD5Worker struct {
	in      chan interface{}
	out     chan interface{}
	wg      sync.WaitGroup
	workers int
	stop    chan struct{}
}

// NewMD5Worker создает MD5-ноду с параллелизмом.
func NewMD5Worker(workers int) *MD5Worker {
	return &MD5Worker{
		in:      make(chan interface{}, 100),
		out:     make(chan interface{}, 100),
		workers: workers,
		stop:    make(chan struct{}),
	}
}

func (n *MD5Worker) Run(ctx context.Context) error {
	n.wg.Add(n.workers)
	for i := 0; i < n.workers; i++ {
		go n.worker(ctx)
	}
	n.wg.Wait()
	close(n.out)
	return nil
}

func (n *MD5Worker) worker(ctx context.Context) {
	defer n.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stop:
			return
		case v, ok := <-n.in:
			if !ok {
				return
			}
			path, ok := v.(string)
			if !ok {
				continue
			}
			f, err := os.Open(path)
			if err != nil {
				continue
			}
			h := md5.New()
			_, _ = io.Copy(h, f)
			_ = f.Close()
			sum := fmt.Sprintf("%x", h.Sum(nil))
			n.out <- fmt.Sprintf("%s %s", sum, path)
		}
	}
}

func (n *MD5Worker) In() []chan interface{} {
	return []chan interface{}{n.in}
}

func (n *MD5Worker) Out() []chan interface{} {
	return []chan interface{}{n.out}
}

func (n *MD5Worker) Stop() {
	close(n.stop)
}

// Printer выводит результаты
type Printer struct {
	in   chan interface{}
	stop chan struct{}
}

func NewPrinter() *Printer {
	return &Printer{
		in:   make(chan interface{}, 100),
		stop: make(chan struct{}),
	}
}
func (n *Printer) Run(ctx context.Context) error {
	fmt.Println("Printer started")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Printer: ctx.Done")
			return nil
		case <-n.stop:
			fmt.Println("Printer: stopped")
			return nil
		case v, ok := <-n.in:
			if !ok {
				fmt.Println("Printer: in закрыт")
				return nil
			}
			fmt.Println("Printer:", v)
		}
	}
}

func (n *Printer) In() []chan interface{} {
	return []chan interface{}{n.in}
}

func (n *Printer) Out() []chan interface{} {
	return []chan interface{}{}
}

func (n *Printer) Stop() {
	close(n.stop)
}
