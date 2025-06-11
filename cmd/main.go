package main

import (
	"Pipe-Lib/pipeline"
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	dir := flag.String("dir", ".", "Путь к директории")
	workers := flag.Int("workers", 10, "Степень параллелизма")
	flag.Parse()

	source := pipeline.NewFilePathSource(*dir)
	md5 := pipeline.NewMD5Worker(*workers)
	printer := pipeline.NewPrinter()
	p := pipeline.New(source, md5, printer)

	// Соединяем каналы
	pipeline.Connect(source, md5)
	pipeline.Connect(md5, printer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p.Start(ctx)
	p.Stop()
}
