package main

import (
	"Pipe-Lib/pipeline"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	dir := flag.String("dir", ".", "Путь к директории")
	fmt.Println("DIR:", *dir, "FILES:", fileCount(*dir))

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

	// Ждём сигнала завершения (Ctrl+C) или завершения pipeline
	<-ctx.Done()
	fmt.Println("Остановка по сигналу")
	p.Stop()

}

func fileCount(dir string) int {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0
	}
	return len(files)
}
