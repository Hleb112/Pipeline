package pipeline

// Connect соединяет out первого нода с in второго.
func Connect(from Node, to Node) {
	if len(from.Out()) == 0 || len(to.In()) == 0 {
		return
	}
	go func() {
		out := from.Out()[0]
		in := to.In()[0]
		for v := range out {
			in <- v
		}
		close(in)
	}()
}
