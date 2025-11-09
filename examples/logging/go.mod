module github.com/andreademasi/goku/examples/logging

go 1.25.1

replace github.com/andreademasi/goku => ../..

require (
	github.com/andreademasi/goku v0.0.0-00010101000000-000000000000
	github.com/rs/zerolog v1.34.0
	github.com/sirupsen/logrus v1.9.3
	go.uber.org/zap v1.27.0
)

require (
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
)
