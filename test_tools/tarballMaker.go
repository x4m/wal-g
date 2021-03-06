package tools

import (
	"github.com/x4m/wal-g/internal"
)

// FileTarBallMaker creates a new FileTarBall
// with the directory that files should be
// extracted to.
type FileTarBallMaker struct {
	number int
	size   int64
	Trim   string
	Out    string
}

// Make creates a new FileTarBall.
func (f *FileTarBallMaker) Make(inheritState bool) internal.TarBall {
	f.number++
	return &FileTarBall{
		number: f.number,
		size:   f.size,
		trim:   f.Trim,
		out:    f.Out,
	}
}

// NOPTarBallMaker creates a new NOPTarBall. Used
// for testing purposes.
type NOPTarBallMaker struct {
	number int
	size   int64
	Trim   string
}

// Make creates a new NOPTarBall.
func (n *NOPTarBallMaker) Make(inheritState bool) internal.TarBall {
	n.number++
	return &NOPTarBall{
		number: n.number,
		size:   n.size,
		trim:   n.Trim,
	}
}
