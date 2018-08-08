package testtools

import (
	"bytes"
	"github.com/wal-g/wal-g"
)

// FileTarBallMaker creates a new FileTarBall
// with the directory that files should be
// extracted to.
type FileTarBallMaker struct {
	number           int
	size             int64
	ArchiveDirectory string
	Out              string
}

// Make creates a new FileTarBall.
func (tarBallMaker *FileTarBallMaker) Make(inheritState bool) walg.TarBall {
	tarBallMaker.number++
	return &FileTarBall{
		number:           tarBallMaker.number,
		size:             tarBallMaker.size,
		archiveDirectory: tarBallMaker.ArchiveDirectory,
		out:              tarBallMaker.Out,
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
func (tarBallMaker *NOPTarBallMaker) Make(inheritState bool) walg.TarBall {
	tarBallMaker.number++
	return &NOPTarBall{
		number:           tarBallMaker.number,
		size:             tarBallMaker.size,
		archiveDirectory: tarBallMaker.Trim,
	}
}

type BufferTarBallMaker struct {
	number           int
	size             int64
	BufferToWrite    *bytes.Buffer
	ArchiveDirectory string
}

func (tarBallMaker *BufferTarBallMaker) Make(dedicatedUploader bool) walg.TarBall {
	tarBallMaker.number++
	return &BufferTarBall{
		number:           tarBallMaker.number,
		size:             tarBallMaker.size,
		archiveDirectory: tarBallMaker.ArchiveDirectory,
		underlying:       tarBallMaker.BufferToWrite,
	}
}
