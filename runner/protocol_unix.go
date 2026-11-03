//go:build !windows

package runner

import (
	"os"
	"os/exec"
)

func configureKernelProtocol(cmd *exec.Cmd, writer *os.File) error {
	cmd.ExtraFiles = []*os.File{writer}
	return nil
}
