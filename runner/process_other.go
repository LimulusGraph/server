//go:build !unix && !windows

package runner

import "os/exec"

func configureKernelProcess(cmd *exec.Cmd) {}
func killKernelProcess(cmd *exec.Cmd)      { _ = cmd.Process.Kill() }
