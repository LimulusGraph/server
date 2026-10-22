//go:build unix

package runner

import (
	"os/exec"
	"syscall"
)

func configureKernelProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killKernelProcess(cmd *exec.Cmd) {
	// Include training workers and subprocesses inheriting the kernel's pipes.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
