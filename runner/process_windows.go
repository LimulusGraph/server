package runner

import (
	"context"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func configureKernelProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}

func killKernelProcess(cmd *exec.Cmd) {
	// Include workers that inherited stdout/stderr; otherwise shutdown waits for them.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	killer := exec.CommandContext(ctx, "taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F")
	killer.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	_ = killer.Run()
	_ = cmd.Process.Kill()
}
