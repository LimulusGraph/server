package runner

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

func configureKernelProtocol(cmd *exec.Cmd, writer *os.File) error {
	handle := syscall.Handle(writer.Fd())
	if err := syscall.SetHandleInformation(handle, syscall.HANDLE_FLAG_INHERIT, syscall.HANDLE_FLAG_INHERIT); err != nil {
		return err
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.AdditionalInheritedHandles = append(cmd.SysProcAttr.AdditionalInheritedHandles, handle)
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	cmd.Env = append(cmd.Env, "LIMULUS_PROTOCOL_HANDLE="+strconv.FormatUint(uint64(handle), 10))
	return nil
}
