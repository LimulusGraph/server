"""One live Python namespace for one route; fd 3 carries control events only."""
import json
import os
import sys
import threading
import traceback
import types

protocol = os.fdopen(3, "w", buffering=1)
lock = threading.Lock()

def emit(**event):
    with lock:
        protocol.write(json.dumps(event) + "\n")
        protocol.flush()

class Stream:
    def __init__(self, source):
        self.source = source
        self.buffer = ""
        self.encoding = "utf-8"
    def write(self, text):
        self.buffer += text
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            emit(kind="log", source=self.source, text=line + "\n")
        return len(text)
    def flush(self):
        if self.buffer:
            emit(kind="log", source=self.source, text=self.buffer)
            self.buffer = ""
    def fileno(self):
        return 1 if self.source == "stdout" else 2
    def writable(self):
        return True
    def isatty(self):
        return False

# A real __main__ module supports introspection, dataclasses, and ordinary pickle
# within the runtime. No runtime objects cross the control protocol.
module = types.ModuleType("__main__")
sys.modules["__main__"] = module
scope = module.__dict__
scope.update(inputs={}, outputs={})
sys.path.insert(0, os.getcwd())
sys.stdout = Stream("stdout")
sys.stderr = Stream("stderr")
commands = sys.stdin
cell_outputs = {}
for line in commands:
    request = json.loads(line)
    try:
        previous = cell_outputs.get(request.get("parent_id"), {})
        inputs = {}
        if request.get("from_port") and isinstance(previous, dict):
            inputs[request.get("to_port") or "in"] = previous.get(request["from_port"])
        scope["inputs"] = inputs
        scope["outputs"] = {}
        scope["__file__"] = request["path"]
        # Cells cannot consume the kernel command stream via input().
        sys.stdin = open(os.devnull)
        try:
            with open(request["path"], "rb") as source:
                code = compile(source.read(), request["path"], "exec")
            exec(code, scope)
        finally:
            sys.stdin.close()
            sys.stdin = commands
        sys.stdout.flush()
        sys.stderr.flush()
        cell_outputs[request["node_id"]] = scope.get("outputs", {})
        success = True
    except BaseException:
        sys.stdout.flush()
        sys.stderr.flush()
        emit(kind="log", source="stderr", text=traceback.format_exc())
        success = False
    # Drain native stdout/stderr before Go publishes the cell completion.
    marker = ("\n" + request["barrier"] + "\n").encode()
    os.write(1, marker)
    os.write(2, marker)
    emit(kind="cell_done", success=success)
