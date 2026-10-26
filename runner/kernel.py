"""One live Python namespace for one route; fd 3 carries control events only."""
import ast
import base64
import builtins
import hashlib
import io
import json
import reprlib
import weakref
import os
import sys
import threading
import traceback
import types

protocol = os.fdopen(3, "w", buffering=1)
lock = threading.Lock()
current_script = None
figure_hashes = weakref.WeakKeyDictionary()


def source_line():
    frame = sys._getframe(1)
    while frame:
        if frame.f_code.co_filename == current_script:
            return frame.f_lineno
        frame = frame.f_back
    return 0

def emit(**event):
    if event.get('kind') in ('log', 'display'):
        event.setdefault('line', source_line())
    with lock:
        protocol.write(json.dumps(event) + "\n")
        protocol.flush()

class Stream:
    def __init__(self, source):
        self.source = source
        self.buffer = ""
        self.encoding = "utf-8"
        self.line = 0
    def write(self, text):
        if not self.buffer:
            self.line = source_line()
        self.buffer += text
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            emit(kind="log", source=self.source, text=line + "\n", line=self.line)
            self.line = source_line()
        return len(text)
    def flush(self):
        if self.buffer:
            emit(kind="log", source=self.source, text=self.buffer, line=self.line)
            self.buffer = ""
    def fileno(self):
        return 1 if self.source == "stdout" else 2
    def writable(self):
        return True
    def isatty(self):
        return False

# Optional libraries are detected only after user imports; ordinary nodes do not
# pay the cost of importing pandas or Matplotlib.
os.environ['MPLBACKEND'] = 'Agg'


def clipped(value, limit=2000):
    text = str(value)
    return text if len(text) <= limit else text[:limit] + '…'


def display_figure(figure):
    buffer = io.BytesIO()
    width, height = figure.get_size_inches()
    dpi = min(120, 1800 / max(float(width), float(height), 1))
    figure.savefig(buffer, format='png', dpi=dpi)
    data = buffer.getvalue()
    digest = hashlib.sha256(data).digest()
    if figure_hashes.get(figure) == digest:
        return
    figure_hashes[figure] = digest
    if len(data) > 4 * 1024 * 1024:
        emit(kind='log', source='stderr', text='Plot exceeds the 4 MiB preview limit. Reduce its size or save it to a file.\n')
        return
    emit(kind='display', output=dict(kind='image', mime='image/png', data=base64.b64encode(data).decode('ascii'), caption='Matplotlib figure'))


def display(*values):
    sys.stdout.flush()
    sys.stderr.flush()
    for value in values:
        if value is None:
            continue
        pandas = sys.modules.get('pandas')
        figures = sys.modules.get('matplotlib.figure')
        if pandas is not None and isinstance(value, (pandas.DataFrame, pandas.Series)):
            frame = value.to_frame() if isinstance(value, pandas.Series) else value
            sample = frame.iloc[:100, :30]
            emit(kind='display', output=dict(
                kind='table', columns=[clipped(c) for c in sample.columns],
                index=[clipped(i) for i in sample.index],
                rows=[[clipped(cell) for cell in row] for row in sample.itertuples(index=False, name=None)],
                total_rows=len(frame.index), total_columns=len(frame.columns),
                truncated=len(frame.index) > 100 or len(frame.columns) > 30,
            ))
        elif figures is not None and isinstance(value, figures.Figure):
            display_figure(value)
        else:
            emit(kind='display', output=dict(kind='text', text=reprlib.repr(value)))


def flush_figures(*args, **kwargs):
    pyplot = sys.modules.get('matplotlib.pyplot')
    if pyplot is None or not hasattr(pyplot, 'get_fignums'):
        return
    sys.stdout.flush()
    sys.stderr.flush()
    for number in list(pyplot.get_fignums()):
        figure = pyplot.figure(number)
        try:
            display_figure(figure)
        finally:
            pyplot.close(figure)


# Patch show after pyplot finishes importing, including `from ... import show`.
# Agg remains the environment backend for subprocesses started by user code.
original_import = builtins.__import__
def import_with_inline_plots(*args, **kwargs):
    result = original_import(*args, **kwargs)
    pyplot = sys.modules.get('matplotlib.pyplot')
    if pyplot is not None and hasattr(pyplot, 'show') and not getattr(pyplot, '_limulus_inline', False):
        pyplot._limulus_inline = True
        pyplot.show = flush_figures
    return result
builtins.__import__ = import_with_inline_plots


def compile_node(source, filename):
    tree = ast.parse(source, filename, 'exec')
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        last = tree.body[-1]
        value = last.value
        is_string = isinstance(value, ast.Constant) and isinstance(value.value, str)
        is_documentation = is_string and (len(tree.body) == 1 or value.value.startswith('md\n'))
        if not is_documentation:
            call = ast.Call(func=ast.Name(id='_limulus_display_last', ctx=ast.Load()), args=[value], keywords=[])
            tree.body[-1] = ast.copy_location(ast.Expr(value=call), last)
            ast.fix_missing_locations(tree)
    return compile(tree, filename, 'exec')


# A real __main__ module supports introspection, dataclasses, and ordinary pickle
# within the runtime. No runtime objects cross the control protocol.
module = types.ModuleType("__main__")
sys.modules["__main__"] = module
scope = module.__dict__
scope.update(inputs={}, outputs={}, display=display)
sys.path.insert(0, os.getcwd())
sys.stdout = Stream("stdout")
sys.stderr = Stream("stderr")
commands = sys.stdin
cell_outputs = {}
for line in commands:
    request = json.loads(line)
    current_script = request['path']
    figure_hashes.clear()
    scope['_limulus_display_last'] = display
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
                code = compile_node(source.read(), request["path"])
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
    try:
        flush_figures()
    except Exception:
        emit(kind='log', source='stderr', text='Could not render plot: ' + traceback.format_exc())
    # Drain native stdout/stderr before Go publishes the cell completion.
    marker = ("\n" + request["barrier"] + "\n").encode()
    os.write(1, marker)
    os.write(2, marker)
    emit(kind="cell_done", success=success)
