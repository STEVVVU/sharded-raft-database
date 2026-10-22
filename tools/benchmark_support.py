"""Bounded benchmark evidence for real product drivers; no simulated work."""
import json
import os
from pathlib import Path
import platform
import signal
import shutil
import subprocess
import threading
import time


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)


def environment(root):
    info=dict(source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(), dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip()), python=platform.python_version(), platform=platform.platform(), cores=os.cpu_count(), clock_ticks=os.sysconf('SC_CLK_TCK') if os.name != 'nt' else None)
    if os.name != 'nt':
        info['cpu_model']=next((line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')),None)
        info['ram_bytes']=next((int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:')),None)
    return info


class Run:
    def __init__(self, path):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=False)
        assert shutil.disk_usage(self.path).free >= 10 * 1024**3, '10GiB free disk required'
        self.processes, self.logs, self.resources = [], [], []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.monitor, daemon=True)
        self.thread.start()

    def launch(self, name, command, env=None):
        log = (self.path / (name + '.log')).open('ab')
        self.logs.append(log)
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=os.name != 'nt', creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        self.processes.append((name, process))
        return process

    def stop(self, process):
        if process.poll() is not None:
            return
        if os.name == 'nt':
            subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'], check=True, capture_output=True)
        else:
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name == 'nt':
                raise
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)

    @staticmethod
    def proc(pid):
        if os.name == 'nt':
            return None
        try:
            stat = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
            status = Path(f'/proc/{pid}/status').read_text().splitlines()
            rss = next((int(line.split()[1])*1024 for line in status if line.startswith('VmRSS:')), 0)
            return dict(pid=pid, cpu_ticks=int(stat[11])+int(stat[12]), rss_bytes=rss)
        except (FileNotFoundError, ProcessLookupError):
            return None

    def monitor(self):
        while not self.done.is_set():
            self.resources.append(dict(at=time.perf_counter(), generator=self.proc(os.getpid()), processes=[dict(name=name, process=self.proc(p.pid)) for name, p in list(self.processes)]))
            self.done.wait(.1)

    def cleanup(self):
        errors = []
        for name, process in reversed(self.processes):
            try:
                self.stop(process)
            except Exception as error:
                errors.append(dict(name=name, error=repr(error)))
        self.done.set()
        self.thread.join(timeout=2)
        for log in self.logs:
            log.close()
        save(self.path / 'resources.json', self.resources)
        return dict(all_exited=all(p.poll() is not None for _, p in self.processes), sampler_stopped=not self.thread.is_alive(), errors=errors)

