import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor

class Scheduler:
    def __init__(self, max_workers: int|None = None):
        if max_workers is not None and max_workers < 1:
            raise ValueError("max_workers doit être supérieur ou égal à 1")
        self.max_workers = max_workers or os.cpu_count()
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.jobs = []
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def add_job(self, func, interval, *args, **kwargs):
        """
        Ajoute une tâche répétée toutes les `interval` secondes.
        """
        self.jobs.append((interval, func, args, kwargs))

    def _run(self):
        next_times = [time.time() + interval for interval, *_ in self.jobs]
        while not self._stop_event.is_set():
            now = time.time()
            for i, (interval, func, args, kwargs) in enumerate(self.jobs):
                if now >= next_times[i]:
                    # planifier l’exécution dans le pool
                    self.executor.submit(func, *args, **kwargs)
                    next_times[i] += interval
            time.sleep(0.01)  # petite pause pour éviter de brûler du CPU

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()
        self.executor.shutdown(wait=True)
