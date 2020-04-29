from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
import threading
import time

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory
        
        # Create thread_lock if multi threaded.
        self.thread_lock = None
        self.threads_in_processing = set()
        if (self.config.threads_count > 1) :
            self.thread_lock = threading.Lock()

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.threads_in_processing, self.thread_lock)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            time.sleep(3.5)
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
