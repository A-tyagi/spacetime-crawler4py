from threading import Thread

from utils.download import download
from utils import get_logger
from scraper import scraper
import time


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, threads_in_processing, thread_lock = None):
        self.worker_id = worker_id
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.thread_lock = thread_lock
        self.threads_in_processing = threads_in_processing
        super().__init__(daemon=True)

    def _lock(self):
        if self.thread_lock:
            self.thread_lock.acquire()

    def _unlock(self):
        if self.thread_lock:
            self.thread_lock.release()

    def run(self):
        try:
            while True:
                self._lock()
                try :
                    tbd_url = self.frontier.get_tbd_url()
                    if tbd_url:
                        self.threads_in_processing.add(self.worker_id)
                    elif tbd_url is None and len(self.threads_in_processing) == 0:
                        self.logger.info(f"Frontier is empty. Stopping the Worker: {self.worker_id}")
                        break
                finally:
                    self._unlock()

                if tbd_url is None or tbd_url == "":
                    time.sleep(0.05)
                    continue

                self.logger.info(f"Worker: {self.worker_id} Downloading: {tbd_url}")
                resp = download(tbd_url, self.config, self.logger)
                if resp.raw_response is None and resp.error.startswith("EMPTYCONTENT"):
                    self.logger.error(f"{resp.error}, status <{resp.status}>")

                self.logger.info(f"Worker: {self.worker_id} Downloaded : {tbd_url}, status <{resp.status}>")
                scraped_urls = scraper(tbd_url, resp)

                new_urls_added = 0
                self._lock()
                try:
                    for scraped_url in scraped_urls:
                        if (self.frontier.add_url(scraped_url)):
                            new_urls_added += 1
                    self.frontier.mark_url_complete(tbd_url)
                finally:
                    self._unlock()
                    self.threads_in_processing.remove(self.worker_id)
 
                
                self.logger.info(f"Worker: {self.worker_id}, Added: {new_urls_added}, Remaining: {self.frontier.count_tbd_urls()}")
        except BaseException:
            self.logger.exception(f"Unexpected exception in Worker: {self.worker_id}")
        finally:
            if self.worker_id in self.threads_in_processing:
                self.threads_in_processing.remove(self.worker_id)
            self.logger.info(f"Worker: {self.worker_id} Stopped")

