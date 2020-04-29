import os
import shelve

from urllib.parse import urlparse
from threading import Thread, RLock
from queue import Queue, Empty
from time import time 

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.download_buckets = {}
        self._create_tbd_buckets()

        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self._get_tbd_bucket(url).append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def _create_tbd_buckets(self):
        for bucket_name in self.config.domains:
            self.logger.info(f"adding tbd_bucket {bucket_name}")
            self.download_buckets[bucket_name] = ([], 0)

    def _get_tbd_bucket(self, url):
        for bucket_name in self.download_buckets.keys():
            if bucket_name in url:
                # self.logger.info(f"URL {url} goes to bucket {bucket_name}")
                return self.download_buckets[bucket_name][0]
        raise ValueError

    def get_tbd_url(self):
        time_now = int(time() * 1000)
        grace_time = 10
        time_delay = (self.config.time_delay * 1000) + grace_time
        all_buckets_empty = True
        for bucket_key in self.download_buckets.keys():
            tbd_bucket, bucket_access_time = self.download_buckets[bucket_key]
            if tbd_bucket:
                all_buckets_empty = False
                if (time_now - bucket_access_time) > time_delay:
                    self.download_buckets[bucket_key] = (tbd_bucket, time_now)
                    return tbd_bucket.pop()
        if all_buckets_empty:
            return None
        else:
            return ""

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self._get_tbd_bucket(url).append(url)
            return True
        return False
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()

    def count_tbd_urls(self):
        return sum(len(tbd_bucket) for tbd_bucket, _ in self.download_buckets.values())

    def close(self):
        self.save.sync()
        self.save.close()
        self.logger.info('######################## STOPPING FRONTIER ##########################')