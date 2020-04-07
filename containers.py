# Imports from external libraries
from collections import deque, OrderedDict
import time
import urllib.parse as urlparse
# Imports from internal libraries


class DomainDeque:
    timeout = 5.0

    def __init__(self, domain):
        self.domain = domain
        self.deque = deque()
        self.last_access = None
        self.robots = None
        self.domain_index = None
        self.robot_parser = None

    def is_available(self):
        if not self.deque:
            return False
        if self.last_access is None:
            return True
        elif time.time() - self.last_access > DomainDeque.timeout:
            return True
        else:
            return False


class Page:
    def __init__(self, url, domain, domain_id, parent_page_id):
        self.url = url
        self.domain = domain
        self.domain_id = domain_id
        self.parent_page_id = parent_page_id
        self.previous_urls = []
        self.page_id = None
        self.head_response = None
        self.content_type = None
        self.binary = False

    def reasign_url(self,new_url):
        if new_url == "data:,":
            self.binary = True
            return
        else:
            new_domain = urlparse.urlparse(new_url).netloc
            if self.domain != new_domain:
                print(f"WARNING: Domain not same after url reasignment: {self.url} --> {new_url}")
            self.previous_urls.append(self.url)
            self.url = new_url