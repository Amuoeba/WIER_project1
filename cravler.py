# Imports from external libraries
import threading
import sys
from collections import deque, OrderedDict
import bs4
import os
import requests
import urllib.parse as urlparse
import selenium
import time
import datetime as dt
import re
from enum import Enum
# from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from typing import Dict

# Imports from internal libraries
import configs
import utills
from database import CrawlDB
from containers import Page,DomainDeque
from utills import Reg, get_sitemap,\
    get_robots_content,parse_robots


class FetchMode(Enum):
    REQUESTS = 0
    SELENIUM = 1


class CravlManager:
    def __init__(self, seed_sites, num_workers=4):
        self.num_workers = num_workers
        self.fifo_deque = deque()
        self.deque_per_domain: OrderedDict[str, DomainDeque] = OrderedDict()
        self.already_visited = set()
        self.already_on_queue = set()
        self.visited_domains = dict()
        self.image_allready_added = set()
        self.lock = threading.Lock()
        self.new_data_evt = threading.Event()
        self.limit = 100
        self.timeout = 5.0
        self.counter = 0
        self.site_page = dict()
        self.current_domain_id = 0

        self.debugCounter = 0
        self.debug_list = list(range(100))
        self.debug_results = []

        # Putting seed sites in queues
        self.init_seeds(seed_sites)

    def init_seeds(self, seed_sites):
        for site in seed_sites:
            if not re.search(Reg.http_begin, site):
                site = f"http://{site}"
            try:
                res = requests.head(site, allow_redirects=True, timeout=10)
                p = urlparse.urlparse(res.url)
                domain = p.netloc
                if domain not in self.deque_per_domain:
                    Cravler.process_new_domain(res.url, self)
            except:
                print(f"Mistake in init_seeds: {site}")
                print(sys.exc_info())

    # def check_if_all_completed(self):
    #     for domain in self.deque_per_domain:
    #         if self.deque_per_domain[domain].deque:
    #             return False
    #     else:
    #         return True

    def find_next_page(self):
        for domain in self.deque_per_domain:
            if self.deque_per_domain[domain].is_available():
                page = self.deque_per_domain[domain].deque.popleft()
                if page.url not in self.already_visited:
                    self.counter += 1
                    self.already_visited.add(page.url)
                    self.deque_per_domain[domain].last_access = time.time()
                    # print(f"Returning {page}")
                    return page
        return False

    @staticmethod
    def run_cravler(cravler, manager):
        # cravler.thread_debug(manager)

        # while not manager.check_if_all_completed() and manager.counter <= manager.limit:
        while True:
        # while manager.counter <= manager.limit:
            # print(f"Event {manager.new_data_evt}")
            # if manager.check_if_all_completed():
            #     manager.new_data_evt.wait(timeout=5)
            #     manager.new_data_evt.clear()
            # time.sleep(0.2)
            with manager.lock:
                page = manager.find_next_page()
            if page:
                cravler.crawl(page, manager)

    def spin_threads(self):
        cravlers = [Cravler() for _ in range(self.num_workers)]
        threads = []
        for i in range(self.num_workers):
            th = threading.Thread(target=CravlManager.run_cravler, args=(cravlers[i], self), name=f"Caravler-{i}")
            threads.append(th)
        for t in threads:
            t.start()
        for t in threads:
            t.join()


class Cravler:
    def __init__(self):
        self.current_site = None
        self.head_response = None
        self.dont_visit = set()
        self.current_page: Page = None

    def thread_debug(self, manager: CravlManager):
        while manager.debugCounter < 20:
            with manager.lock:
                local = manager.debugCounter
                manager.debugCounter += 1

            time.sleep(2)

            with manager.lock:
                print(f"Working on: {local}  Working from thread: {threading.currentThread()}")
                a = local
                c = manager.debug_list[a]
                manager.debug_results.append(c ** 2)

    def crawl(self, page: Page, manager: CravlManager):
        # asignment = self.assign_site(page, manager)
        # if asignment:
        #     self.read_site(manager, how=FetchMode.SELENIUM)
        self.current_page = page
        self.read_site(manager, how=FetchMode.SELENIUM)

    @staticmethod
    def process_new_domain(url, manager, parent_id=None):
        parsed_site = urlparse.urlparse(url)
        root_url = f"{parsed_site.scheme}://{parsed_site.netloc}/"
        robots,robo_parser = get_robots_content(root_url)
        sitemap = get_sitemap(root_url)

        if robots != "missing":
            disalows,sitemaps = parse_robots(robots)
            if len(sitemaps)>0 and sitemap == "missing":
                sitemap = sitemaps[0]

            #TODO Naredi nekaj preprostih vizualizacij

        domain = parsed_site.netloc
        with manager.lock:
            domain_id = CrawlDB.insert_site(domain, robots, sitemap)
            pg_sitemap = None
            if domain_id > 0:
                if parent_id:
                    pg = Page(url, domain, domain_id, parent_id)
                    if sitemap != "missing":
                        pg_sitemap = Page(sitemap,domain,domain_id,parent_id)
                else:
                    pg = Page(url, domain, domain_id, -1)
                    if sitemap != "missing":
                        pg_sitemap = Page(sitemap,domain,domain_id,-1)
                manager.visited_domains[domain] = domain_id
                dq = DomainDeque(domain)
                dq.domain_index = domain_id
                dq.robots = robots
                if pg_sitemap:
                    dq.deque.append(pg_sitemap)
                dq.deque.append(pg)
                dq.robot_parser = robo_parser
                manager.deque_per_domain[domain] = dq

    def assign_site(self, page: Page, manager: CravlManager):
        self.current_site = None
        if re.search(Reg.http_begin, page.url):
            self.current_site = f"{page.url}"
        else:
            self.current_site = f"https://{page.url}"
        try:
            res = requests.head(self.current_site, allow_redirects=True, timeout=10)
            self.head_response = res.status_code
        except:
            print(f"Something went wrong at {self.current_site}")
            print(sys.exc_info())
            manager.already_visited.add(self.current_site)
            return False

        parsed_site = urlparse.urlparse(res.url)
        schema = parsed_site.scheme
        domain = parsed_site.netloc
        path = parsed_site.path

        self.current_site = f"{schema}://{domain}{path}"

        new_domain = False

        with manager.lock:
            if page.domain not in manager.visited_domains:
                new_domain = True

        if new_domain:
            self.process_new_domain(self.current_site, manager)

        self.current_page = page
        return True

    def process_page(self,access_time):
        domain_id = self.current_page.domain_id
        # Add page urls to already visited set
        if len(self.current_page.previous_urls) > 0:
            for site in self.current_page.previous_urls:
                manager.already_visited.add(site)
        manager.already_visited.add(self.current_page.url)

        page_id = CrawlDB.insert_page(domain_id, self.current_page.url,
                                      self.current_page.head_response,
                                      self.current_page.content_type,
                                      str(access_time))
        self.current_page.page_id = page_id
        if self.current_page.parent_page_id != -1:
            CrawlDB.insert_link(self.current_page.parent_page_id, page_id)
            data = utills.MimeTypes.infer_page_type(self.current_page)
            if data:
                CrawlDB.insert_page_data(page_id,data)



    def read_site(self, manager: CravlManager, how=FetchMode.REQUESTS):
        if how == FetchMode.REQUESTS:
            print(f"Current site: {self.current_site}")
            res = requests.get(self.current_site)
            print(f"URL: {res.url}")
            for h in res.history:
                print(f"History from site read {h.url}")
            print(res.text)
        elif how == FetchMode.SELENIUM:
            opts = webdriver.ChromeOptions()
            # opts = Options()
            opts.add_argument("--headless")
            opts.add_argument(f"user-agent={configs.AGENT_NAME}")
            prefs = {
                "download_restrictions": 3,
                "download.default_directory": "/dev/null",
            }
            opts.add_experimental_option("prefs", prefs)
            driver = selenium.webdriver.Chrome(configs.CHROME_DRIVER, options=opts)

            access_time = dt.datetime.fromtimestamp(time.time())
            # driver.get(self.current_site)
            manager.deque_per_domain[self.current_page.domain].last_access = time.time()
            driver.get(self.current_page.url)
            utills.selenium_wait(driver, verbose=True)


            # Update current_page url if Selenium redirected to some other side.
            # Make head request to determine page content type and response code
            if driver.current_url != self.current_page.url:
                self.current_page.reasign_url(driver.current_url)
                try:
                    res = requests.head(self.current_page.url)
                    self.current_page.head_response = res.status_code
                    self.current_page.content_type = res.headers["content-type"]
                except:
                    print(f"Error in requesting headers to: {self.current_page.url}")
                    self.current_page.head_response = -1
                    self.current_page.content_type = "unknown"
            else:
                try:
                    res = requests.head(self.current_page.url)
                    self.current_page.head_response = res.status_code
                    self.current_page.content_type = res.headers["content-type"]
                except:
                    print(f"Error in requesting headers to: {self.current_page.url}")
                    self.current_page.head_response = -1
                    self.current_page.content_type = "unknown"

            curr_domain = self.current_page.domain
            new_domain = False

            # Add page to the database
            with manager.lock:
                if curr_domain in manager.deque_per_domain:
                    self.process_page(access_time)
                else:
                    new_domain = True

            if new_domain:
                Cravler.process_new_domain(curr_domain,manager)
                with manager.lock:
                    self.process_page(access_time)

            # Parsing page source code if page is not binary
            if not self.current_page.binary:
                if utills.MimeTypes.is_xml(self.current_page.content_type) and self.current_page.page_id > 0:
                    self.extract_XML_links(driver.page_source, manager, self.current_page.page_id)

                if not utills.MimeTypes.is_xml(self.current_page.content_type) and self.current_page.page_id > 0:
                    self.extract_HTML_links(driver.page_source, manager, self.current_page.page_id)
                    images = self.extract_image_sources(driver.page_source)
                    with manager.lock:
                        for im in images:
                            if (self.current_page.page_id,im) not in manager.image_allready_added:
                                CrawlDB.insert_image(self.current_page.page_id,im,access_time)
                                manager.image_allready_added.add((self.current_page.page_id,im))


            print(f"Thread:{threading.currentThread().getName()}: {self.current_page.previous_urls} --> {self.current_page.url}")

    @staticmethod
    def f_href_javascript(href):
        if re.search(Reg.href_script, href):
            return True
        else:
            return False

    def link_filtering(self, href):
        filters = [
            self.f_href_javascript
        ]
        for f in filters:
            if f(href):
                return False
        return True

    def extract_HTML_links(self, html, manager: CravlManager, parent_id):
        soup = bs4.BeautifulSoup(html, features="html.parser")
        links = soup.findAll("a", href=True)
        for link in links:
            href = None
            if not (link["href"] == "" or link["href"] is None):
                if re.search(Reg.http_begin, link['href']):
                    href = link['href']
                else:
                    parsed_url = urlparse.urlparse(self.current_site)
                    if link["href"][0] == "/":
                        slash = ""
                    else:
                        slash = "/"
                    href = f"{parsed_url.scheme}://{parsed_url.netloc}{slash}{link['href']}"
            paresd_href = urlparse.urlparse(href)

            if re.search(Reg.is_gow_domain, str(paresd_href.netloc)):
                cleaned_href = f"{paresd_href.scheme}://{paresd_href.netloc}{paresd_href.path}"
                p = urlparse.urlparse(cleaned_href)
                domain = p.netloc

                if self.link_filtering(cleaned_href):
                    new_domain = False
                    with manager.lock:
                        if (cleaned_href not in manager.already_visited) and (
                                cleaned_href not in manager.already_on_queue):
                            if domain not in manager.deque_per_domain:
                                new_domain = True
                            else:
                                rp = manager.deque_per_domain[domain].robot_parser
                                if rp:
                                    if rp.can_fetch(configs.AGENT_NAME, cleaned_href):
                                        domain_id = manager.deque_per_domain[domain].domain_index
                                        pg = Page(cleaned_href, domain, domain_id, parent_id)
                                        manager.deque_per_domain[domain].deque.append(pg)
                                else:
                                    domain_id = manager.deque_per_domain[domain].domain_index
                                    pg = Page(cleaned_href, domain, domain_id, parent_id)
                                    manager.deque_per_domain[domain].deque.append(pg)
                            manager.already_on_queue.add(cleaned_href)
                    if new_domain:
                        Cravler.process_new_domain(cleaned_href, manager, parent_id)

    def extract_image_sources(self,html):
        soup = bs4.BeautifulSoup(html, features="html.parser")
        images = soup.findAll("img")
        full_sources = []
        for im in images:
            if "src" in im.attrs:
                im = im["src"]
                # schema,domain = urlparse.urlparse(self.current_page.url)[:2]
                # source = f"{schema}://{domain}{im}"
                full_sources.append(im)
        return full_sources


    def extract_XML_links(self, xml, manager: CravlManager, parent_id):
        bs_content = bs4.BeautifulSoup(xml, "lxml")
        a = [x.text for x in bs_content.find_all("loc")]
        cleaned_urls = []
        for x in a:
            parsed = urlparse.urlparse(x)
            url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            cleaned_urls.append(url)

        for link in cleaned_urls:
            print(f"XML link:{link}")
            p = urlparse.urlparse(link)
            domain = p.netloc
            new_domain = False
            with manager.lock:
                if (link not in manager.already_visited) and (link not in manager.already_on_queue):
                    if domain not in manager.deque_per_domain:
                        new_domain = True
                    else:
                        domain_id = manager.deque_per_domain[domain].domain_index
                        pg = Page(link, domain, domain_id, parent_id)
                        manager.deque_per_domain[domain].deque.append(pg)
                    manager.already_on_queue.add(pg.url)
            if new_domain:
                Cravler.process_new_domain(link, manager, parent_id)

if __name__ == "__main__":
    print("Testing cravler")

    start = time.time()
    manager = CravlManager(configs.SEED_SITES, 20)
    manager.spin_threads()
    end = time.time()
    print(manager.debug_results)
    print(f"Threaded took: {end - start}")
    for domain in manager.deque_per_domain:
        print(len(manager.deque_per_domain[domain].deque))

    print(list(manager.already_visited))
