# Imports from external libraries
import time
import hashlib
import requests
import sys
import re
import urllib.robotparser as rob_parser


# Imports from internal libraries
from containers import Page

class Reg:
    http_begin = re.compile("^https*:\/\/", re.IGNORECASE)
    is_gow_domain = re.compile("\.gov\.si$", re.IGNORECASE)
    href_script = re.compile("javascript", re.IGNORECASE)
    has_body = re.compile("<body.*>", re.IGNORECASE)

    @staticmethod
    def multiple_check(text, regs):
        checks = []
        for r in regs:
            if re.search(r, text):
                checks.append(True)
            else:
                checks.append(False)
        return checks


def get_robots_content(root_site):
    robots_txt = f"{root_site}robots.txt"
    try:
        response = requests.get(robots_txt, allow_redirects=False)
    except:
        print(f"Robots for: {root_site}")
        print(sys.exc_info())
        return ("missing",None)
    robot = response.text
    robo_checks = Reg.multiple_check(robot, [Reg.has_body])
    if response.status_code == 200 and not all(robo_checks):
        rp = rob_parser.RobotFileParser()
        rp.set_url("http://www.pis.gov.si/robots.txt")
        rp.read()
        return (robot,rp)
    else:
        return ("missing",None)


def get_sitemap(root_site):
    sitemap = f"{root_site}sitemap.xml"
    try:
        response = requests.get(sitemap, allow_redirects=False)
    except:
        print(f"Sitemap for :{root_site}")
        print(sys.exc_info())
        return "missing"
    if response.status_code == 200:
        return response.url
    else:
        return "missing"

def parse_robots(robots_file):
    """Extracts dissalow markers and potential sitemap from robots.txt"""
    if not (robots_file == "missing" or robots_file is None):
        remove_coments = re.compile("#.*\n")
        disalows = re.compile("disallow: *(.*?)(?:\n|sitemap)", re.IGNORECASE)
        sitemap = re.compile("sitemap: *(.*)\n", re.IGNORECASE)

        robots_file = re.sub(remove_coments, "", robots_file)
        disalows = [x.replace(" ", "") for x in re.findall(disalows, robots_file)]
        disalows = [x.replace("\r", "") for x in disalows]
        sitemaps = re.findall(sitemap, robots_file)
        return disalows,sitemaps
    else:
        return None,None

def selenium_wait(driver, verbose=False, max_wait=10):
    sh1 = hashlib.sha256(driver.page_source.encode('utf-8')).hexdigest()
    sh2 = hashlib.sha256(driver.page_source.encode('utf-8')).hexdigest()
    t = time.time()
    while sh1 != sh2 and time.time() - t <= max_wait:
        sh2 = sh1
        sh1 = hashlib.sha256(driver.page_source.encode('utf-8')).hexdigest()
        time.sleep(0.2)
    if verbose:
        print(f"Selenium waited at: {driver.current_url}\nfor: {time.time() - t}")


class MimeTypes:
    mime_dict = {
        "application/pdf": "PDF",
        "application/msword": "DOC",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "DOCX",
        "application/vnd.ms-powerpoint": "PPT",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": "PPTX"
    }

    @staticmethod
    def is_xml(response_type):
        xml_mimes = ["application/xml", "text/xml"]
        for mime in xml_mimes:
            if mime in response_type:
                return True
        return False

    @staticmethod
    def infer_page_type(page:Page):
        for t in MimeTypes.mime_dict:
            if t in page.content_type:
                return MimeTypes.mime_dict[t]
        return None


