# Imports from external libraries
import lxml
from lxml import html,etree
# Imports from internal libraries
import configs
import utils
import re


def xpath_overstock(tree):
    xpath_dict = {
        "title": '/html/body/table[2]/tbody/tr[1]/td[5]/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody//tr/td[2]/a/b/text()',
        "list price": lambda
            x: f"/html/body/table[2]/tbody/tr[1]/td[5]/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody/tr[@bgcolor][td[2]/a/b][{x}]/td[2]/table/tbody/tr/td[1]/table/tbody/tr[1]/td[2]/s/text()",
        "price": lambda
            x: f"/html/body/table[2]/tbody/tr[1]/td[5]/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody/tr[@bgcolor][td[2]/a/b][{x}]/td[2]/table/tbody/tr/td[1]/table/tbody/tr[2]/td[2]/span/b/text()",
        "saving": lambda
            x: f"/html/body/table[2]/tbody/tr[1]/td[5]/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody/tr[@bgcolor][td[2]/a/b][{x}]/td[2]/table/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/span/text()",
        "saving percent": lambda
            x: f"/html/body/table[2]/tbody/tr[1]/td[5]/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody/tr[@bgcolor][td[2]/a/b][{x}]/td[2]/table/tbody/tr/td[1]/table/tbody/tr[3]/td[2]/span/text()",
        "content": lambda
            x: f"/html/body/table[2]/tbody/tr[1]/td[5]/table/tbody/tr[2]/td/table/tbody/tr/td/table/tbody/tr[@bgcolor][td[2]/a/b][{x}]/td[2]/table/tbody/tr/td[2]/span/text()"
    }
    titles = tree.xpath(xpath_dict["title"])

    data_items = {"results": []}
    matches = {"title": titles}

    gen = (x for x in xpath_dict if x != "title")
    for xpath in gen:
        aux_results = []
        for i, title in enumerate(titles):
            res = tree.xpath(xpath_dict[xpath](i + 1))
            if len(res) > 1:
                print(f"Lenght of res > 1:{res}")
            if xpath == "saving":
                saving_reg = re.compile("\$[0-9,\.]+")
                res = re.findall(saving_reg, res[0])
            if xpath == "saving percent":
                percent_reg = re.compile("\(([0-9]+%)\)")
                res = re.findall(percent_reg, res[0])

            aux_results.append(res[0])

        matches[xpath] = aux_results

    no_match = len(matches["title"])
    current = 0
    for i in range(no_match):
        it = {}
        for m in matches:
            it[m] = matches[m][current]
        current += 1
        data_items["results"].append(it)

    return data_items


def xpath_rtvslo(tree):
    xpath_dict = {
        "title": '//*[@id="main-container"]/div[3]/div/header/h1/text()',
        "subtitle": '//*[@id="main-container"]/div[3]/div/header/div[2]/text()',
        "author": '//*[@id="main-container"]/div[3]/div/div[1]/div[1]/div/text()',
        "date": '//*[@id="main-container"]/div[3]/div/div[1]/div[2]/text()[1]',
        "lead": '//*[@id="main-container"]/div[3]/div/header/p/text()',
        "content": '//*[@id="main-container"]/div[3]/div/div[2]/div/figure/figcaption/text()|//*[@id="main-container"]/div[3]/div/div[2]/article//p/text()'
    }
    titles = tree.xpath(xpath_dict["title"])
    data_items = {"results": []}
    matches = {"title": titles}

    gen = (x for x in xpath_dict if x != "title")
    for xpath in gen:
        res = tree.xpath(xpath_dict[xpath])
        if xpath == "date":
            res[0] = res[0].replace("ob", "")
        if xpath == "content":
            if len(res) > 1:
                res = ["\n".join(res)]

        matches[xpath] = res
    no_match = len(matches["title"])

    current = 0
    for i in range(no_match):
        it = {}
        for m in matches:
            it[m] = matches[m][current]
        current += 1
        data_items["results"].append(it)

    return data_items


def xpath_mobilede(tree):
    xpath_dict = {
        "title": '/html/body//span[@class="h3 u-text-break-word"]/text()',
        "regMilPow": lambda
            x: f'(//a[.//span[@class="h3 u-text-break-word"]])[{x}]//div[@class="rbt-regMilPow"]/text()',
        "price": lambda
            x: f'(//a[.//span[@class="h3 u-text-break-word"]])[{x}]//span[@class="h3 u-block"]/text()',
        "description": lambda
            x: f'(//a[.//span[@class="h3 u-text-break-word"]])[{x}]//div[@class="rbt-regMilPow"]/following-sibling::node()'
    }

    titles = tree.xpath(xpath_dict["title"])

    data_items = {"results": []}
    matches = {"title": titles}

    gen = (x for x in xpath_dict if x != "title")
    for xpath in gen:
        aux_results = []
        for i, title in enumerate(titles):
            res = tree.xpath(xpath_dict[xpath](i + 1))
            if len(res)>0:
                if not isinstance((res[0]),str):
                    aux_results.append("".join(res[0].itertext()))
                else:
                    aux_results.append(res[0])
            else:
                aux_results.append("")

        matches[xpath] = aux_results

    no_match = len(matches["title"])
    current = 0
    for i in range(no_match):
        it = {}
        for m in matches:
            it[m] = matches[m][current]
        current += 1
        data_items["results"].append(it)

    return data_items


def run():
    print("Running xpath extraction")
    # tree = html.fromstring(pageContent)
    for site in configs.overstock_sites:
        print(f"Extracting: {site}")
        content = open(site, 'r', encoding="utf8", errors='ignore').read()
        content = content.replace("\n", "")
        content = content.replace("\t", "")
        xpath_tree = html.fromstring(content)
        extracted_data = xpath_overstock(xpath_tree)
        utils.save_and_print_results(site, extracted_data, "xpath")

    for site in configs.rtvslo_sites:
        print(f"Extracting: {site}")
        content = open(site, 'r', encoding="utf8", errors='ignore').read()
        content = content.replace("\n", "")
        content = content.replace("\t", "")
        xpath_tree = html.fromstring(content)
        extracted_data = xpath_rtvslo(xpath_tree)
        utils.save_and_print_results(site, extracted_data, "xpath")

    for site in configs.mobilede_sites:
        print(f"Extracting: {site}")
        content = open(site, 'r', encoding="utf8", errors='ignore').read()
        content = content.replace("\n", "")
        content = content.replace("\t", "")
        xpath_tree = html.fromstring(content)
        extracted_data = xpath_mobilede(xpath_tree)
        utils.save_and_print_results(site, extracted_data, "xpath")

        # extracted_data = reg_overstock(content)
        # utils.save_and_print_results(site, extracted_data,"regex")
