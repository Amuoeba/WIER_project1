# Imports from external libraries
import re
# Imports from internal libraries
import configs
import utils


# PROD_ID=[0-9]*"><b>(.*?)<\/b>(?:.*?)<s>(.*?)<\/s>(?:.*?)<span class="bigred"><b>(.*?)<\/b><\/span>(?:.*?)<td align="left" nowrap="nowrap"><span class="littleorange">(.*?) \((.*?)\)<\/span>(?:.*?)<span class="normal">(.*?)<br>(?:.*?)<\/td>

def reg_overstock(html):
    re_dict = {"title":re.compile('PROD_ID=[0-9]*"><b>(.*?)<\/b>'),
               "list price":re.compile('<s>(\$[0-9\.,]*)<\/s>'),
               "price":re.compile('<span class="bigred"><b>(\$[0-9,\.]*)<\/b><\/span>'),
               "saving": re.compile('<td align="left" nowrap="nowrap"><span class="littleorange">(\$[0-9,\.]*) \(.*?\)<\/span>'),
               "saving percent": re.compile('<td align="left" nowrap="nowrap"><span class="littleorange">.*?\(([%0-9\.]*)\)<\/span>'),
               "content": re.compile('<span class="normal">(.*?)<br>')
               }

    matches = {}
    data_items = {"results":[]}
    for r in re_dict:
        results = re.findall(re_dict[r],html)
        matches[r] = results

    no_match = len(matches["title"])
    current = 0
    for i in range(no_match):
        it = {}
        for m in matches:
            it[m] = matches[m][current]
        current += 1
        data_items["results"].append(it)

    return data_items

def reg_rtvslo(html):
    re_dict = {"title":re.compile('<h1>(.*?)<\/h1>'),
               "subtitle":re.compile('<div class="subtitle">(.*?)<\/div>'),
               "author":re.compile('<div class="author-name">(.*?)<\/div>'),
               "date":re.compile('<div class="author-timestamp">.*([0-9]+\.) *([A-Ža-z]*) *([0-9]+) *ob *([0-9]*\:[0-9]*)'),
               "lead":re.compile('<p class="lead">(.*?)<\/p>'),
               "content":re.compile('<p>(.*?)<\/p>|<figcaption itemprop="caption description">(?:<span class="icon-photo"><\/span>)?(.*?)<\/figcaption>')
               }

    matches = {}
    data_items = {"results":[]}
    for r in re_dict:
        results = re.findall(re_dict[r],html)
        if r == "content":
            res = []
            for i in results:
                sub_match = " ".join(list(i))
                res.append(sub_match)
            aux_result = ["\n".join(res)]
        else:
            if type(results[0]) is tuple:
                aux_result = [" ".join(list(results[0]))]
            else:
                aux_result = results
        matches[r] = aux_result


    no_match = len(matches["title"])
    current = 0
    for i in range(no_match):
        it = {}
        for m in matches:
            it[m] = matches[m][current]
        current += 1
        data_items["results"].append(it)

    return data_items

def reg_mobilede(html):
    re_dict = {"car_name":re.compile('<span class="h3 u-text-break-word">(.*?)<\/span>'),
               "first_registration":re.compile('<div class="rbt-regMilPow"> *?FR *?([0-9]+?\/[0-9]+?)|(new car)(?:[^s])',re.IGNORECASE),
               "price":re.compile('<span class="h3 u-block">(€[0-9,]+)'),
               "kilometers":re.compile('([0-9\.]*)&nbsp;(km)'),
               "kilowats":re.compile('([0-9\.]*)&nbsp;(kw)',re.IGNORECASE),
               "horsepower": re.compile('([0-9\.]*)&nbsp;(hp)', re.IGNORECASE),
               "description":re.compile('<div class="rbt-regMilPow">(?:.*?)<\/div><div>(.*?)<\/div><\/div>'),
               }

    matches = {}
    data_items = {"results":[]}
    for r in re_dict:
        results = re.findall(re_dict[r],html)
        # print(f"Len results: {len(results)}")
        # print(results)
        if r == "content":
            res = []
            for i in results:
                sub_match = " ".join(list(i))
                res.append(sub_match)
            aux_result = ["\n".join(res)]
        else:
            if type(results[0]) is tuple:
                aux_result = []
                for t in results:
                    aux_tup = []
                    for i,_ in enumerate(t):
                        aux_tup.append(t[i])
                    aux_tup = " ".join(aux_tup)
                    aux_result.append(aux_tup)
            else:
                aux_result = results
        matches[r] = aux_result


    no_match = len(matches["car_name"])
    current = 0
    for i in range(no_match):
        it = {}
        for m in matches:
            it[m] = matches[m][current]
        current += 1
        data_items["results"].append(it)

    return data_items


def run():
    print("Running regex extraction")
    for site in configs.overstock_sites:
        print(f"Extracting: {site}")
        content = open(site, 'r',encoding = "utf8", errors = 'ignore').read()
        content = content.replace("\n","")
        content = content.replace("\t", "")
        extracted_data = reg_overstock(content)
        utils.save_and_print_results(site, extracted_data,"regex")

    for site in configs.rtvslo_sites:
        print(f"Extracting: {site}")
        content = open(site, 'r',encoding = "utf8", errors = 'ignore').read()
        content = content.replace("\n","")
        content = content.replace("\t", "")
        extracted_data = reg_rtvslo(content)
        utils.save_and_print_results(site,extracted_data,"regex")

    for site in configs.mobilede_sites:
        print(f"Extracting: {site}")
        content = open(site, 'r',encoding = "utf8", errors = 'ignore').read()
        content = content.replace("\n","")
        content = content.replace("\t", "")
        extracted_data = reg_mobilede(content)
        utils.save_and_print_results(site,extracted_data,"regex")
