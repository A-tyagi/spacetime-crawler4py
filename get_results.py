import json
import re
from typing import List, Dict
from urllib.parse import urlparse
import time
import sys

def compute_word_frequencies(token_list: list) -> Dict[str, int]:
    freq_dict = {}
    for item in token_list: 
        if (item in freq_dict): 
            freq_dict[item] += 1
        else: 
            freq_dict[item] = 1
    return freq_dict

def compute_page_tokenlength(input_file_name):
    big_list = list()
    lineNo = 0
    lines_found = 0

    with open(input_file_name, 'r') as input_file:
        with open("./page_tokenlength.csv", 'w') as page_tokenlength:
            for line in input_file:
                lineNo += 1
                if not line.strip():
                    print("skipping blank")
                    continue
                lines_found += 1
                line_json=json.loads(line.encode("utf-8"))
                line_tokens = line_json['token_list']
                big_list.extend(line_tokens)
                page_tokenlength.write(f"{line_json['URL']}, {len(line_tokens)}\n")
                print("linesfound ", lines_found)

    print(f"Total tokens from all pages, {len(big_list)}")
    master_dictionary = compute_word_frequencies(big_list)
    with open("./token_frequency.csv", 'w') as token_frequency:
        for key in master_dictionary.keys():
            token_frequency.write(f"{key}, {master_dictionary[key]}\n")

def subdomain_counter(input_file_name):
    print("#####################################################")
    hostname_list = list()
    lineNo = 0
    lines_found = 0
    with open(input_file_name, 'r') as input_file:
        for line in input_file:
            lineNo += 1
            if not line.strip():
                print("skipping blank")
                continue
            lines_found += 1
            print("linesfound ", lines_found)
            line_json=json.loads(line.encode("utf-8"))
            url = line_json['URL']
            if not re.match(r"^https?://(.*\.)?ics.uci.edu(/.*$|/?$)", url):
                continue
            print(f"url match, {url}")
            hostname_list.append(urlparse(url).hostname)
    hostname_freq_dict = compute_word_frequencies(hostname_list)
    with open("./hostname_frequency.csv", 'w') as hostname_frequency:
        for key in hostname_freq_dict.keys():
            hostname_frequency.write(f"{key}, {hostname_freq_dict[key]}\n")


if __name__ == "__main__":
    if  len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <input_file_path>")
        sys.exit()

    compute_page_tokenlength(sys.argv[1])
    subdomain_counter(sys.argv[1])