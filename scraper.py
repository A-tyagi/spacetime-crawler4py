import io
import json
import re
import threading
import utils
from bs4 import BeautifulSoup
from threading import Thread
from urllib.parse import urlparse
from urllib.parse import urljoin
from utils import get_logger

logger = get_logger(f"Scraper", "Scraper")
logger.info("Creating thread lock for scraper output file")
scarper_filelock = threading.Lock()

def scraper(source_url, resp):
    print('')
    logger.info(f"SCRAPING: {source_url}")
    print('')
    print('-'*250)
    if resp.status != 200 or resp.raw_response is None:
        logger.info(f"URL: {source_url}, status: {resp.status}, status not 200 or raw_response is empty")
        return []
    else:
        return proccess_raw_response(source_url, resp.raw_response)

def proccess_raw_response(source_url, raw_response):
    page_soup = None
    page_content = None
    page_links = None

    try:
        page_soup = BeautifulSoup(raw_response.content, 'html.parser')
        page_content = page_soup.get_text()
    except UnicodeDecodeError:
        logger.exception(f"UnicodeDecodeError for URL: {source_url}")
        page_soup = BeautifulSoup(raw_response.content.decode('latin1'), 'html.parser')
        page_content = page_soup.get_text()
    except BaseException:
        logger.exception(f"Unexpected exception: for URL: {source_url}")
        return []
    finally:
        if page_soup is None:
            logger.info(f"Failed to create page_soup: {source_url}")
            return []
        elif page_content is None:
            logger.info(f"Failed get text content: {source_url}")
            return []

    page_links = find_page_links(source_url, page_soup)
    page_tokens = find_page_tokens(page_content)
    
    data_entry = {'URL': source_url, 'token_list': page_tokens}
    scarper_filelock.acquire()
    try:
        with open('data.json', 'a') as file:
            line = json.dumps(data_entry)
            file.write(line + '\n')
    finally:
        scarper_filelock.release()

    print('-'*250)
    print('')
    print('')
    logger.info(f"URL: {source_url}, status: {raw_response.status_code}, Tokens: {len(page_tokens)}, Links: {len(page_links)}")
    return page_links

def find_page_tokens(page_text_content):
    token_list = re.split(r'[^A-Za-z0-9]', page_text_content)
    stop_words = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'aren', 't', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 
                 'below', 'between', 'both', 'but', 'by', 'can', 't', 'cannot', 'could', 'couldn', 't', 'did', 'didn', 't', 'do', 'does', 'doesn', 't', 'doing', 'don', 't', 'down',
                 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', 't', 'has', 'hasn', 't', 'have', 'haven', 't', 'having', 'he', 'he', 'd', 'he', 'll', 'he', 's', 
                 'her', 'here', 'here', 's', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'how', 's', 'i', 'i', 'd', 'i', 'll', 'i', 'm', 'i', 've', 'if', 'in', 'into', 'is', 
                 'isn', 't', 'it', 'it', 's', 'its', 'itself', 'let', 's', 'me', 'more', 'most', 'mustn', 't', 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only',
                 'or', 'other', 'ought', 'our', 'ours	ourselves', 'out', 'over', 'own', 'same', 'shan', 't', 'she', 'she', 'd', 'she', 'll', 'she', 's', 'should', 'shouldn', 't',
                 'so', 'some', 'such', 'than', 'that', 'that', 's', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'there', 's', 'these', 'they', 'they', 'd', 'they',
                 'll', 'they', 're', 'they', 've', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', 'wasn', 't', 'we', 'we', 'd', 'we', 'll', 'we', 're',
                 'we', 've', 'were', 'weren', 't', 'what', 'what', 's', 'when', 'when', 's', 'where', 'where', 's', 'which', 'while', 'who', 'who', 's', 'whom', 'why', 'why', 's', 'with',
                  'won', 't', 'would', 'wouldn', 't', 'you', 'you', 'd', 'you', 'll', 'you', 're', 'you', 've', 'your', 'yours', 'yourself', 'yourselves']
    token_list = [token.lower() for token in token_list if token and len(token) > 1 and token not in stop_words ]
    return token_list

def find_page_links(source_url, page_soup):
    page_links = set()
    for link in page_soup.find_all('a'):
        href_link = link.get('href')
        if href_link is None:
            continue
        href_link = href_link.strip()
        if (href_link == '') or (href_link == '/') or href_link.startswith('#') or ('mailto:' in href_link):
            continue

        linked_url = urljoin(source_url, href_link)

        if "#" in linked_url:
            linked_url = linked_url.split('#')[0]
        if linked_url.endswith("/"):
            linked_url = linked_url.rstrip("/")
        linked_url = linked_url.strip().lower()

        if is_valid(linked_url):
            page_links.add(linked_url)
    
    return list(page_links)

def is_valid(url):
    try:
        if not re.match(r"^https?://(.*\.)?"
            + r"(ics.uci.edu" 
            + r"|cs.uci.edu" 
            + r"|informatics.uci.edu" 
            + r"|stat.uci.edu"
            + r"|today.uci.edu/department/information_computer_sciences)" 
            + r"(/.*$|/?$)", url):
            logger.info(f"{url} - out of scope, skipping")
            return False

        parsed = urlparse(url)

        if re.match(
            r".*\.(css|bmp|gif|jpe?g|ico|lif|ss|rkt|ppsx?"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpe?g|ram|m4v|mkv|ogg|ogv|pdf|bib"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|tarz|gzip|ova|ovf"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|js|c|cpp|h|py|m|img|r|fig|rle"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|woff|woff2|svg|ico|war|xml)$", parsed.path.lower()) :
            return False

        return not low_value_trap(url, parsed)
    except BaseException:
        logger.exception(f"Unexpected exception in is_valid for URL: {url}")
        return False


def low_value_trap(url, parsed):
    external_reference = re.compile(r'^.*google.com.*$|^.*facebook.com.*$|^.*twitter.com.*$')
    calendar_events = re.compile(r'^.*calendar.*$|^.*\?ical.*$|^https://wics.ics.uci.edu/events?/.*$')
    low_value = re.compile(r'^.*download.*$|^.*\?replytocom=.*$|^.*doku.php.*$|^.*&format=xml.*$|^.*/wp-json.*$'
        + r'|^.*oembed.*$|^.*/ml/machine-learning-databases.*$|^.*/ml/datasets.*$|^.*/run_maize.*$|^.*/lineup_sql.pl.*$'
        + r'|^.*\.gif.*$|^.*/ads/dl.js.*$|^.*template.*$|^.*action=login.*$|^.*action=edit.*$|^.*filelist.*$|^.*img_.*$'
        + r'|^.*.gif>.*$|^.*?.m$|^.*.jpg$|^.*.gz$|^.*.pdf$|^.*.zip$|^.*feed(.php)?$')

    trap_pattern = re.compile(r'^.*?(/.+?/).*?\1.*$|^.*/index.php/.*$|^.*(\.php.*){2}$|^.*\s.*$|^.*â.*$')
    
    more_trap_patterns = re.compile(r'^http://swiki.ics.uci.edu/lib/exe/indexer.php?id=.*$'
        + r'|^https://www.ics.uci.edu/honors/.*$'
        + r'|^http://www.ics.uci.edu/~eppstein/pix/.*$')

    if re.match(external_reference, url):
        logger.info(f"{url} - Match in external_reference, skipping")
        return True
    elif re.match(calendar_events, url):
        logger.info(f"{url} - Match in calendar_events, skipping")
        return True        
    elif re.match(low_value, url):
        logger.info(f"{url} - Match in low_value, skipping")
        return True
    elif re.match(trap_pattern, url):
        logger.info(f"{url} - Match in trap_pattern, skipping")
        return True
    elif re.match(more_trap_patterns, url):
        logger.info(f"{url} - Match in more_trap_patterns, skipping")
        return True
    

    return False
