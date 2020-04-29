from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


config = None
def main(config_file, restart):
    crawler = None

    try:
        cparser = ConfigParser()
        cparser.read(config_file)
        global config
        config = Config(cparser)
        config.cache_server = get_cache_server(config, restart)
        crawler = Crawler(config, restart)
        crawler.start()
    except KeyboardInterrupt:
        if crawler:
            crawler.frontier.save.sync()
        print('Keyboard Interrupt Detected !!')
    finally:
        if crawler:
            crawler.frontier.close()
        print('Goodbye !!')

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
    print('ALL DONE!!!!!!')
