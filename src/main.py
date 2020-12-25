from crawler import Crawler
from session_mgr import Session_mgr
from database import Database

def main():
    session_mgr = Session_mgr()
    crawl = Crawler(session_mgr, ["https://barnaul.drom.ru/toyota/sprinter_trueno/40394300.html", "https://habr.com/ru/post/497808/"], max_depth=2)
    crawl.crawl()
    pass

main()
