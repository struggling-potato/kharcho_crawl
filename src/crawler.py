from session_mgr import Session_mgr, Parsed_page
import datetime
import requests
import bs4
import re

class Crawler:
    """Description"""

    def __init__(self, session_mgr, initial_page_list=["https://barnaul.drom.ru/toyota/sprinter_trueno/40394300.html"], max_depth = 1):
        """Attrs desctiption"""
        print(self.__class__.__name__, "init")
        self.session_mgr = session_mgr
        self.initial_page_list = initial_page_list
        self.depth = max_depth

    def __del__(self):
        print(self.__class__.__name__, "del")
        pass

    socials_list = ["vk.com", "ok.ru", "twitter.com", "instagram.com", "tiktok.com", "facebook.com"]
    @staticmethod
    def find_socials(str):
        for social in Crawler.socials_list:
            found_idx = str.find(social)
            if -1 != found_idx:
                return [social, found_idx]
        pass

    @staticmethod
    def plain_text(doc_soup):
        text = []

        notext_tags = doc_soup.find_all(["link", "noscript", "header", "meta", "input", "script", "style", "app-content", "footer", "nav"])
        for tag in notext_tags:
            tag.decompose()

        for tag in doc_soup.find_all(["head", "body"]):
            for str in re.split(r"\n+", tag.prettify()):
                text_str = re.match(r"\s*([\wА-Яа-я][\wА-Яа-я\s\W]*)", str)
                if text_str:
                    str_words = re.split(r"\s", text_str.group(1))
                    final_words = []
                    for word in str_words:
                        final_word = re.match(r"\W*(([\wА-Яа-я\W]+(?<=[\wА-Яа-я])))\W*$", word)
                        if final_word:
                            final_words.append(final_word.group(1).lower())

                    # .append for list of lists
                    text.extend(final_words)
        return text

    @staticmethod
    def page_links(doc_soup):
        links_list = []

        links = doc_soup.find_all("a")

        added_links = set()
        for link in links:
            if ('href' in link.attrs):
                new_link = link.attrs['href']

                if new_link[0:4] == "http":
                    found_social = Crawler.find_socials(new_link)
                    if found_social:
                        print("Socials' link found({}), filtering url {}".format(found_social[0], new_link))
                        continue

                    if new_link not in added_links:
                        text_words = []
                        text_str = re.match(r"\s*\W*([\wА-Яа-я\W]+)", link.text)
                        if text_str:
                            space_splits = re.split(r"\s+", text_str.group(1))
                            for word in space_splits:
                                word_match = re.match(r"\W*(([\wА-Яа-я\W]+(?<=[\wА-Яа-я])))\W*$", word)
                                if word_match:
                                    text_words.append(word_match.group(1).lower())

                        added_links.add(new_link)
                        links_list.append([new_link, text_words])

        print("Added {} new links".format(len(links_list)))

        return links_list


    def crawl(self):
        print(self.__class__.__name__, "crawl")
        print("Pages:", self.initial_page_list)
        print("Depth:", self.depth)

        # TODO: volatile url_set
        url_set = set()
        urls_list = self.session_mgr.saved_pages()
        for url in urls_list:
            url_set.add(url)

        for url in self.initial_page_list:
            url_set.add(url)
        depth_line = len(url_set)

        print("Crawling", url_set)
        crawled_count = 0
        cur_depth_set = url_set.copy()
        # TODO: volatile visited_urls
        visited_urls = set()
        visited_list = self.session_mgr.indexed_pages()
        for url in visited_list:
            visited_urls.add(url)

        print("Visited:", visited_urls)

        for cur_depth in range(self.depth):
            print("Visiting {} depth set(len={})".format(cur_depth + 1, len(cur_depth_set)))
            for url in cur_depth_set:
                if url in visited_urls:
                    print("Skipping visited url {}".format(url))
                    continue
                crawled_count += 1
                cur_time = datetime.datetime.now().time()

                try:
                    print("{}/{} {} Requesting {}...". format(crawled_count, len(cur_depth_set), cur_time, url))
                    html = requests.get(url)
                    html_doc = requests.get(url)
                except Exception as e:
                    print(e)
                    continue

                soup = bs4.BeautifulSoup(html_doc.text, "html.parser")
                page_title = ""
                if soup.title:
                    print("Title:", soup.title.text)
                    page_title = soup.title.text
                visited_urls.add(url)

                page_text = self.plain_text(soup)

                page_links = self.page_links(soup)

                for link in page_links:
                    url_set.add(link[0])

                parsed_page = Parsed_page(url, page_text, page_links)
                self.session_mgr.save_parsed_page(parsed_page)

            cur_depth_set = url_set.copy()

        print("Crawled", crawled_count)

        pass
