from database import Database

class Session_mgr:
    """Description"""

    database = Database()
    def __init__(self):
        print(self.__class__.__name__, "init")

        pass

    def __del__(self):
        print(self.__class__.__name__, "del")

        pass

    def print_db_statistics(self):
        indexed_count = len(self.database.get_visited_urls())
        structure_file = open("{}.structure".format(indexed_count), "w", encoding="utf-8")
        urls_top_file = open("{}.utop".format(indexed_count), "w", encoding="utf-8")
        words_top_file = open("{}.wtop".format(indexed_count), "w", encoding="utf-8")

        self.database.print_db_structure(structure_file)
        self.database.print_db_urls_top(urls_top_file)
        self.database.print_db_words_top(words_top_file)

        structure_file.close()
        urls_top_file.close()
        words_top_file.close()

        pass

    def save_parsed_page(self, parsed_page):
        self.database.save_words(parsed_page.url, parsed_page.words_list)
        self.database.save_links(parsed_page.url, parsed_page.links_list)

        self.print_db_statistics()

        pass

    def indexed_pages(self):
        return self.database.get_visited_urls()

    def saved_pages(self):
        return self.database.get_saved_urls()


class Parsed_page:
    """Description"""

    def __init__(self, url, words_list, links_list):
        print(self.__class__.__name__, "init")
        self.url = url
        self.words_list = words_list
        self.links_list = links_list

        pass

    def __del__(self):
        print(self.__class__.__name__, "del")

        pass
