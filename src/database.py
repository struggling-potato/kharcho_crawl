import sqlite3
import re

class Database:
    """Description"""

    def __init__(self):
        self.connection = sqlite3.connect("webDB.db")
        #self.__drop_tables()
        curs = self.connection.cursor()

        sql_create_word_list = """
            CREATE TABLE IF NOT EXISTS wordList(
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                word TEXT NOT NULL,
                isFiltered INTEGER
            );
        """

        #print(sql_create_word_list)
        curs.execute(sql_create_word_list)

        sql_create_url_list = """
            CREATE TABLE IF NOT EXISTS urlList(
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL
            );
        """

        #print(sql_create_url_list)
        curs.execute(sql_create_url_list)

        sql_create_word_location = """
            CREATE TABLE IF NOT EXISTS wordLocation(
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                fk_wordid INTEGER,
                fk_urlid INTEGER,
                location INTEGER,
                FOREIGN KEY(fk_wordid) REFERENCES wordList(rowid),
                FOREIGN KEY(fk_urlid) REFERENCES urlList(rowid)
            );
        """

        #print(sql_create_word_location)
        curs.execute(sql_create_word_location)

        sql_create_link_url = """
            CREATE TABLE IF NOT EXISTS urlLink(
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                fk_fromid INTEGER,
                fk_toid INTEGER,
                FOREIGN KEY(fk_fromid) REFERENCES urlList(rowid),
                FOREIGN KEY(fk_toid) REFERENCES urlList(rowid)
            );
        """

        #print(sql_create_link_url)
        curs.execute(sql_create_link_url)

        sql_create_link_word = """
            CREATE TABLE IF NOT EXISTS linkWord(
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                fk_wordid INTEGER,
                fk_linkid INTEGER,
                FOREIGN KEY(fk_wordid) REFERENCES wordList(rowid),
                FOREIGN KEY(fk_linkid) REFERENCES urlList(rowid)
            );
        """

        #print(sql_create_link_word)
        curs.execute(sql_create_link_word)

        self.connection.commit()

        pass

    def __del__(self):

        self.connection.commit()
        self.connection.close()

        pass

    def __drop_tables(self):
        curs = self.connection.cursor()

        sql_drop_word_list = """
            DROP TABLE wordList;
        """

        #print(sql_drop_word_list)
        curs.execute(sql_drop_word_list)

        sql_drop_url_list = """
            DROP TABLE urlList;
        """

        #print(sql_drop_url_list)
        curs.execute(sql_drop_url_list)

        sql_drop_word_location = """
            DROP TABLE wordLocation;
        """

        #print(sql_drop_word_location)
        curs.execute(sql_drop_word_location)

        sql_drop_link_url = """
            DROP TABLE urlLink;
        """

        #print(sql_drop_link_url)
        curs.execute(sql_drop_link_url)

        sql_drop_link_word = """
            DROP TABLE linkWord;
        """

        #print(sql_drop_link_word)
        curs.execute(sql_drop_link_word)

        self.connection.commit()

    def save_words(self, url, words):
        print(self.__class__.__name__, "save_words")

        words_count = len(words)
        print(words_count)
        curs = self.connection.cursor()
        if words_count:
            sql_insert_url = """
                INSERT INTO urlList(url)
                SELECT '{}' WHERE NOT EXISTS
                (
                    SELECT * FROM urlList WHERE url='{}'
                );
            """.format(url, url)
            #print(sql_insert_url)
            result = curs.execute(sql_insert_url)

        for idx in range(words_count):
            word = words[idx]
            word = word.replace("'","''")

            sql_insert_word = """
                INSERT INTO wordList(word, isFiltered)
                SELECT '{}', 0 WHERE NOT EXISTS
                (
                    SELECT * FROM wordList WHERE word='{}'
                );
            """.format(word, word)

            if url=="https://habr.com/ru/post/248611/" or url=="https://forums.drom.ru/":
                print(sql_insert_word)
            result = curs.execute(sql_insert_word)

            sql_insert_word_location = """
                INSERT INTO wordLocation(fk_wordid, fk_urlid, location)
                SELECT wordList.rowid as wordid, urlList.rowid as urlid, {} as loc FROM urlList, wordList
                    WHERE NOT EXISTS
                        (
                            SELECT * FROM wordLocation WHERE fk_wordid=wordid AND fk_urlid=urlid AND location=loc
                        )
                    AND urlList.url='{}' AND wordList.word='{}';
            """.format(idx,
                       url, word)

            #print(sql_insert_word_location)
            result = curs.execute(sql_insert_word_location)

        self.connection.commit()
        pass

    def save_links(self, url, links):
        print(self.__class__.__name__, "save_links")

        links_count = len(links)
        curs = self.connection.cursor()
        if links_count:
            sql_insert_url = """
                INSERT INTO urlList(url)
                SELECT '{}' WHERE NOT EXISTS
                (
                    SELECT * FROM urlList WHERE url='{}'
                );
            """.format(url, url)
            #print(sql_insert_url)
            result = curs.execute(sql_insert_url)

        for idx in range(links_count):
            link = links[idx]
            link_url = link[0]
            link_words = link[1]

            sql_insert_link_url = """
                INSERT INTO urlList(url)
                SELECT '{}' WHERE NOT EXISTS
                (
                    SELECT * FROM urlList WHERE url='{}'
                );
            """.format(link_url, link_url)

            #print(sql_insert_link_url)
            result = curs.execute(sql_insert_link_url)

            sql_insert_urls_link = """
                INSERT INTO urlLink(fk_fromid, fk_toid)
                SELECT list1.rowid as from_url, list2.rowid as to_url FROM urlList as list1, urlList as list2
                    WHERE NOT EXISTS
                        (
                            SELECT * FROM urlLink WHERE fk_fromid=from_url AND fk_toid=to_url
                        )
                    AND list1.url='{}' AND list2.url='{}';
            """.format(url, link_url)

            #print(sql_insert_urls_link)
            result = curs.execute(sql_insert_urls_link)

            for word in link_words:
                word = word.replace("'","''")
                sql_insert_word = """
                    INSERT INTO wordList(word, isFiltered)
                    SELECT '{}', 0 WHERE NOT EXISTS
                    (
                        SELECT * FROM wordList WHERE word='{}'
                    );
                """.format(word, word)

                #print(sql_insert_word)
                result = curs.execute(sql_insert_word)

                sql_insert_link_word = """
                    INSERT INTO linkWord(fk_wordid, fk_linkid)
                    SELECT wordList.rowid as wordid, urlList.rowid as linkid FROM wordList, urlList
                        WHERE NOT EXISTS
                            (
                                SELECT * FROM linkWord WHERE fk_wordid=wordid AND fk_linkid=linkid
                            )
                        AND wordList.word='{}' AND urlList.url='{}';
                """.format(word, link_url)

                #print(sql_insert_link_word)
                result = curs.execute(sql_insert_link_word)

        self.connection.commit()
        pass

    def get_visited_urls(self):
        curs = self.connection.cursor()

        sql_get_visited_urls = """
            SELECT DISTINCT url FROM urlList
            INNER JOIN urlLink on urlLink.fk_fromid=urlList.rowid;
        """

        print(sql_get_visited_urls)
        curs.execute(sql_get_visited_urls)

        urls = curs.fetchall()

        urls_list = []
        for url in urls:
            urls_list.append(url[0])

        return urls_list

    def get_saved_urls(self):
        curs = self.connection.cursor()

        sql_get_saved_urls = """
            SELECT DISTINCT url FROM urlList;
        """

        print(sql_get_saved_urls)
        curs.execute(sql_get_saved_urls)

        urls = curs.fetchall()

        urls_list = []
        for url in urls:
            urls_list.append(url[0])

        return urls_list

    def print_db_structure(self, s_file):
        curs = self.connection.cursor()

        sql_rows_count = """
            SELECT COUNT(*) FROM wordList;
        """

        #print(sql_rows_count)
        curs.execute(sql_rows_count)

        count = curs.fetchone()
        s_file.write("wordList: {}\n".format(count[0]))

        sql_rows_count = """
            SELECT COUNT(*) FROM urlList;
        """

        #print(sql_rows_count)
        curs.execute(sql_rows_count)

        count = curs.fetchone()
        s_file.write("urlList: {}\n".format(count[0]))

        sql_rows_count = """
            SELECT COUNT(*) FROM wordLocation;
        """

        #print(sql_rows_count)
        curs.execute(sql_rows_count)

        count = curs.fetchone()
        s_file.write("wordLocation: {}\n".format(count[0]))

        sql_rows_count = """
            SELECT COUNT(*) FROM urlLink;
        """

        #print(sql_rows_count)
        curs.execute(sql_rows_count)

        count = curs.fetchone()
        s_file.write("urlLink: {}\n".format(count[0]))

        sql_rows_count = """
            SELECT COUNT(*) FROM linkWord;
        """

        #print(sql_rows_count)
        curs.execute(sql_rows_count)

        count = curs.fetchone()
        s_file.write("linkWord: {}\n".format(count[0]))

        pass

    def print_db_urls_top(self, u_file):
        urls_list = self.get_saved_urls()
        domens_dict = {}
        for url in urls_list:
            domen = re.match(r"(http.*\/\/.*?\/).*", url)
            if domen:
                domens_dict[domen.group(1)] = domens_dict.get(domen.group(1), 0) + 1

        sorted_dict = dict(sorted(domens_dict.items(), key=lambda item:item[1], reverse=True))
        for pair in sorted_dict.items():
            u_file.write("{}: {}\n".format(pair[0], pair[1]))

        pass

    def print_db_words_top(self, w_file):
        curs = self.connection.cursor()
        sql_words_sorted_by_count = """
            SELECT lst.word, COUNT(*) as cnt FROM wordList as lst
            INNER JOIN wordLocation as loc ON loc.fk_wordid=lst.rowid
            GROUP BY lst.word
            ORDER BY cnt DESC;
        """

        #print(sql_words_sorted_by_count)
        curs.execute(sql_words_sorted_by_count)

        words = curs.fetchall()

        for word in words:
            w_file.write("{}: {}\n".format(word[0], word[1]))

        pass
