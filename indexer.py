import os
import json
from bs4 import BeautifulSoup
from lxml import html
from urllib.parse import urlparse
import math
from nltk.stem import PorterStemmer

class IndexerM1:
    def __init__(self, seed):
        self.doc_num = 0
        self.num_tok = 0
        self.index = {}
        self.hash_url = {}
        self.seed = seed


    def read_file(self, file):
        tok = {}
        with open(file, 'r') as f:
            data = json.load(f)
            content = data["content"]
            url = data['url']

        url = urlparse(url)._replace(fragment="").geturl()
        if url in self.hash_url.values():
            return None

        soup = BeautifulSoup(content, 'lxml')
        content = soup.get_text()
        if len(content) < 200:
            return None

        ps = PorterStemmer()
        pos = 0
        for line in content.splitlines():
            try:
                for char in line:
                    if not char.isalnum():
                        line = line.replace(char, ' ')
            except:
                pass
            for words in line.split(' '):
                if words != "":
                    words = words.lower()
                    words = ps.stem(words)
                    if words not in tok:
                        tok[words] = []
                    tok[words].append(pos)
                    pos += 1
        if pos < 200:
            return None
        self.doc_num += 1
        self.hash_url[self.doc_num] = url
        return [self.doc_num, tok]



    def get_files(self):
        for root, dirs, files in os.walk(self.seed):
            for file in files:
                yield os.path.join(root, file)

    def create_index(self, tok_l):
        if tok_l is None:
            return None

        for tok in tok_l[1].keys():
            if tok not in self.index:
                self.index[tok] = {}
            self.index[tok][tok_l[0]] = [1 + math.log(len(tok_l[1][tok]), 10), tok_l[1][tok]] #{doc_id: [tf, [index of occurrence]]}

    def finish(self):
        self.num_tok = len(self.index)
        print(f"Number of tokens: {self.num_tok}")
        print(f"Number of documents: {self.doc_num}")

        for tok in self.index.keys():
            self.index[tok] = [len(self.index[tok]), self.index[tok], math.log(self.doc_num/len(self.index[tok]), 10)] #{token: [number of docs for the token, {doc_id: [tf, [index of occurrence]]}, idf]}

        with open("hash_url.json", "w") as outfile:
            json.dump(self.hash_url, outfile, indent=2)

        with open("index.json", "w") as outfile:
            json.dump({i: self.index[i] for i in sorted(self.index.keys())}, outfile, indent=2)


    def run(self):
        for file in self.get_files():
            self.create_index(self.read_file(file))
        self.finish()

if __name__ == '__main__':
    seed = input("Please enter directory path to start. Default = ./analyst/\n")
    try:
        if seed:
            IndexerM1(seed).run()
        else:
            IndexerM1('./analyst/').run()
    except Exception as e:
        print(e)
