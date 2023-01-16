from __future__ import annotations

import json
import re
from functools import reduce
from typing import Dict, List


class InvertedIndex:
    def __init__(self, data: Dict[str, list]):
        self.inverted_index = data

    def query(self, words: List[str]) -> List[int]:
        """Return the list of relevant documents for the given query"""
        if len(words) > 0 and isinstance(words, list):
            result_set = reduce(set.intersection,
                                [set(self.inverted_index.get(word))
                                 if word in self.inverted_index
                                 and len(self.inverted_index.get(word)) > 0
                                 else set() for word in words])
            return list(result_set)
        return []

    def dump(self, filepath: str) -> None:
        try:
            with open(filepath, 'w', encoding="utf-8") as fout:
                json.dump(self.inverted_index, fout)
        except (FileNotFoundError, TypeError):
            print("Input valid filepath")

    @classmethod
    def load(cls, filepath: str) -> InvertedIndex:
        try:
            with open(filepath, 'r', encoding="utf-8") as fin:
                inverted_index = json.load(fin)
        except FileNotFoundError:
            print(f"{filepath} not found!")
            return InvertedIndex({})

        return InvertedIndex(inverted_index)

    def __eq__(self, other):
        return self.inverted_index == other.inverted_index


def load_documents(filepath: str) -> Dict[int, str]:
    data = {}
    with open(filepath, 'r', encoding="utf-8") as fin:
        lines_list = fin.readlines()
        for line in lines_list:
            try:
                doc_id, content = line.lower().split("\t", 1)
                doc_id = int(doc_id)
                content = content.strip("\n")
                data[doc_id] = content
            except ValueError:
                continue
    return data


def build_inverted_index(documents: Dict[int, str]) -> InvertedIndex:
    inverted_data = {}
    for key, value in documents.items():
        words = re.split(r"\W+", value)
        for word in words:
            if word in inverted_data:
                inverted_data[word].add(key)
            else:
                inverted_data[word] = set()
                inverted_data[word].add(key)
    for key, value in inverted_data.items():
        inverted_data[key] = list(value)
    return InvertedIndex(inverted_data)


def main():
    pass


if __name__ == "__main__":
    main()
