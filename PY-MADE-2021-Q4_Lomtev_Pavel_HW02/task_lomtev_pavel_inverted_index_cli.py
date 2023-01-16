"""
Library for working with an inverted index

use query to find relevant documents for the given query
use dump to save inverted index
use load to load inverted index from hard desc
"""

from __future__ import annotations

import os
import json
import re
from functools import reduce
from typing import Dict, List
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

CURRENT_DIR = os.getcwd()
DUMP_PATH = CURRENT_DIR + r"\wikipedia_sample"
OUTPUT_PATH = CURRENT_DIR + r"\wikipedia_inverted.index"


class InvertedIndex:
    """Realization inverted index"""

    def __init__(self, data: Dict[str, list]):
        """initialization inverted index

        :param data: - it's dict value where key it's word (str) and value its list of text id
        """
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
        """Save to filepath dump of inverted index in json format"""
        try:
            with open(filepath, 'w', encoding="utf-8") as fout:
                json.dump(self.inverted_index, fout)
        except (FileNotFoundError, TypeError):
            print("Input valid filepath")

    @classmethod
    def load(cls, filepath: str) -> InvertedIndex:
        """Load from filepath data and return instance of InvertedIndex"""
        try:
            with open(filepath, 'r', encoding="utf-8") as fin:
                inverted_index = json.load(fin)
        except FileNotFoundError:
            print(f"{filepath} not found!")
            return InvertedIndex({})

        return InvertedIndex(inverted_index)

    def __eq__(self, other):
        """Comparison instances of InvertedIndex by dicts"""
        return self.inverted_index == other.inverted_index


def load_documents(filepath: str) -> Dict[int, str]:
    """Load data from filepath, divides to keys and values,
     where key - text id, value - text, return dict
     """
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
    """Build instance of InvertedIndex from dict"""
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


def callback_build(arguments):
    """Realization callback build parser"""
    if arguments.strategy == "json":
        documents = load_documents(arguments.dataset)
        inverted_index = build_inverted_index(documents)
        inverted_index.dump(arguments.output)
    else:
        pass


def callback_query(arguments):
    """Realization callback query parser"""
    inverted_index = InvertedIndex.load(arguments.json_index)
    for query in arguments.query:
        documents_ids = inverted_index.query(query)
        print(*documents_ids, sep=',')


def setup_parser(parser):
    """Realization setup parser"""
    subparsers = parser.add_subparsers(help="choose command")

    build_parser = subparsers.add_parser(
        "build",
        help="build inverted index and save in binary format into hard drive",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )

    build_parser.add_argument(
        "--strategy",
        default="json",
        choices=["json", "pickle"],
        help="strategy saves InvertedIndex",
        dest="strategy",
    )

    build_parser.add_argument(
        "--dataset",
        help="path to dump dataset",
        default=DUMP_PATH,
        dest="dataset",
    )

    build_parser.add_argument(
        "--output",
        help="path to load dataset",
        dest="output",
        default=OUTPUT_PATH,
    )

    build_parser.set_defaults(callback=callback_build)

    query_parser = subparsers.add_parser(
        "query",
        help="query inverted index",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )

    query_parser.add_argument(
        "--json-index",
        default=OUTPUT_PATH,
        help="json filepath to load and take query",
        dest="json_index",
    )

    query_parser.add_argument(
        "--query",
        action="append",
        help="query to run against inverted index",
        nargs="*",
        dest="query",
    )

    query_parser.set_defaults(callback=callback_query)


def main():
    """Main function"""
    parser = ArgumentParser(
        description="realization Inverted Index CLI",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    setup_parser(parser)
    arguments = parser.parse_args()

    arguments.callback(arguments)


if __name__ == "__main__":
    main()
