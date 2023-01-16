"""
Library for working with an inverted index

use query to find relevant documents for the given query
use dump to save inverted index
use load to load inverted index from hard desc
"""

from __future__ import annotations
import os
import sys
import json
import re
from functools import reduce
from typing import Dict, List
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType, ArgumentTypeError
from io import TextIOWrapper
import struct

CURRENT_DIR = os.getcwd()
DUMP_PATH = CURRENT_DIR + r"\wikipedia_sample"
OUTPUT_PATH = CURRENT_DIR + r"\wikipedia_inverted.index"


class EncodedFileType(FileType):
    def __call__(self, string):
        if string == '-':
            if 'r' in self._mode:
                stdin = TextIOWrapper(sys.stdin.buffer, encoding=self._encoding)
                return stdin
            elif 'w' in self._mode:
                stdout = TextIOWrapper(sys.stdout.buffer, encoding=self._encoding)
                return stdout
            else:
                msg = "argument '-' with mode %r" % self._mode
                raise ValueError(msg)
        try:
            return open(string, self._mode, self._bufsize, self._encoding, self._errors)
        except OSError as e:
            message = "can't open '%s': %s"
            raise ArgumentTypeError(message % (string, e))


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

    def dump(self, filepath: str, strategy: str) -> None:
        """Save to filepath dump of inverted index in json format"""
        try:
            if strategy == "json":
                with open(filepath, 'w', encoding="utf-8") as fout:
                    json.dump(self.inverted_index, fout)
            elif strategy == "struct":
                pairs = []
                doc_ids = []
                with open(filepath, 'wb') as fout:
                    for key, value in self.inverted_index.items():
                        pairs.append((key, len(value)))
                        for doc_id in value:
                            doc_ids.append(doc_id)
                    header = json.dumps(pairs).encode("utf-8")
                    size = len(header)
                    pack = struct.pack("I", size)
                    fout.write(pack)
                    pack = struct.pack(f"{size}s", header)
                    fout.write(pack)
                    pack = struct.pack(f"{len(doc_ids)}H", *doc_ids)
                    fout.write(pack)
        except (FileNotFoundError, TypeError):
            print("Input valid filepath", file=sys.stderr)

    @classmethod
    def load(cls, filepath: str, strategy: str) -> InvertedIndex:
        """Load from filepath data and return instance of InvertedIndex"""
        try:
            if strategy == "json":
                with open(filepath, 'r', encoding="utf-8") as fin:
                    inverted_index = json.load(fin)
            else:
                with open(filepath, 'rb') as fin:
                    calcsize = struct.calcsize('I')
                    header_size, = struct.unpack("I", fin.read(calcsize))
                    header, = struct.unpack(f"{header_size}s", fin.read(header_size))
                    header = json.loads(header.decode("utf-8"))
                    inverted_index = {}
                    for item in header:
                        values_to_read = item[1]
                        calcsize = struct.calcsize(f"{values_to_read}H")
                        values = list(struct.unpack(f"{values_to_read}H", fin.read(calcsize)))
                        inverted_index[item[0]] = values
        except FileNotFoundError:
            print(f"{filepath} not found!", file=sys.stderr)
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
    documents = load_documents(arguments.dataset)
    inverted_index = build_inverted_index(documents)
    inverted_index.dump(arguments.output, arguments.strategy)


def process_queries(inverted_index_filepath, queries, strategy):
    """Realization process queries"""
    inverted_index = InvertedIndex.load(inverted_index_filepath, strategy)
    doc_ans = []
    for query in queries:
        query = query.strip().split() if isinstance(queries, TextIOWrapper) else query
        documents_ids = inverted_index.query(query)
        doc_ans.append(documents_ids)
    return doc_ans


def callback_query(arguments):
    """Realization callback query parser"""
    documents_ids = process_queries(arguments.inverted_index_filepath, arguments.query, arguments.strategy)
    for doc_id in documents_ids:
        print(*doc_id, sep=',')


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
        default="struct",
        choices=["json", "struct"],
        help="strategy to save InvertedIndex",
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

    query_file_group = query_parser.add_mutually_exclusive_group(required=True)

    query_file_group.add_argument(
        "--query",
        action="append",
        help="query to run against inverted index",
        nargs="*",
        dest="query",
    )

    query_file_group.add_argument(
        "--query-file-utf8",
        dest="query",
        type=EncodedFileType("r", encoding="utf-8"),
        default=TextIOWrapper(sys.stdin.buffer, encoding="utf-8"),
        help="query file to get queries for inverted index",
    )

    query_file_group.add_argument(
        "--query-file-cp1251",
        dest="query",
        type=EncodedFileType("r", encoding="cp1251"),
        default=TextIOWrapper(sys.stdin.buffer, encoding="cp1251"),
        help="query file to get queries for inverted index",
    )

    query_parser.add_argument(
        "--index",
        default=OUTPUT_PATH,
        help="filepath to inverted index",
        dest="inverted_index_filepath",
    )

    query_parser.add_argument(
        "--strategy",
        default="struct",
        choices=["struct", "json"],
        dest="strategy",
        help="type of file",
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
