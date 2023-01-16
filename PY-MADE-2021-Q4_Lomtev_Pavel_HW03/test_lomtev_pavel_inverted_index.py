import pytest
from textwrap import dedent

from task_lomtev_pavel_inverted_index import InvertedIndex, load_documents, build_inverted_index, process_queries, \
    callback_build

DATASET_BIG_FPATH = r"test_inverted.txt"

DATASET_TINY_STR = dedent("""\
        123\tsome words A_word and nothing
        2\tsome word B_word in this dataset
        5\tfamous_phrases to be or not to be
        37\tall words such as A_word and B_word are here
""")

QUERY_STR = dedent("""\
        a_word
        b_word
        a_word b_word
        word_does_not_exist
""")


@pytest.fixture()
def tiny_dataset_fio(tmpdir):
    dataset_fio = tmpdir.join("dataset.txt")
    with open(dataset_fio, "w", encoding="utf-8") as fin:
        fin.write(DATASET_TINY_STR)
    return dataset_fio


@pytest.fixture()
def tiny_dataset_inverted_index_fio_json(tmpdir):
    inverted_index_fio = tmpdir.join("inverted.index")
    with open(inverted_index_fio, "w", encoding="utf-8") as fin:
        fin.write(DATASET_TINY_STR)
    documents = load_documents(inverted_index_fio)
    tiny_inverted_index = build_inverted_index(documents)
    tiny_inverted_index.dump(inverted_index_fio, "json")
    return inverted_index_fio


@pytest.fixture()
def tiny_dataset_inverted_index_fio_struct(tmpdir):
    inverted_index_fio = tmpdir.join("inverted.index")
    with open(inverted_index_fio, "w", encoding="utf-8") as fin:
        fin.write(DATASET_TINY_STR)
    documents = load_documents(inverted_index_fio)
    tiny_inverted_index = build_inverted_index(documents)
    tiny_inverted_index.dump(inverted_index_fio, "struct")
    return inverted_index_fio


@pytest.fixture()
def query_cp1251(tmpdir):
    query_fio = tmpdir.join("cp1251.txt")
    with open(query_fio, "w", encoding="cp1251") as fin:
        fin.write(QUERY_STR)
    return query_fio


@pytest.fixture()
def query_utf8(tmpdir):
    query_fio = tmpdir.join("utf8.txt")
    with open(query_fio, "w", encoding="utf-8") as fin:
        fin.write(QUERY_STR)
    return query_fio


def test_can_load_documents(tiny_dataset_fio):
    documents = load_documents(tiny_dataset_fio)
    etalon_documents = {
        123: "some words a_word and nothing",
        2: "some word b_word in this dataset",
        5: "famous_phrases to be or not to be",
        37: "all words such as a_word and b_word are here",
    }
    assert etalon_documents == documents, (
        "load_documents incorrectly loaded dataset"
    )


@pytest.mark.parametrize(
    "query, etalon_answer",
    [
        pytest.param(["a_word"], [123, 37], id="A_word"),
        pytest.param(["b_word"], [2, 37], id="B_word"),
        pytest.param(["a_word", "b_word"], [37], id="both words"),
        pytest.param(["word_does_not_exist"], [], id="word does not exist"),
    ],
)
def test_query_inverted_index_intersect_results(tiny_dataset_fio, query, etalon_answer):
    documents = load_documents(tiny_dataset_fio)
    tiny_inverted_index = build_inverted_index(documents)
    answer = tiny_inverted_index.query(query)
    assert sorted(answer) == sorted(etalon_answer), (
        f"Expected answer is {etalon_answer}, but you got {answer}"
    )


@pytest.mark.parametrize(
    "queries, etalon_answer",
    [
        pytest.param([["a_word"], ["b_word"], ["a_word", "b_word"], ["word_does_not_exist"]],
                     [[123, 37], [2, 37], [37], []]),
    ],
)
def test_query_inverted_index_intersect_results_process_queries_json(tiny_dataset_inverted_index_fio_json, queries,
                                                                     etalon_answer):
    answer = process_queries(tiny_dataset_inverted_index_fio_json, queries, "json")
    print(answer)
    assert sorted(answer) == sorted(etalon_answer), (
        f"Expected answer is {etalon_answer}, but you got {answer}"
    )


@pytest.mark.parametrize(
    "queries, etalon_answer",
    [
        pytest.param([["a_word"], ["b_word"], ["a_word", "b_word"], ["word_does_not_exist"]],
                     [[123, 37], [2, 37], [37], []]),
    ],
)
def test_query_inverted_index_intersect_results_process_queries_struct(tiny_dataset_inverted_index_fio_struct, queries,
                                                                       etalon_answer):
    answer = process_queries(tiny_dataset_inverted_index_fio_struct, queries, "struct")
    print(answer)
    assert sorted(answer) == sorted(etalon_answer), (
        f"Expected answer is {etalon_answer}, but you got {answer}"
    )


@pytest.fixture()
def wikipedia_documents():
    documents = load_documents(DATASET_BIG_FPATH)
    return documents


def test_can_build_and_query_inverted_index(wikipedia_documents):
    wikipedia_inverted_index = build_inverted_index(wikipedia_documents)
    doc_ids = wikipedia_inverted_index.query(["wikipedia"])
    assert isinstance(doc_ids, list), "inverted index query should return list"


@pytest.fixture()
def wikipedia_inverted_index(wikipedia_documents):
    wikipedia_inverted_index = build_inverted_index(wikipedia_documents)
    return wikipedia_inverted_index


def test_can_dump_and_load_inverted_index_strategy_json(tmpdir, wikipedia_inverted_index):
    index_fio = tmpdir.join("index.dump")
    wikipedia_inverted_index.dump(index_fio, "json")
    loaded_inverted_index = InvertedIndex.load(index_fio, "json")
    assert wikipedia_inverted_index == loaded_inverted_index, (
        "load should return the same inverted index"
    )


def test_can_dump_and_load_inverted_index_strategy_struct(tmpdir, wikipedia_inverted_index):
    index_fio = tmpdir.join("index.dump")
    wikipedia_inverted_index.dump(index_fio, "struct")
    loaded_inverted_index = InvertedIndex.load(index_fio, "struct")
    assert wikipedia_inverted_index == loaded_inverted_index, (
        "load should return the same inverted index"
    )


def test_load_incorrect_filepath(tmpdir, wikipedia_inverted_index, capsys):
    index_fio = tmpdir.join("index.dump")
    wikipedia_inverted_index.dump(index_fio, "struct")
    incorrect_filepath = ''
    InvertedIndex.load(incorrect_filepath, "struct")
    captured = capsys.readouterr()
    assert f"{incorrect_filepath} not found!" in captured.err


def test_callback_build_json(tmpdir, wikipedia_inverted_index):
    index_fio = tmpdir.join("index.dump")

    class Arguments:
        def __init__(self):
            self.dataset = DATASET_BIG_FPATH
            self.output = index_fio
            self.strategy = "json"

    arguments = Arguments()
    callback_build(arguments)
    loaded_inverted_index = InvertedIndex.load(index_fio, "json")
    assert wikipedia_inverted_index == loaded_inverted_index, (
        "load should return the same inverted index"
    )


def test_callback_build_struct(tmpdir, wikipedia_inverted_index):
    index_fio = tmpdir.join("index.dump")

    class Arguments:
        def __init__(self):
            self.dataset = DATASET_BIG_FPATH
            self.output = index_fio
            self.strategy = "struct"

    arguments = Arguments()
    callback_build(arguments)
    loaded_inverted_index = InvertedIndex.load(index_fio, "struct")
    assert wikipedia_inverted_index == loaded_inverted_index, (
        "load should return the same inverted index"
    )
