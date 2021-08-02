import pytest
import typing
import experimenter
import sqlite3
from dataclasses import dataclass
from contextlib import contextmanager

@contextmanager
def database_cursor() -> typing.Iterator[sqlite3.Cursor]:
    con = sqlite3.connect(':memory:')
    cur = con.cursor()
    try:
        yield cur
    finally:
        con.close()


def test_empty_dataclass_has_no_fields() -> None:
    @dataclass
    class Empty:
        pass

    fields = experimenter.dataclass_to_field_specs(Empty)
    assert fields == []


def test_dataclass_with_all_root_types() -> None:
    @dataclass
    class LotsOfColumns:
        int_value: int
        float_value: float
        str_value: str
        bytes_value: bytes

    expected_fields = [
        experimenter.ColumnSpec(name="int_value",   type="INTEGER", mods="NOT NULL"),
        experimenter.ColumnSpec(name="float_value", type="REAL",    mods="NOT NULL"),
        experimenter.ColumnSpec(name="str_value",   type="TEXT",    mods="NOT NULL"),
        experimenter.ColumnSpec(name="bytes_value", type="BLOB",    mods="NOT NULL"),
    ]

    fields = experimenter.dataclass_to_field_specs(LotsOfColumns)
    assert fields == expected_fields


def test_optional_becomes_nullable() -> None:
    @dataclass
    class LotsOfColumns:
        maybe_str: typing.Optional[str]

    expected_fields = [
        experimenter.ColumnSpec(name="maybe_str", type="TEXT", mods=""),
    ]

    fields = experimenter.dataclass_to_field_specs(LotsOfColumns)
    assert fields == expected_fields


def test_function_with_no_params() -> None:
    def stupid() -> None: pass

    columns = experimenter.function_args_to_columns(stupid)
    assert columns == []


def test_function_with_params() -> None:
    def stupid(x: int, y: typing.Optional[str]) -> None: pass

    columns = experimenter.function_args_to_columns(stupid)
    assert columns == [
        experimenter.ColumnSpec(name="x", type="INTEGER", mods="NOT NULL"),
        experimenter.ColumnSpec(name="y", type="TEXT",    mods=""),
    ]


def test_function_with_keyword_only_int_param() -> None:
    def stupid(*, x: int) -> None: pass

    columns = experimenter.function_args_to_columns(stupid)
    assert columns == [
        experimenter.ColumnSpec(name="x", type="INTEGER", mods="NOT NULL"),
    ]


@pytest.mark.xfail
def test_function_with_args() -> None:
    def stupid(*args: typing.List[typing.Any]) -> None: pass

    with pytest.raises(ValueError):
        experimenter.function_args_to_columns(stupid)


@pytest.mark.xfail
def test_function_with_kwargs() -> None:
    def stupid(*kwargs: typing.Mapping[str, typing.Any]) -> None: pass

    with pytest.raises(ValueError):
        experimenter.function_args_to_columns(stupid)


def test_function_to_create_table() -> None:
    @dataclass
    class Return:
        x: int
        y: str

    def test(a: float, b: bytes) -> Return:
        return Return(x=0, y="")

    expected = """\
CREATE TABLE TestTable(
    a REAL NOT NULL,
    b BLOB NOT NULL,
    x INTEGER NOT NULL,
    y TEXT NOT NULL
)"""

    create_table_sql = experimenter.function_to_create_table_sql("TestTable", test)

    assert expected == create_table_sql


def test_function_to_insert_parmeterized() -> None:
    @dataclass
    class Return:
        x: int
        y: str

    def test(a: float, b: bytes) -> Return:
        return Return(x=0, y="")

    expected_sql = """\
INSERT INTO TestTable(a, b, x, y)
VALUES (?, ?, ?, ?)"""

    insert_info = experimenter.function_to_insert_sql("TestTable", test)

    assert expected_sql == insert_info.sql_text
    assert ["a", "b"] == insert_info.arguments
    assert ["x", "y"] == insert_info.return_fields


def does_test_table_exist(db: sqlite3.Cursor) -> bool:
    matches = list(db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'"))
    assert matches == [] or matches == [("test",)]
    return len(matches) == 1


def test_maybe_create_table_creates_if_doesnt_exist() -> None:
    with database_cursor() as db:
        assert not does_test_table_exist(db)
        experimenter.maybe_create_table(db, "test", "CREATE TABLE test (x INTEGER NOT NULL)")
        assert does_test_table_exist(db)

def test_maybe_create_table_does_not_creates_if_does_exist() -> None:
    with database_cursor() as db:
        assert not does_test_table_exist(db)
        db.execute("CREATE TABLE test (x INTEGER NOT NULL)")
        assert does_test_table_exist(db)

        experimenter.maybe_create_table(db, "test", "CREATE TABLE test (x INTEGER NOT NULL)")
        assert does_test_table_exist(db)


def test_inserter() -> None:
    @dataclass
    class Return:
        quot: int
        rem: str

    with database_cursor() as db:
        @experimenter.experiment(table_name="test", db=db)
        def div_rem(a: int, b: int) -> Return:
            return Return(
                quot=a//b,
                rem=str(a % b),
            )

        div_rem(12, 5)
        inserted_rows = list(db.execute("SELECT * FROM test"))
        assert inserted_rows == [
            (12, 5, 2, '2'),
        ]

        div_rem(20, 7)
        inserted_rows = list(db.execute("SELECT * FROM test"))
        assert inserted_rows == [
            (12, 5, 2, '2'),
            (20, 7, 2, '6'),
        ]
