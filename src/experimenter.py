import typing
import inspect
import dataclasses
import sqlite3
import functools

# Python 3.8 workaound
if typing.TYPE_CHECKING:
    DataclassesFieldAny = dataclasses.Field[typing.Any]
else:
    DataclassesFieldAny = dataclasses.Field


@dataclasses.dataclass
class ColumnSpec:
    name: str
    type: str
    mods: str

    def to_create_table_sql_line(self) -> str:
        mods = " " + self.mods if self.mods else ""
        return f"    {self.name} {self.type}{mods}"


def unwrap_union(
        python_type: typing.Any,
) -> typing.Optional[typing.List[typing.Any]]:
    if getattr(python_type, "__origin__", None) != typing.Union:
        return None
    else:
        return [arg for arg in python_type.__args__]


def unwrap_optional(python_type: typing.Any) -> typing.Optional[typing.Any]:
    union_args = unwrap_union(python_type)
    if union_args is None or len(union_args) != 2:
        return None

    type1, type2 = union_args
    if type1 == type(None):
        return type2
    elif type2 == type(None):
        return type1
    else:
        return None


def python_type_to_sqlite_type(python_type: typing.Any) -> typing.Tuple[str, str]:
    if (optional_type := unwrap_optional(python_type)) is not None:
        sql_type, mods = python_type_to_sqlite_type(optional_type)
        return sql_type, ""
    elif python_type == int:
        return "INTEGER", "NOT NULL"
    elif python_type == float:
        return "REAL", "NOT NULL"
    elif python_type == str:
        return "TEXT", "NOT NULL"
    elif python_type == bytes:
        return "BLOB", "NOT NULL"
    assert False


def dc_field_to_columnspec(field: DataclassesFieldAny) -> ColumnSpec:
    sql_type, mods = python_type_to_sqlite_type(field.type)
    return ColumnSpec(
        name=field.name,
        type=sql_type,
        mods=mods,
    )


def dataclass_to_field_specs(x: typing.Any) -> typing.List[ColumnSpec]:
    return [
        dc_field_to_columnspec(field)
        for field in dataclasses.fields(x)
    ]


def parameter_to_columnspec(parameter: inspect.Parameter) -> ColumnSpec:
    sql_type, mods = python_type_to_sqlite_type(parameter.annotation)
    return ColumnSpec(
        name=parameter.name,
        type=sql_type,
        mods=mods,
    )
    

def function_args_to_columns(function: typing.Callable[..., typing.Any]) -> typing.List[ColumnSpec]:
    args = inspect.signature(function).parameters.values()
    return [
        parameter_to_columnspec(parameter)
        for parameter in args
    ]


def function_to_columns(function: typing.Callable[..., typing.Any]) -> typing.Tuple[typing.List[ColumnSpec], typing.List[ColumnSpec]]:
    arg_columns = function_args_to_columns(function)
    ret_columns = dataclass_to_field_specs(inspect.signature(function).return_annotation)
    return arg_columns, ret_columns


def function_to_create_table_sql(table_name: str, function: typing.Callable[..., typing.Any]) -> str:
    arg_cols, ret_cols = function_to_columns(function)
    cols = arg_cols + ret_cols
    cols_sql_lines = [col.to_create_table_sql_line() for col in cols]
    cols_sql = ",\n".join(cols_sql_lines)
    return f"CREATE TABLE {table_name}(\n{cols_sql}\n)"
    

@dataclasses.dataclass
class InsertSQLInfo:
    sql_text: str
    arguments: typing.List[str]
    return_fields: typing.List[str]


def function_to_insert_sql(table_name: str, function: typing.Callable[..., typing.Any]) -> InsertSQLInfo:
    arg_cols, ret_cols = function_to_columns(function)
    cols = arg_cols + ret_cols
    col_names = [col.name for col in cols]
    col_names_str = ", ".join(col_names)
    question_marks = ", ".join(["?"] * len(cols))

    sql_text = f"INSERT INTO {table_name}({col_names_str})\nVALUES ({question_marks})"
    return InsertSQLInfo(
        sql_text=sql_text,
        arguments=[col.name for col in arg_cols],
        return_fields=[col.name for col in ret_cols],
    )


def maybe_create_table(db: sqlite3.Cursor, table_name: str, create_table_sql: str) -> None:
    check_rows = list(db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ))
    assert check_rows == [] or check_rows == [(table_name,)]
    if check_rows == []:
        db.execute(create_table_sql)


RetT = typing.TypeVar('RetT')
def do_experiment(
        func: typing.Callable[..., RetT],
        table_name: str,
        db: sqlite3.Cursor,
        *args: typing.Any,
        **kwargs: typing.Any,
) -> RetT:
    bound_args = inspect.getcallargs(func, *args, **kwargs)
    result = func(*args, **kwargs)

    #
    create_table_sql = function_to_create_table_sql(table_name, func)
    maybe_create_table(db, table_name, create_table_sql)

    #
    insert_info = function_to_insert_sql(table_name, func)
    arg_values = [
        bound_args[arg_name]
        for arg_name in insert_info.arguments
    ]
    ret_values = [
        getattr(result, field_name)
        for field_name in insert_info.return_fields
    ]
    db.execute(insert_info.sql_text, arg_values+ret_values)

    return result


# Thanks to https://stackoverflow.com/a/65613529 for getting the types
# of 'wrapped' and the 'cast' call correct
FuncT = typing.TypeVar('FuncT', bound=typing.Callable[..., typing.Any]) 
def experiment(
        table_name: str,
        db: sqlite3.Cursor,
) -> typing.Callable[[FuncT], FuncT]:
    def decorator(func: FuncT) -> FuncT:
        @functools.wraps(func)
        def wrapped(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            return do_experiment(func, table_name, db, *args, **kwargs)
        return typing.cast(FuncT, wrapped)
    return decorator
