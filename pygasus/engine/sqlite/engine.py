# Copyright (c) 2021, LE GOFF Vincent
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.

# * Neither the name of ytranslate nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""Module containing the Sqlite3 database engine."""

import datetime
from itertools import count
import pathlib
import pickle
import sqlite3
from typing import Any, Dict, Optional, Type, Union

from pygasus.engine.base import BaseEngine
from pygasus.engine.generic.columns import IntegerColumn, OneToOneColumn
from pygasus.engine.generic.columns.base import BaseColumn
from pygasus.engine.generic.table import GenericTable
from pygasus.engine.sqlite.constants import (
        CREATE_MIGRATION_TABLE_QUERY, CREATE_TABLE_QUERY,
        DELETE_QUERY, INSERT_QUERY, SELECT_QUERY, UPDATE_QUERY, SQL_TYPES)
from pygasus.engine.sqlite.operators import QueryWalker
from pygasus.schema.field import Field
from pygasus.schema.transaction import Transaction

class Sqlite3Engine(BaseEngine):

    """
    The sqlite3 dabase engine for Pygasus.

    This engine allows to connect and query a Sqlite3 database (stored
    in a file or in memory).

    """

    def __init__(self, database):
        super().__init__(database)
        self.file_name = None
        self.memory = False
        self.savepoints = {}
        self.savepoint_id = count(1)

    def init(self, file_name: Union[str, pathlib.Path, None] = None,
            memory: bool = False):
        """
        Initialize the database engine.

        Args:
            file_name (str or Path): the file_name in which the database
                    is stored, or will be stored.  It can be a relative
                    or absolute file name.  If you want to just store
                    in memory, don't specify a file name and just set
                    the `memory` argument to `True`.
            memory (bool): whether to store this database in memory or
                    not?  If `True`, the file name is ignored.

        """
        self.file_name = file_name if not memory else None
        self.memory = memory

        # Connect to the database.
        if memory:
            sql_file_name = ":memory:"
        else:
            if isinstance(file_name, str):
                file_name = pathlib.Path(file_name)

            assert isinstance(file_name, pathlib.Path)
            if file_name.absolute():
                sql_file_name = str(file_name)
            else:
                sql_file_name = str(file_name.resolve())
            self.file_name = file_name
        self.connection = sqlite3.connect(sql_file_name)
        self.connection.isolation_level = None
        self.connection.create_function("pylower", 1, str.lower)
        #self.connection.set_trace_callback(print)
        self.cursor = self.connection.cursor()

    def close(self):
        """Close the database."""
        if self.connection:
            self.connection.close()

    def destroy(self):
        """Destroy the database."""
        self.close()
        if self.file_name:
            self.file_name.unlink()

    def create_migration_table(self):
        """
        Create the migration table, if it doesn't exist.

        Notice that this method will be called each time the engine
        is created, so this method should do nothing if the migration
        table already exists.

        """
        self.cursor.execute(CREATE_MIGRATION_TABLE_QUERY)

    def create_table_for(self, table: GenericTable):
        """
        Create a database table for this generic table.

        Notice that this method is called each time the model is
        loaded, therefore this method should do nothing if the table
        already exists.

        Args:
            table (GenericTable): the generic table.

        """
        sql_columns = []
        for column in table.columns.values():
            if isinstance(column, OneToOneColumn):
                sql_column = f"{column.name} INTEGER"
            else:
                sql_type = SQL_TYPES.get(type(column), "BLOB")
                sql_column = f"{column.name} {sql_type}"

            if column.primary_key:
                sql_column += " PRIMARY KEY"
                if isinstance(column, IntegerColumn):
                    # Autoincrement by default on primary key ints.
                    sql_column += " AUTOINCREMENT"

            if not column.has_default:
                sql_column += " NOT NULL"

            sql_columns.append(sql_column)

        # Now browse columsnt o add constraints.
        for column in table.columns.values():
            if isinstance(column, OneToOneColumn):
                refmodel = column.to_model
                refname = refmodel._alt_name or refmodel.__name__.lower()
                pk = column.primary.name
                sql_columns.append(
                        f"FOREIGN KEY({column.name}) "
                        f"REFERENCES {refname}({pk})"
                )


        # Send the create query.
        columns = ", ".join(sql_columns)
        self._execute(CREATE_TABLE_QUERY.format(table_name=table.name,
                columns=columns))

    def get_saved_schema_for(self, table: GenericTable):
        """
        Return the saved schema for this table, if any.

        Returning `None` will lead to the database calling `create_table_for`.
        If migrations are supported for this engine, the schema should
        be returned (the list of columns stored in the last migration).

        Args:
            table (GenericTable): the generic table.

        """
        return None

    def select_rows(self, table, query, filters):
        """
        Return a query object filtered according to the specified arguments.

        Positional arguments should contain query filters, like
        `Person.name == "Vincent"`.  Keyword arguments should contain
        direct matches tested on equality, like `first_name="Vincent`).

        Hence, here are some examples of ways to call this method:

            engine.select_row(Person.first_name == "Vincent")
            engine.select_row(Person.age > 21, Person.name.lower() == "lucy")
            engine.select_row(name="Vincent")

        Returns:
            The list of rows matching the specified queries.

        """
        walker = QueryWalker(query)
        walker.walk()
        where = walker.sql_statement
        sql_values = walker.sql_values
        columns = self._get_sql_columns(table, sep=",")

        # Send the query.
        self._execute(SELECT_QUERY.format(table_name=table.name,
                columns=columns, filters=where, join=""), sql_values)

        # Return the rows.
        rows = self.cursor.fetchall()
        return [self._get_dict_of_values(table, row) for row in rows]

    def get_row(self, table: GenericTable,
            columns: Dict[BaseColumn, Any]) -> Optional[Dict[str, Any]]:
        """
        Get, if possible, a row with the specified columns.

        If more than one row would match the specified columns,
        `None` is returned.  If no match is found, `None` is also returned.
        For greater precision, use `select`.

        Args:
            table (GenericTable): the generic table.
            columns (dict): the column dictionary, containing, as keys,
                    column objects, and as values, whatever value
                    (of whatever type) has been set by the user.

        Returns:
            row (dict or None): the row columns as a dict.

        """
        sql_filters = []
        sql_values = []
        joins = []
        for column, value in columns.items():
            sql_filters.append(f"{column.table.name}.{column.name}=?")
            if column.table is not table:
                joins.append(column)

            sql_values.append(value)
        columns = self._get_sql_columns(table, sep=",")

        # Send the query.
        filters = " AND ".join(sql_filters)
        join = "\n".join([
                f"INNER JOIN {col.table.name} "
                f"ON {col.table.name}.{col.name} = {table.name}.id"
                for col in joins]) if joins else ""
        rows = self._execute(SELECT_QUERY.format(table_name=table.name,
                columns=columns, filters=filters, join=join), sql_values)
        rows = self.cursor.fetchall()
        if len(rows) == 0 or len(rows) < 1:
            return None

        return self._get_dict_of_values(table, rows[0])

    def insert_row(self, table: GenericTable,
            columns: Dict[BaseColumn, Any]) -> Dict[str, Any]:
        """
        Insert a row in the database.

        Args:
            table (GenericTable): the generic table.
            columns (dict): the dictionary of columns.  This should contain
                    column objects as keys and their values (can be
                    a default value).

        """
        sql_columns = []
        sql_values = []
        for column, value in columns.items():
            sql_columns.append(column.name)
            sql_values.append(value)

        # Send the query.
        values = ", ".join(["?"] * len(columns))
        sql_columns = ", ".join(sql_columns)
        self._execute(INSERT_QUERY.format(table_name=table.name,
                columns=sql_columns, values=values), sql_values)

        data = {}
        for column in table.columns.values():
            value = columns.get(column)
            if column.set_by_database:
                value = self.cursor.lastrowid

            data[column.name] = value

        return data

    def update_row(self, table: GenericTable, primary_keys: Dict[str, Any],
            column: BaseColumn, value: Any):
        """
        If possible, update the specified row's field.

        Args:
            table (GenericTable): the generic table.
            primary_keys (dict): the dictionary of primary keys.
            column (BaseColumn): the column to update.
            value (Any): the column's new value.

        This value is supposed to have been filtered and allowed by the
        model layer.

        """
        sql_filters = []
        sql_values = [value]
        for key, value in primary_keys.items():
            sql_filters.append(f"{key}=?")
            sql_values.append(value)

        sql_filters = " AND ".join(sql_filters)
        self._execute(UPDATE_QUERY.format(table_name=table.name,
                filters=sql_filters, column=column.name), sql_values)

    def delete_row(self, table: GenericTable, primary_keys: Dict[str, Any]):
        """
        Delete the specified row from the database.

        Args:
            table (GenericTable): the generic table.
            primary_keys (dict): the dictionary of primary keys.

        """
        sql_filters = []
        sql_values = []
        for key, value in primary_keys.items():
            sql_filters.append(f"{key}=?")
            sql_values.append(value)

        sql_filters = " AND ".join(sql_filters)
        self._execute(DELETE_QUERY.format(table_name=table.name,
                filters=sql_filters), sql_values)

    def begin_transaction(self, transaction: Transaction):
        """
        Begin a transaction.

        Args:
            transaction: the transacrion to begin.

        """
        if transaction.parent:
            t_id = next(self.savepoint_id)
            savepoint = f"sp{t_id}"
            self.savepoints[transaction] = t_id
            self._execute(f"SAVEPOINT {savepoint};")
        else:
            self._execute("BEGIN TRANSACTION;")

    def commit_transaction(self, transaction: Transaction):
        """
        Commit a transaction.

        Args:
            transaction: the transacrion to commit.

        """
        if transaction.parent:
            t_id = self.savepoints.pop(transaction)
            savepoint = f"sp{t_id}"
            self._execute(f"RELEASE SAVEPOINT {savepoint};")
        else:
            self._execute("COMMIT;")

    def rollback_transaction(self, transaction: Transaction):
        """
        Rollback a transaction.

        Args:
            transaction: the transacrion to rollback.

        """
        if transaction.parent:
            t_id = self.savepoints.pop(transaction)
            savepoint = f"sp{t_id}"
            self._execute(f"ROLLBACK TRANSACTION TO SAVEPOINT {savepoint};")
        else:
            self._execute("ROLLBACK;")

    def _execute(self, query, fields=None):
        """Execute a query."""
        fields = fields if fields is not None else ()
        try:
            result = self.cursor.execute(query, fields)
        except Exception as err:
            raise RuntimeError(f"{err}: {query}, {fields}")

        return result

    def _get_sql_columns(self, table: GenericTable, sep=" ") -> str:
        """
        Return the SQL statement containing column names.

        Args:
            sep (str): the separator to place between columns.

        """
        names = tuple(f"{col.table.name}.{col.name}"
                for col in table.columns.values())
        return sep.join(names)

    def _get_dict_of_values(self, table: GenericTable, row: tuple) -> dict:
        """Get and return the dictionary of values for this table."""
        columns = table.columns.keys()
        attrs = {}
        for column, value in zip(columns, row):
            attrs[column] = value

        return attrs
