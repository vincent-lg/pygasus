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

"""Module containing the SQLAlchemy-compatible database engine."""

import datetime
from itertools import count
import pathlib
import pickle
from typing import Any, Dict, Optional, Type, Union

from pygasus.engine.base import BaseEngine
from pygasus.engine.generic.columns import IntegerColumn, OneToOneColumn
from pygasus.engine.generic.columns.base import BaseColumn
from pygasus.engine.generic.table import GenericTable
from pygasus.engine.sqlalchemy.query import QueryWalker
from pygasus.schema.field import Field
from pygasus.schema.transaction import Transaction

# Try to import SQLAlchemy.
try:
    from sqlalchemy import (
            create_engine, event, Column, ForeignKey, MetaData, Table
    )
    from sqlalchemy.sql import select, text
except ModuleNotFoundError:
    raise ModuleNotFoundError("SQLAlchemy is not installed")

from pygasus.engine.sqlalchemy.constants import SQL_TYPES

class SQLAlchemyEngine(BaseEngine):

    """
    The SQLAlchemy dabase engine for Pygasus.

    This engine allows to connect and query a database compatible with
    SQLAlchemy.  SQLAlchemy is used with the SQL Expression Language,
    not the ORM itself, this task is fulfilled by Pygasus.

    """

    def __init__(self, database):
        super().__init__(database)
        self.file_name = None
        self.memory = False
        self.savepoints = {}
        self.savepoint_id = count(1)
        self.transaction = None
        self.printout = False

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
        self.engine = create_engine(f"sqlite:///{sql_file_name}")

        @event.listens_for(self.engine, "connect")
        def setup_lower(dbapi_connection, conn_rec):
            dbapi_connection.create_function("pylower", 1, str.lower)

        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.tables = {}

    def close(self):
        """Close the database."""

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
                sql_column = Column(column.name, None, ForeignKey(
                        f"{column.to_model.__name__.lower()}.id"))
            else:
                sql_type = SQL_TYPES[type(column)]
                sql_column = Column(column.name, sql_type,
                        primary_key=column.primary_key,
                        nullable=column.has_default)

            sql_columns.append(sql_column)

        # Create the table object.
        self.tables[table.name] = Table(table.name, self.metadata,
                *sql_columns)

    def run_after_table_creation(self):
        """When all the tables have been created."""
        self.metadata.create_all(self.engine)

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

        The query object will contain the filters themselves, as a
        tree of conditions.  Additional filters can also be specified
        as keyword arguments.

        Args:
            query (Query): the query object, containing the filters.

        More keyword arguments can be sent as additional filters.

        Note:
            This method EXECUTES the query object and sends it to
            the database, effectively querying the results.

        Returns:
            rows (list): The list of rows matching the specified query.

        """
        sql_table = self.tables[table.name]
        walker = QueryWalker(self, query)
        where = walker.walk()
        query = select(sql_table)

        for table in walker.tables:
            if table is not sql_table:
                query = query.select_from(sql_table.join(table))

        query = query.where(where)


        # Send the query.
        rows = self.connection.execute(query).fetchall()
        if len(rows) == 0 or len(rows) < 1:
            return None

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
        sql_table = self.tables[table.name]
        query = select(sql_table)
        where = []
        tables = set()
        for column, value in columns.items():
            matching_table = self.tables[column.table.name]
            where.append(getattr(matching_table.c, column.name) == value)
            tables.add(matching_table)

        query = query.where(*where)

        # Build required joins if necessary.
        for other_table in tables:
            if other_table is not sql_table:
                query = query.select_from(sql_table.join(other_table))

        # Send the query.
        rows = self.connection.execute(query).fetchall()
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
        sql_table = self.tables[table.name]
        sql_columns = {}
        for column, value in columns.items():
            sql_columns[column.name] = value

        # Send the query.
        insert = sql_table.insert().values(**sql_columns)
        result = self.connection.execute(insert)

        data = {}
        primary_keys = iter(result.inserted_primary_key)
        for column in table.columns.values():
            value = columns.get(column)
            if column.set_by_database:
                value = next(primary_keys)

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
        sql_table = self.tables[table.name]
        sql_primary_keys = []
        for primary, p_value in primary_keys.items():
            sql_primary_keys.append(getattr(sql_table.c,
                    primary) == p_value)

        # Send the query.
        sql_columns = {column.name: value}
        update = sql_table.update().where(*sql_primary_keys).values(
                **sql_columns)
        self.connection.execute(update)

    def delete_row(self, table: GenericTable, primary_keys: Dict[str, Any]):
        """
        Delete the specified row from the database.

        Args:
            table (GenericTable): the generic table.
            primary_keys (dict): the dictionary of primary keys.

        """
        sql_table = self.tables[table.name]
        sql_primary_keys = []
        for primary, p_value in primary_keys.items():
            sql_primary_keys.append(getattr(sql_table.c,
                    primary) == p_value)

        # Send the query.
        delete = sql_table.delete().where(*sql_primary_keys)
        self.connection.execute(delete)

    def begin_transaction(self, transaction: Transaction):
        """
        Begin a transaction.

        Args:
            transaction: the transacrion to begin.

        Note:
            SQLAlchemy implements save points and transactions as
            different concepts.  Pygasus tries to unify both concepts
            as also does Sqlite: an inner transaction is linked to
            a savepoint and can be rolled back.  The outer transaction,
            however, is handled by SQLAlchemy.  To handle inner
            transactions, Pygasus has to send raw SQL to SQLAlchemy.

        """
        if transaction.parent: # This is an inner transaction.
            t_id = next(self.savepoint_id)
            savepoint = f"sp{t_id}"
            self.savepoints[transaction] = t_id
            self.connection.execute(text(f"SAVEPOINT {savepoint};"))
        else: # This is an outer transaction.
            self.transaction = self.connection.begin()

    def commit_transaction(self, transaction: Transaction):
        """
        Commit a transaction.

        Args:
            transaction: the transacrion to commit.

        Note:
            SQLAlchemy implements save points and transactions as
            different concepts.  Pygasus tries to unify both concepts
            as also does Sqlite: an inner transaction is linked to
            a savepoint and can be rolled back.  The outer transaction,
            however, is handled by SQLAlchemy.  To handle inner
            transactions, Pygasus has to send raw SQL to SQLAlchemy.

        """
        if transaction.parent: # This is an inner transaction.
            t_id = self.savepoints.pop(transaction)
            savepoint = f"sp{t_id}"
            self.connection.execute(text(f"RELEASE SAVEPOINT {savepoint};"))
        else: # This is an outer transaction.
            self.transaction.commit()
            self.transaction.close()
            self.transaction = None

    def rollback_transaction(self, transaction: Transaction):
        """
        Rollback a transaction.

        Args:
            transaction: the transacrion to rollback.

        Note:
            SQLAlchemy implements save points and transactions as
            different concepts.  Pygasus tries to unify both concepts
            as also does Sqlite: an inner transaction is linked to
            a savepoint and can be rolled back.  The outer transaction,
            however, is handled by SQLAlchemy.  To handle inner
            transactions, Pygasus has to send raw SQL to SQLAlchemy.

        """
        if transaction.parent: # This is an inner transaction.
            t_id = self.savepoints.pop(transaction)
            savepoint = f"sp{t_id}"
            self.connection.execute(text(f"ROLLBACK TRANSACTION TO SAVEPOINT {savepoint};"))
        else: # This is an outer transaction.
            self.transaction.rollback()
            self.transaction = None

    def _get_dict_of_values(self, table: GenericTable, row: tuple) -> dict:
        """Get and return the dictionary of values for this table."""
        columns = table.columns.keys()
        attrs = {}
        for column, value in zip(columns, row):
            attrs[column] = value

        return attrs
