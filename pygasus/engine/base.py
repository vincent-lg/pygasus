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

"""Module containing the base class for a database engine."""

from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Optional, Type

from pygasus.engine.generic.columns.base import BaseColumn
from pygasus.engine.generic.table import GenericTable
from pygasus.schema.transaction import Transaction

class BaseEngine(metaclass=ABCMeta):

    """
    Base class for a database engine.

    Adding a database engine can be done by inheriting from `BaseEngine`
    and setting the new engine in the database.

    """

    def __init__(self, database):
        self.database = database

    @abstractmethod
    def init(self, *args, **kwargs):
        """
        Initialize the database engine.

        Override this method in sub-classes.

        Optional and keyword arguments are supported.

        """

    @abstractmethod
    def close(self):
        """Close the database."""

    @abstractmethod
    def destroy(self):
        """Destroy the database."""

    @abstractmethod
    def create_migration_table(self):
        """
        Create the migration table, if it doesn't exist.

        Notice that this method will be called each time the engine
        is created, so this method should do nothing if the migration
        table already exists.

        """

    @abstractmethod
    def create_table_for(self, table: GenericTable):
        """
        Create a database table for this Generic table.

        Notice that this method is called each time the model is
        loaded, therefore this method should do nothing if the table
        already exists.

        Args:
            table (GenericTable): the generic table.

        """

    @abstractmethod
    def get_saved_schema_for(self, table: GenericTable):
        """
        Return the saved schema for this table, if any.

        Returning `None` will lead to the database calling `create_table_for`.
        If migrations are supported for this engine, the schema should
        be returned (the list of columns stored in the last migration).

        Args:
            table (GenericTable): the generic table.

        """

    @abstractmethod
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

    @abstractmethod
    def select_rows(self, query, filters):
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

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
    def delete_row(self, table: GenericTable, primary_keys: Dict[str, Any]):
        """
        Delete the specified row from the database.

        Args:
            table (GenericTable): the generic table.
            primary_keys (dict): the dictionary of primary keys.

        """

    @abstractmethod
    def begin_transaction(self, transaction: Transaction):
        """
        Begin a transaction.

        Args:
            transaction: the transacrion to begin.

        """

    @abstractmethod
    def commit_transaction(self, transaction: Transaction):
        """
        Commit a transaction.

        Args:
            transaction: the transacrion to commit.

        """

    @abstractmethod
    def rollback_transaction(self, transaction: Transaction):
        """
        Rollback a transaction.

        Args:
            transaction: the transacrion to rollback.

        """
