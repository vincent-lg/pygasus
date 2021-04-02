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
from textwrap import dedent
from typing import Any, Dict, Optional, Type, Union

from pygasus.engine.base import BaseEngine
from pygasus.schema.field import Field
from pygasus.schema.transaction import Transaction

Model = 'pygasus.schema.model.Model'

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
        self.cursor.execute(CREATE_MIGRATION_TABLE)

    def create_table_for(self, model: Type[Model]):
        """
        Create a table for this model.

        Notice that this method is called each time the model is
        bound, therefore this method should do nothing if the table
        already exists.

        Args:
            model (subclass of Model): the model.

        """
        table_name = model._alt_name or model.__name__.lower()
        sql_fields = []
        for field in model._fields.values():
            sql_type = SQL_TYPES.get(field.field_type, "BLOB")
            sql_field = f"{field.name} {sql_type}"
            if field.primary_key:
                sql_field += " PRIMARY KEY"
                if field.field_type is int:
                    # Autoincrement by default on primary key ints.
                    sql_field += " AUTOINCREMENT"

            if not field.has_default:
                sql_field += " NOT NULL"

            sql_fields.append(sql_field)

        # Send the create query.
        fields = ", ".join(sql_fields)
        self.cursor.execute(CREATE_TABLE.format(table_name=table_name,
                fields=fields))

    def get_saved_schema_for(self, model: Type[Model]):
        """
        Return the saved schema for this model, if any.

        Returning `None` will lead to the database calling `create_table_for`.
        If migrations are supported for this engine, the schema should
        be returned (the list of fields stored in the last migration).

        Args:
            model (subclass of Model): the model.

        """
        return None

    def get_instance(self, model: Type[Model],
            fields: Dict[Field, Any]) -> Optional[Model]:
        """
        Get, if possible, an instance with the specified fields.

        If more than one instance would match the specified fields,
        `None` is returned.  If no match is found, `None` is also returned.
        For greater precision, use `select`.

        Args:
            model (subclass of Model): the model class.
            fields (dict): the field dictionary, containing, as keys,
                    field objects, ans as values, whatever value
                    (of whatever type) has been set by the user.

        Returns:
            instance (Model or None): the instance matching these fields.

        """
        table_name = model._alt_name or model.__name__.lower()
        sql_fields = []
        sql_values = []
        for field, value in fields.items():
            sql_field = f"{field.name}=?"
            sql_fields.append(sql_field)
            sql_values.append(value)

        # Determine the field of the queries.
        fields = []
        for field in model._fields.values():
            fields.append(field.name)
        fields = ", ".join(fields)

        # Send the query.
        filters = " AND ".join(sql_fields)
        rows = self._execute(SELECT_QUERY.format(table_name=table_name,
                fields=fields, filters=filters), sql_values)

        # Loop over the rows.
        rows = self.cursor.fetchall()
        if len(rows) == 0 or len(rows) < 1:
            return None

        row = rows[0]
        instance_data = {}
        primary = {}
        for i, field in enumerate(model._fields.values()):
            value = row[i]

            # Convert this SQL value to Python if necessary.
            if isinstance(value, bytes) and field.field_type is not bytes:
                # Unpickle the data.
                value = pickle.loads(value)

            instance_data[field.name] = value

            if field.primary_key:
                primary[field.name] = value

        # If there's an ID mapper, ask it to retrieve the object.
        mapper = self.database.id_mapper
        if mapper:
            obj = mapper.get(model, primary)
            if obj is not None:
                return obj

        # Create a model instance.
        instance = model(**instance_data)

        if mapper:
            mapper.set(model, primary, instance)

        return instance

    def create_instance(self, model: Type[Model], fields: Dict[Field, Any]):
        """
        Create and update a model's instance fields.

        Args:
            instance (Model): the model to be populated.
            fields (dict): the dictionary or fields.  This should contain
                    field objects as keys and their values (can be
                    a default value).

        """
        table_name = model._alt_name or model.__name__.lower()
        sql_fields = []
        sql_values = []
        for field, value in fields.items():
            sql_fields.append(field.name)
            sql_values.append(value)

        # Send the query.
        values = ", ".join(["?"] * len(fields))
        sql_fields = ", ".join(sql_fields)
        self._execute(INSERT_QUERY.format(table_name=table_name,
                fields=sql_fields, values=values), sql_values)

        instance_data = {}
        primary = {}
        for field in model._fields.values():
            value = fields.get(field)
            if field.set_by_database:
                value = self.cursor.lastrowid

            instance_data[field.name] = value

            if field.primary_key:
                primary[field] = value

        # Create an instance object.
        instance = model(**instance_data)

        # If there's an ID mapper, ask it to retrieve the object.
        mapper = self.database.id_mapper
        if mapper:
            mapper.set(model, primary, instance)

        return instance

    def update_instance(self, instance: Model, field: Field, value: Any):
        """
        If possible, update the specific instance's field.

        Args:
            instance (Model): the model to modify.
            field (Field): the field to be applied.
            value (Any): the field's new value.

        This value is supposed to have been filtered and allowed by the
        instance.

        """
        # Get the primary key field.
        model = type(instance)
        table_name = model._alt_name or model.__name__.lower()
        primary = instance._schema.primary_key
        id_value = getattr(instance, primary.name)
        sql_values = (value, id_value)
        self._execute(UPDATE_QUERY.format(table_name=table_name,
                primary=primary.name, field=field.name), sql_values)
        instance._has_init = False
        setattr(instance, field.name, value)
        instance._has_init = True

    def delete_instance(self, instance: Model):
        """
        Delete the specified instance from the database.

        Args:
            instance (Model): the model instance to be deleted.

        """
        # Get the primary key field.
        model = type(instance)
        table_name = model._alt_name or model.__name__.lower()
        primary = instance._schema.primary_key
        id_value = getattr(instance, primary.name)
        self._execute(DELETE_QUERY.format(table_name=table_name,
                primary=primary.name), (id_value, ))
        instance._is_deleted = True

        # If there's an ID mapper, remove the object.
        mapper = self.database.id_mapper
        if mapper:
            fields = instance._schema.get_fields(instance)
            primary = {field: value
                    for field, value in fields.items()
                    if field.primary_key}
            mapper.delete(model, primary)

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
        return self.cursor.execute(query, fields)


## Constants
SQL_TYPES = {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
        bytes: "BLOB",
        datetime.datetime: "TIMESTAMP",
        datetime.date: "DATE",
}

# Database queries
CREATE_MIGRATION_TABLE = dedent("""
    CREATE TABLE IF NOT EXISTS pygasus_migration (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT UNIQUE NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        schema BLOB NOT NULL
    );
""".strip("\n"))

CREATE_TABLE = dedent("""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {fields}
    );
""".strip("\n"))

SELECT_QUERY = dedent("""
    SELECT {fields} FROM {table_name}
    WHERE {filters};
""".strip("\n"))

INSERT_QUERY = dedent("""
    INSERT INTO {table_name} ({fields})
    VALUES ({values});
""".strip("\n"))

UPDATE_QUERY = dedent("""
    UPDATE {table_name}
    SET {field}=?
    WHERE {primary}=?
""".strip("\n"))

DELETE_QUERY = dedent("""
    DELETE FROM {table_name}
    WHERE {primary}=?
""".strip("\n"))
