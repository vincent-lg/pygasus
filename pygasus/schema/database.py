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

"""Class describing a database, working with a database engine."""

from typing import Any, Dict, Optional, Sequence, Type, Union

from pygasus.engine.base import BaseEngine
from pygasus.engine.generic.columns.base import BaseColumn
from pygasus.engine.generic.table import GenericTable
from pygasus.engine.sqlite import Sqlite3Engine
from pygasus.query.query import Query
from pygasus.schema.field import Field
from pygasus.schema.mapper import IDMapper
from pygasus.schema.model import Model, MODELS
from pygasus.schema.transaction import Transaction

class Database:

    """
    A Pygasus database, linking the ORM with an engine.

    This is the main class for all databases.  It is linked to a database
    engine (sqlite by default) and will delegate to this engine most
    of the tasks it should perform.  However, this class also provides
    the true meat of the Object-Relational Mapping (ROM).

    It is possible to ovverride this class to change most aspects of the
    library, optimize some details, alter some behavior.

    """

    def __init__(self):
        self._engine = Sqlite3Engine(self)
        self._models = {}
        self._current_transaction = None
        self.id_mapper = IDMapper(self)
        Query._database = self
        Query._engine = self._engine
        Query._id_mapper = self.id_mapper

    @property
    def engine(self):
        """Return the database engine currently being used."""
        return self._engine
    @engine.setter
    def engine(self, engine: Union[BaseEngine, Type[BaseEngine]]):
        """
        Change the engine in use.

        Args:
            engine (Engine or subclass of Engine): the new database engine.
                    It can be instantiated or not.  It is preferrable
                    to let this property handles instantiation.

        """
        if issubclass(engine, BaseEngine):
            engine = engine(self)
        else:
            engine.database = self

        self._engine = engine
        Query._engine = engine

    @property
    def transaction(self):
        """Create and return a transaction."""
        return Transaction(self, parent=self._current_transaction)

    def bind(self, models: Optional[Sequence[Model]] = None):
        """
        Bind this database to the specified, or the detected, models.

        If this method is called with no argument, all imported classes
        inherited from models are bound with this database.  You can
        also spedcify a list (or a sequence of any type) of models to bind.

        Args:
            models (optional, sequence of Model): the models to bind.

        """
        models = MODELS if models is None else models
        self._models = {cls.__name__: cls for cls in models}
        names = {cls.__name__: cls for cls in models}

        # Check that all models equire no external bound models.
        for cls in models:
            cls._fields = cls.get_fields(cls, names)
            cls.load_schema()
            cls._database = self
            cls._engine = self._engine

        # Complete fields.
        for cls in models:
            cls.complete_fields(cls)

        # Generate generic tables for models.
        for model in models:
            table = GenericTable.create_from_model(model, self)
            model._generic = table


    def init(self, *args, **kwargs):
        """
        Initialize (create if necessary) the database.

        Positional or keyword arguments are sent to the database engine.

        """
        self._engine.init(*args, **kwargs)
        self._engine.create_migration_table()

        # Check the model schemas.
        for cls in self._models.values():
            saved_schema = self._engine.get_saved_schema_for(cls)
            if saved_schema is None:
                self._engine.create_table_for(cls._generic)
            else:
                diff = self.create_diff_between(saved_schema, cls._schema)
                if diff is not None:
                    self._engine.apply_diff_for(cls, diff)

    def close(self):
        """Close the database."""
        return self._engine.close()

    def destroy(self):
        """Destroy the database."""
        self._engine.destroy()

    # Interactions between models and generic tables:
    def create_instance(self, model: Type[Model],
            fields: Dict[Field, Any]) -> Model:
        """
        Create an instance of a model.

        This method connects the model layer to the database engine layer.  It accepts parameters from a model, contacts the database engine and handles generation of an instance.  Instance creation can also be intercepted by the ID mapper if the instance already exists in the cache.

        Args:
            model (subclass of Model): the model class.
            fields (dict): the fields to create, as a dictionary,
                    containing `{field object: value}`.

        Returns:
            instance (Model): the model instance.

        """
        table = model._generic
        columns = table.prepare_columns(dict(fields))
        data = self._engine.insert_row(table, columns)

        # Normalizes data.
        for key, value in tuple(data.items()):
            if key not in model._fields:
                data.pop(key)

        for field, value in fields.items():
            data[field.name] = value

        instance = model(**data)
        if self.id_mapper:
            self.id_mapper.set(model, instance._primary_values, instance)

        return instance

    def get_instance(self, model: Type[Model],
            fields: Dict[Field, Any]) -> Optional[Model]:
        """
        Fetch a model from the database.

        This method connects the model layer to the database engine layer.
        It accepts parameters from a model, contacts the database engine
        and handles generation of an instance.  Instance creation can
        also be intercepted by the ID mapper if the instance already
        exists in the cache.

        Args:
            model (subclass of Model): the model class.
            fields (dict): the fields to query as a dictionary,
                    containing `{field object: value}`.

        Returns:
            instance (Model): the model instance or None.

        """
        table = model._generic
        columns = table.prepare_columns(fields, search_outside=True)
        data = self._engine.get_row(table, columns)
        if data is None:
            return None

        if self.id_mapper:
            primary = model._primary_values_from_dict(data)
            instance = self.id_mapper.get(model, primary)
            if instance is not None:
                return instance

        instance = model(**data)
        if self.id_mapper:
            self.id_mapper.set(model, primary, instance)

        return instance

    def select(self, model: Model, query: Query, filters):
        """
        Select one or more results from the database.

        Args:
            model (subclass of Model): the model class.
            query (Query): the query to select from.
            filters (dict): queries on fields.

        Returns:
            combied (Query): the combined query to be executed.

        """
        query.model = model
        return query

    def update_instance(self, instance: Model, field: Field, value: Any,
            propagate: bool = True):
        """
        Update the value of a given field.

        Args:
            instance (Model): the model instance.
            field (Field): the field to update.
            value (Any): the new field's value.
            propagate (bool): If True (the default), modify
                    the instance's field in memory.

        """
        transaction = self._current_transaction
        if transaction:
            if instance not in transaction.objects:
                attrs = {field.name: getattr(instance, field.name)
                        for field in instance._fields.values()}
                transaction.objects[instance] = attrs
        table = type(instance)._generic
        columns = table.prepare_columns({field: value})
        primary = {field.name: getattr(instance, field.name)
                for field in type(instance)._fields.values()
                if field.primary_key}
        for column, col_value in columns.items():
            self._engine.update_row(table, primary, column, col_value)
        if propagate:
            instance._has_init = False
            setattr(instance, field.name, value)
            instance._has_init = True

    def delete_instance(self, instance: Model):
        """
        Delete the specified instance.

        Args:
            instance (Model): the model instance to delete.

        """
        transaction = self._current_transaction
        if transaction:
            if instance not in transaction.objects:
                attrs = {field.name: getattr(instance, field.name)
                        for field in instance._fields.values()}
                transaction.objects[instance] = attrs
        table = type(instance)._generic
        primary = {field.name: getattr(instance, field.name)
                for field in type(instance)._fields.values()
                if field.primary_key}
        self._engine.delete_row(table, primary)
        instance._has_init = False

    def _get_columns(self, table: GenericTable,
            fields: Dict[Field, Any]) -> Dict[BaseColumn, Any]:
        """Return the column and their values."""
        columns = {}
        for key, value in fields.items():
            column = table.columns.get(key)
            if column is None:
                continue

            columns[column] = value

        return columns
