﻿# Copyright (c) 2021, LE GOFF Vincent
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

"""Module containing the Table class."""

from collections import OrderedDict
import datetime
from typing import Any, Dict, Type

from pygasus.engine.generic.columns import (
        BlobColumn, DateColumn, IntegerColumn, OneToOneColumn,
        RealColumn, TimestampColumn, TextColumn)
from pygasus.engine.generic.columns.base import BaseColumn
from pygasus.schema.model import Model

COL_TYPES = {
        bytes: BlobColumn,
        datetime.date: DateColumn,
        datetime.datetime: TimestampColumn,
        int: IntegerColumn,
        float: RealColumn,
        str: TextColumn,
}

class GenericTable:

    """
    A generic table, to represent a model class.

    Model classes generate generic tables, with generic columns.
    It is easier for a database engine to deal with these tables
    and columns (they're usually closer to the database) and it allows
    a greater level of abstraction from the model level.

    """

    def __init__(self, model: Type[Model]):
        self.model = model
        self.name = model._alt_name or model.__name__.lower()
        self.columns = OrderedDict()
        self.values = {}

    def generate_column_from_field(self, field, database):
        """
        Generate a column object from a field, and add it to this table.

        Args:
            field (Field): the model field.

        """
        columns = []
        if issubclass(field.field_type, Model):
            # Try to find the counterpart.
            opposed = field.field_type

            # Browse the opposite model for a link to the current model.
            for opposite_field in opposed._schema.fields.values():
                if opposite_field.field_type is self.model:
                    break
            else:
                raise ValueError(
                        f"model {field.model.__name__}.{field.name}: "
                        f"cannot find an opposed field in model "
                        f"{opposed.__name__}, have you forgotten a HasOne "
                        "or HasMany?"
                )

            # Now determines the type of relationship.
            back = opposite_field
            if back.field_type is self.model and back.has_default: # One-to-one
                for pk in field.field_type._schema.primary_keys:
                    columns.append(OneToOneColumn(self, self.model,
                            field.field_type, field, back, pk))
        else:
            col_type = COL_TYPES.get(field.field_type, BlobColumn)
            columns.append(col_type(field, self))

        for column in columns:
            self.columns[column.name] = column

    def prepare_columns(self,
            fields: Dict['pygasus.schema.field.Field', Any],
            search_outside: bool = False) -> Dict[BaseColumn, Any]:
        """
        Return the column and their values.

        This method allows to customize columns that are not stored simply
        according to what field holds (relations).

        Args:
            fields (dict): the dictionary of field and data.
            search_outside (bool): if set to True, search fields in
                    other models.

        Returns:
            columsn (dict): the columns to store.

        """
        columns = {}
        for field, value in tuple(fields.items()):
            key = field.name
            column = self.columns.get(key)
            if column is None:
                continue

            columns[column] = value
            fields.pop(key)

        # Ask the remaining columns if they want to do something.
        for column in self.columns.values():
            columns.update(column.retrieve_additional_columns(fields))

        if search_outside and fields:
            for field, value in fields.items():
                if field.mirror:
                    columns.update(field.mirror.model._generic.prepare_columns({field.mirror: value}, search_outside=False))


        return columns

    @classmethod
    def create_from_model(cls, model, database):
        """
        Create a generic table from a model class.

        Args:
            model (subclass of Model): the model class.
            database (Database): the database.

        Returns:
            generic (GenericTable): the new generic table.

        """
        generic = cls(model)
        for field in model._schema.fields.values():
            generic.generate_column_from_field(field, database)

        model._table = generic
        return generic
