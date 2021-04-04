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
from pygasus.engine.generic.columns import (
        BlobColumn, DateColumn, IntegerColumn,
        RealColumn, TimestampColumn, TextColumn)

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

    def __init__(self, model):
        self.name = model._alt_name or model.__name__.lower()
        self.columns = OrderedDict()
        self.values = {}

    def generate_column_from_field(self, field):
        """
        Generate a column object from a field, and add it to this table.

        Args:
            field (Field): the model field.

        """
        col_type = COL_TYPES.get(field.field_type, BlobColumn)
        column = col_type(field, self)
        self.columns[column.name] = column

    @classmethod
    def create_from_model(cls, model):
        """
        Create a generic table from a model class.

        Args:
            model (subclass of Model): the model class.

        Returns:
            generic (GenericTable): the new generic table.

        """
        generic = cls(model)
        for field in model._fields.values():
            generic.generate_column_from_field(field)

        return generic
