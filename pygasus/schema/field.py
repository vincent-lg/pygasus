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

"""Module containing the field class, defining database columns."""

from typing import Any

from pygasus.query.operation import Unary
from pygasus.query.query import Query

_NOT_SET = object()

class Field(Query):

    """A field, to represent a database column."""

    def __init__(self, field_type, primary_key=False,
            name=None, default=_NOT_SET):
        super().__init__(Unary.RETRIEVE)
        self.field_type = field_type
        self.primary_key = primary_key
        self.name = name
        self.default = default
        self.model = None
        self.store_sequence = False
        self.mirror = None
        self.memory = {}

    def __get__(self, instance, owner=None):
        if instance is None:
            return self

        # The value might be cached in `memory`
        identifier = hash(instance)
        value = self.memory.get(identifier, _NOT_SET)
        if value is _NOT_SET:
            value = None
            self.memory[identifier] = value

        return value

    def __set__(self, instance, value):
        identifier = hash(instance)
        if self.mirror:
            old_value = self.mirror.memory.get(hash(value))

        self.memory[identifier] = value

        if self.mirror:
            if old_value:
                self.memory[hash(old_value)] = None

            if value:
                self.mirror.memory[hash(value)] = instance

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"<Field {self.name!r}>"

    def __str__(self):
        text = f"{self.name!r} of type {self.field_type.__name__}"
        info = []
        if self.primary_key:
            info.append("primary_key")

        if self.has_default:
            info.append(f"default={self.default!r}")

        if self.store_sequence:
            info.append("store_sequence")

        if self.mirror:
            info.append(f"mirror={self.mirror.name} "
                    f"on {self.mirror.model.__name__}")

        return f"{text} ({', '.join(info)})"

    @property
    def set_by_database(self):
        """This field is to be set by the database only."""
        return self.field_type is int and self.primary_key

    @property
    def has_default(self):
        """Has this field got a default value?"""
        return self.default is not _NOT_SET

    def accept(self, value: Any) -> bool:
        """Return whether this value is accepted."""
        accepted = self.field_type
        if accepted in (int, float, str, bytes):
            return isinstance(value, accepted)

        return True
