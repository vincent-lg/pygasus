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

"""Module containing the SQLAlchemy query walker."""

from operator import eq

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.sql.operators import contains

from pygasus.query.operation import Binary, Function, Unary
from pygasus.schema.field import Field

OPERATIONS = {
    # Binary operators
    Binary.EQUAL: eq,

    # Unary operators
    Unary.RETRIEVE: ...,

    # Functions
    Function.CONTAINS: func.INSTR,
    Function.LOWER: func.PYLOWER,
}

class QueryWalker:

    """Query walker, to walk through operators."""

    def __init__(self, engine, query):
        self.engine = engine
        self.query = query
        self.tables = set()

    def walk(self):
        """Walk through the query."""
        return self.decode(self.query)

    def decode(self, query):
        """Decode and recursively convert to SQL a query."""
        operation = getattr(query, "operation", None)
        if operation:
            sql_operation = OPERATIONS[operation]

        args = []
        if isinstance(operation, (Binary, Function)):
            first = query.arguments[0]
            args.append(self.decode(first))

            for argument in query.arguments[1:]:
                args.append(self.decode(argument))
            return sql_operation(*args)
        elif isinstance(operation, Unary):
            if operation is Unary.RETRIEVE:
                field = query.arguments[0]
                column = field.column
                sql_table = self.engine.tables[column.table.name]
                self.tables.add(sql_table)
                sql_column = getattr(sql_table.c, column.name)
                return sql_column
        else:
            primary = getattr(query, "_primary_values", (query, ))[0]
            return primary
