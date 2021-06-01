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

"""Module containing the Query class, to handle database queries."""

from pygasus.query.operation import Binary, Function, Unary

class Query:

    """
    Class representing a query.

    The query syntax is meant to remain close (but not equivalent) to Python, using standard operators whenever possible  or methods whenever appropriate.  Queries can easily be chained as well.

        print(Person.first_name == "John"))

    ... or:

        print(Person.first_name.lower() == "john"))

    ... or:

        print(Person.select)Person.age <= 25))

    A field is a query, so using `Person.name` is already a query in itself that `Person.select` will accept.  However, queries can be nested, which makes them much more interesting.

    Supported operators:

        ==: quality, no matter the field type.

    Supported methods:

        contains: on string fields, check whether a substring is in a string.
        lower(): on string fields, return the lowercase string.

    """

    _database = None
    _engine = None
    _id_mapper = None

    def __init__(self, operation, arguments=None):
        self.operation = operation
        self.arguments = arguments or (self, )
        self.model = None
        self.results = None

    def __eq__(self, other):
        return Query(Binary.EQUAL, arguments=(self, other))

    def __iter__(self):
        results = self.execute()
        return iter(results)

    def execute(self):
        """
        Execute the query.

        This method will send the query to the database engine and
        will return the result.

        """
        if self.results is not None:
            return self.results

        results = type(self)._engine.select_rows(self.model._generic, self, {})

        # Add or get from IDMapper.
        id_mapper = type(self)._database.id_mapper
        for i, data in enumerate(results):
            primary = self.model._primary_values_from_dict(data)
            if id_mapper:
                instance = id_mapper.get(self.model, primary)
                if instance is not None:
                    results[i] = instance
                    continue

            instance = self.model(**data)
            if id_mapper:
                id_mapper.set(self.model, primary, instance)
            results[i] = instance

        self.results = results
        return results

    def contains(self, other):
        """
        Check whether a substring is contained in this parent string.

        Args:
            substring (str): the substring to test.

        Usually, this function is called on a field:

            Person.name.contains("ab")

        ... which will only return True if the field contains "ab".
        Of course, you can also combine it:

            Person.name.lower().contains("ab")

        ... will do the same, but the case will be ignored.

        """
        return Query(Function.CONTAINS, arguments=(self, other))

    def lower(self):
        """
        Return the lowercase string.

        Args:
            None.

        This method, called on a field, will return the lowercase version
        of this field.  Accented letters (NON-ASCII letters) will
        be lowercased if possible (`str.lower` is used, not the default
        `LOWER` function provided in Sqlite).  Of course, keep in mind
        such an operation can be costly, if you have 1 million rows
        in a table and ask to lowercase all text entries to select,
        but this method has been optimized.

            Person.select(Person.name.lower() == "john")

        """
        return Query(Function.LOWER, arguments=(self, ))

    def prefetch(self, field):
        """Do a prefetch."""
        query = Query(Unary.PREFETCH, arguments=(self, field))
        query.model = field.model
        return query
