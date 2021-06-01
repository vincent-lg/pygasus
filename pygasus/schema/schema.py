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

"""Module defining the model's schema."""

from typing import Any, Dict, Optional, Sequence, Union

from pygasus.exceptions import ForbiddenArgument, MissingArgument

_NOT_SET = object()

Field = 'pygasus.schema.field.Field'
Model = 'pygasus.schema.model.Model'

class ModelSchema:

    """
    Class to represent the model's current (and past) schema.

    This class contains references to model fields (one field being then
    translated into a generic column, a SQL column).  Schemas can also be
    partial, linked with values (bound) or not.

    Full schema:
        At its most common, a schema is a set of fields representing
        a model.  A model has one schema, the current schema, extracted from
        the model's class attributes.  This schema, called the model
        schema, is a "full schema", because it contains every possible
        information to form a model.  However, by default, it is an "unbound
        schema", (no field is assigned a value).

    Bound and unbound schemas:
        A bound schema is one where every field in the schema has a
        value.  A bound schema is created from an unbound schema.
        Bounded schemas can be full schemas (with every information
        needed to create a model) or partial schemas (with only one, or
        some fields of a model).

    Example:
        To better illustrate this situation, here's a short example:

        ```python
        class Car(Model):

            '''A car.'''

            name: str # this will be a field of type str.
            make: str # This will be another field of the same type.
        ```

        When this model is added in a database, an unbound schema is created
        containing two fields and a direct reference to the `Car` class.
        This schema would also contain additional information if
        specified, like default values.

        When creating a car (`Car.create(name=""...", make="...")`),
        the unbound schema will be matched against the specified values
        and a bound schema will be sent, with every field having now a value,
        to the database.  This bound schema will not live for very long.
        Notice that, though it is now a  bound schema, it is still
        a full schema (all information have been set).  Even if some
        information was missing from the call to `Car.create`, it
        could be supplied by default values and still constitute a bound,
        full schema.

        However, the unbound schema on `Car` could also be extracted in
        a partial way (not all information will be required).  One example
        of this, is if the user calls `car.name = "new name"`.  In this
        case, the attribute (`name`) is matched against the schema.  The
        sole field is extracted and then, it is linked with the `"new name"`
        value, thus forming a small bound schema.  It is not a full
        schema, however (the make hasn't been specified), it is
        a partial schema.

    """

    def __init__(self, fields: Dict[str, Field],
            model: Optional[Model] = None):
        self.fields = fields
        self.model = model

        # Only useful for bound schemas.
        self.values = {}

    def __getitem__(self, key: Union[str, Field]) -> Any:
        """Get a value from a bound schema."""
        if not isinstance(key, str):
            key = key.name

        return self.values[key]

    @property
    def fields_with_values(self):
        """Return the fields and their values in a dictionary."""
        values = {}
        for key, field in self.fields.items():
            value = self.values.get(key, _NOT_SET)
            if value is not _NOT_SET:
                values[field] = value

        return values

    @property
    def primary_keys(self):
        """Return the tuple of primary fields."""
        return tuple(field for field in self.fields.values()
                if field.primary_key)

    @property
    def primary_names(self):
        """Return the dictionary of field names and values."""
        return {field.name: value for field, value in
                self.primary_fields.items()}

    @property
    def primary_fields(self):
        """Return the dictionary of primary fields with their values."""
        return {field: self.values.get(field.name) for field in
                self.fields.values() if field.primary_key}

    def bind(self, args: Sequence[Any], kwargs: Dict[str, Any],
            full: bool = False) -> "ModelSchema":
        """
        Extract and return a new schema.

        This will return an extracted bound schema.  It can be a
        full or partial extraction.

        Args:
            args (sequence): the positional argument's values.
            kwargs (dict): the keyword argument's values.
            full (bool): should the returned schema be a full schema
                    (all fields should have a value), or a partial
                    schema (not all fields will be returned with a value)?

        Returns:
            schema (ModelSchema): the new bound schema.

        """
        fields = iter(self.fields.values())
        for arg in args:
            field = None
            while field is None and (not field.has_default or field.set_by_database):
                try:
                    field = next(fields)
                except StopIteration:
                    raise ValueError(
                            f"field {arg!r}: cannot find a matching "
                            "field.  It might be better to use "
                            "keyword arguments."
                    )

            key = field.name
            if key in kwargs:
                raise ValueError(
                        f"argument {name!r} has been defined both with "
                        "a positional and keyword argument.   It might "
                        "be good to use only keyword arguments to avoid "
                        "this confusion"
                )
            kwargs[key] = arg

        # Now check kwargs
        fields = {}
        values = {}
        for field in self.fields.values():
            value = kwargs.get(field.name, _NOT_SET)
            if value is _NOT_SET:
                if not full:
                    continue

                if field.has_default:
                    value = field.default
                elif field.set_by_database:
                    continue
                else:
                    raise MissingArgument(self.model, field)
            elif full and field.set_by_database:
                raise ForbiddenArgument(self.model, field)

            fields[field.name] = field
            values[field.name] = value

        schema = ModelSchema(fields, self.model)
        schema.values = values
        return schema

    def bind_from(self, model: Model) -> "ModelSchema":
        """
        Create a bound schema from a model instance.

        This can only create a full bound schema (not a partial one).

        Args:
            model (Model): the model instance.

        Returns:
            schema (ModelSchema): the bound schema.

        """
        schema = ModelSchema(self.fields, self.model)
        schema.values = {key: getattr(model, key)
                for key in self.fields.keys()}
        return schema
