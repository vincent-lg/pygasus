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

"""Base class for all models."""

from typing import get_type_hints, Any, Dict, Optional, Type

from pygasus.schema.field import Field, HasOne
from pygasus.schema.schema import ModelSchema

MODELS = set()
_NOT_SET = object()

class MetaModel(type):

    """Metaclass for all models."""

    def __new__(cls, name, bases, attrs):
        cls = super().__new__(cls, name, bases, attrs)
        if cls.__name__ != "Model":
            MODELS.add(cls)
        return cls

    def _primary_values_from_dict(self, data: Dict[str, Any]) -> tuple:
        """Return a tuple of primary fields."""
        primary = []
        for key, field in self._schema.fields.items():
            if field.primary_key:
                value = data[key]
                primary.append(value)

        return tuple(primary)

    @staticmethod
    def get_fields(model: Type["Model"],
            others: Dict[str, Type["Model"]]) -> Dict[str, Field]:
        """Get the tuple of fields from a model."""
        import pygasus
        others["pygasus"] = pygasus
        fields = {}
        # Wrap annotated fields.
        for key, annotation in get_type_hints(model, others).items():
            value = getattr(model, key, _NOT_SET)
            if key.startswith("_"):
                continue

            if value is _NOT_SET:
                # Create a field for this annotation.
                setattr(model, key, Field(annotation))
            elif not isinstance(value, Field): # This is not a field, wrap it.
                setattr(model, key, Field(annotation, default=value))

        # Browse the field objects.
        for key, value in tuple(model.__dict__.items()):
            if isinstance(value, Field):
                value.model = model
                value.name = key
                fields[key] = value

        # If there is no PrimaryKey field, add one.
        if not any(field for field in fields.values() if field.primary_key):
            if any(field for field in fields.values() if field.name == "id"):
                raise ValueError(
                        f"model {model!r}: no primary key is defined, but "
                        "there already exists a field of name 'id', so no "
                        "key can be added.  You should specify ONE "
                        "primary key in your model class"
                )

            primary_key = Field(int, primary_key=True, name="id")
            fields = dict(id=primary_key, **fields)
            setattr(model, "id", primary_key)
        elif len([field for field in fields.values() if field.primary_key]) < 1:
            raise ValueError(
                    f"model {model!r}: there are at least two primary key "
                    "fields, which is not allowed.  Please choose one "
                    "primary key field, although you can have secondary "
                    "key fields, and composite keys, if you want"
            )

        return fields

    @staticmethod
    def complete_fields(model: Type["Model"]):
        """Complete model fields in relations."""
        for key, field in model._schema.fields.items():
            if field.mirror:
                continue

            if issubclass(field.field_type, Model):
                # Try to find the opposite field.
                for opposite in field.field_type._schema.fields.values():
                    if opposite.field_type is field.model:
                        break
                else:
                    raise ValueError(
                            f"cannot find the opposite field of the relation "
                            f"started in {field.model.__name__}."
                            f"{field.name}: no field in "
                            f"{field.field_type.__name__} points to "
                            f"{field.model.__name__}"
                    )

                field = HasOne(field)
                opposite = HasOne(opposite)
                field.mirror = opposite
                model._schema.fields[key] = field
                setattr(model, field.name, field)
                opposite.mirror = field
                opposite.model._schema.fields[opposite.name] = opposite
                setattr(opposite.model, opposite.name, opposite)

    def __str__(self):
        text = f"{self.__name__} ("
        if self._fields:
            indent = "\n" + " " * 4
            text += indent + indent.join(
                    str(field) for field in self._fields.values())
            text += "\n)"
        else:
            text += ")"

        return text

    def load_schema(self, fields: Dict[str, Field]):
        """
        Load the model's schema.

        Args:
            fields (dict): the model's current fields.

        """
        self._schema = ModelSchema(fields, self)

    def create(self, *args, **kwargs):
        """Create and return a model instance."""
        schema = self._schema.bind(args, kwargs, full=True)
        return self._database.create_instance(self, schema)

    def get(self, *args, **kwargs):
        """
        Get a model instance from the database.

        Optional and keyword arguments are supported.

        Returns:
            instance (Model): the model instance or None.

        Note:
            If no model is found, or if several models with these fields
            are found, return None.

        """
        schema = self._schema.bind(args, kwargs, full=False)
        return self._database.get_instance(self, schema)

    def select(self, query, **kwargs):
        """
        Return the matching instances.

        """
        return self._database.select(self, query, kwargs)

    def update_instance(self, instance, field, value):
        """
        Update the value of a given field.

        Args:
            instance (Model): the model instance.
            field (Field): the field to update.
            value (Any): the new field value.

        """
        transaction = instance._engine.database._current_transaction
        if transaction:
            if instance not in transaction.objects:
                schema = instance._schema.bind_from(instance)
                transaction.objects[instance] = dict(schema.values)
        return self._database.update_instance(instance, field, value)


class Model(metaclass=MetaModel):

    """
    Base class for all database models.

    Models match tables in a database.  A class in-code contains fields,
    each field being a database column when stored.  Models are not
    directly linked to a database, though once instantiated, models
    usually can connect to a database engine to perform operations.
    A database can decide to map all models (all imported classes inheriting
    from `Model`) or a specific set.

    """

    _alt_name: Optional[str] = None
    _database: Optional['pygasus.schema.database.Database'] = None
    _engine: Optional['pygasus.engine.base.BaseEngine'] = None
    _table = None

    def __init__(self, **kwargs):
        self._has_init = False
        self._schema = type(self)._schema.bind((), kwargs)

        # First, insert only scalar data.
        for key, value in kwargs.items():
            if value is not None and not isinstance(value, Model):
                setattr(self, key, value)

        # Add the rest.
        for key, value in kwargs.items():
            if isinstance(value, Model):
                setattr(self, key, value)

        self._has_init = True

    def __repr__(self):
        pk = {}
        for field in self._schema.primary_keys:
            value = getattr(self, field.name)
            pk[field.name] = value

        pk = ", ".join([f"{key}={value!r}" for key, value in pk.items()])
        return f"<{type(self).__name__}({pk})>"

    @property
    def _primary_values(self):
        """Return a tuple of primary values for this model."""
        primary = []
        for key, field in type(self)._schema.fields.items():
            if field.primary_key:
                value = getattr(self, key)
                primary.append(value)

        return tuple(primary)

    def delete(self):
        """Remove this object from the database."""
        return self._database.delete_instance(self)
