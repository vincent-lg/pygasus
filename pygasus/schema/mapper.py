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

"""Module containing the basic IDMapper."""

class IDMapper:

    """Basic ID mapper."""

    def __init__(self, database):
        self.database = database
        self.objects = {}

    def get(self, model, primary):
        """
        Get an object from the ID mapper, or None.

        Args:
            model (Model): the model class.
            primary (dict): the primary fields.

        Returns:
            model (Model instance or None).

        """
        fields = tuple(primary.values())
        return self.objects.get(model, {}).get(fields)

    def set(self, model, primary, instance):
        """
        Set the object in the ID mapper.

        Args:
            model (Model): the model class.
            primary (dict): the primary fields.
            instance (Model): the model instance.

        """
        if self.get(model, primary):
            return

        fields = tuple(primary.values())
        objects = self.objects.get(model)
        if objects is None:
            objects = {}
            self.objects[model] = objects
        objects[fields] = instance

    def delete(self, model, primary):
        """
        Delete the specified model instance from the ID mapper.

        Args:
            model (Model): the model subclass.
            primary (dict): the primary field dictionary.

        """
        fields = tuple(primary.values())
        objects = self.objects.get(model)
        if objects is None:
            return

        return objects.pop(fields)
