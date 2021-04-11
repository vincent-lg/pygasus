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

"""Test the model API."""

from test.base import BaseTest

from pygasus import Model
from pygasus.exceptions import *

class Book(Model):

    """A simple book model."""

    title: str
    author: str
    year: int

class TestModels(BaseTest):

    """Test the model API."""

    models = (Book, )

    def setUp(self):
        super().setUp()

    def test_create(self):
        """Create several instances."""
        # Create a book with all fields.
        book = Book.create(title="A Voyage in a Balloon",
                author="Jules Verne", year=1851)
        self.assertEqual(book.title, "A Voyage in a Balloon")
        self.assertEqual(book.author, "Jules Verne")
        self.assertEqual(book.year, 1851)

        # Check that this book has a valid ID.
        self.assertIsNotNone(book.id)

        # Creating a book with missing fields should raise an error.
        with self.assertRaises(MissingArgument):
            Book.create(title="Something")

        # Creating a book with an ID shouldn't be allowed.
        with self.assertRaises(ForbiddenArgument):
            Book.create(id=4, title="A Voyage in a Balloon",
                    author="Jules Verne", year=1851)

    def test_get(self):
        """Test to get a model."""
        book = Book.create(title="A Voyage in a Balloon",
                author="Jules Verne", year=1851)
        self.assertIsNotNone(Book.get(id=book.id))
        self.assertIsNone(Book.get(id=book.id + 1))

    def test_update(self):
        """Test to update a model."""
        book = Book.create(title="A Voyage in a Balloon",
                author="Jules Verne", year=1851)
        book.year = 1852
        self.assertEqual(book.year, 1852)

        # Check that the same result is obtained through getting the object.
        self.assertEqual(Book.get(id=book.id).year, 1852)

        # But editing the ID raises an error.
        with self.assertRaises(SetByDatabase):
            book.id = 32

    def test_delete(self):
        """Create and delete a model."""
        book = Book.create(title="A Voyage in a Balloon",
                author="Jules Verne", year=1851)
        self.assertIsNotNone(Book.get(id=book.id))
        book.delete()
        self.assertIsNone(Book.get(id=book.id))
