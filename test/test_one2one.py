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

"""Test the model API with a one-to-one relations."""

from test.base import BaseTest

from pygasus import Model
from pygasus.exceptions import *

class Book(Model):

    """A simple book model."""

    title: str
    author: "Author"
    year: int

class Author(Model):

    """Book author."""

    last_name: str
    first_name: str
    book: "Book" = None
    born_in: int

class TestModels(BaseTest):

    """Test the model API."""

    models = (Book, Author)

    def setUp(self):
        super().setUp()

    def test_create(self):
        """Create several books and authors, linking them."""
        dickens = Author.create(first_name="Charles", last_name="Dickens",
                born_in=1812)
        carol = Book.create(title="A Christmas Carol", author=dickens, year=1843)
        self.assertIs(carol.author, dickens)
        self.assertIs(dickens.book, carol)

        # Create a second author and assign the book to it.
        london = Author.create(first_name="Jack", last_name="London",
                born_in=1876, book=carol)
        self.assertIs(london.book, carol)
        self.assertIs(carol.author, london)
        self.assertIsNone(dickens.book)

