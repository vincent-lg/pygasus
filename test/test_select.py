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

"""Test the select API."""

from test.base import BaseTest

from pygasus import Model
from pygasus.exceptions import *

class Book(Model):

    """A simple book model."""

    title: str
    author: str
    year: int


class TestSelect(BaseTest):

    """Test the model API."""

    models = (Book, )

    def setUp(self):
        super().setUp()

        # Create default books.
        self.balloon = Book.create(title="A Voyage in a Balloon",
                author="Jules Verne", year=1851)
        self.carol = Book.create(title="A Christmas Carol",
                author="Charles Dickens", year=1843)
        self.miserables = Book.create(title="Les Miserables",
                author="Victor Hugo", year=1862)
        self.hunchback = Book.create(title="The Hunchback of Notre-Dame",
                author="Victor Hugo", year=1831)
        self.bellew = Book.create(title="Smoke Bellew",
                author="Jack London", year=1912)
        self.amor = Book.create(title="Amor de Perdição",
                author="Camilo Castelo Branco", year=1862)
        self.eternity = Book.create(title="Le Cap Éternité",
                author="Charles Gill", year=1919)

    def test_equal(self):
        """Test the select operation."""
        # Select by title.
        results = list(Book.select(Book.title == "A Voyage in a Balloon"))
        self.assertIn(self.balloon, results)
        self.assertNotIn(self.carol, results)
        self.assertNotIn(self.miserables, results)
        self.assertNotIn(self.hunchback, results)
        self.assertNotIn(self.bellew, results)
        self.assertNotIn(self.amor, results)
        self.assertNotIn(self.eternity, results)

        # Query by year.
        results = list(Book.select(Book.year == 1862))
        self.assertNotIn(self.balloon, results)
        self.assertNotIn(self.carol, results)
        self.assertIn(self.miserables, results)
        self.assertNotIn(self.hunchback, results)
        self.assertNotIn(self.bellew, results)
        self.assertIn(self.amor, results)
        self.assertNotIn(self.eternity, results)

    def test_lower(self):
        """Test to lowercase fields."""
        results = list(Book.select(Book.title.lower() == "a voyage in a balloon"))
        self.assertIn(self.balloon, results)
        self.assertNotIn(self.carol, results)
        self.assertNotIn(self.miserables, results)
        self.assertNotIn(self.hunchback, results)
        self.assertNotIn(self.bellew, results)
        self.assertNotIn(self.amor, results)
        self.assertNotIn(self.eternity, results)

        # Test the unicode lowercase.
        results = list(Book.select(Book.title.lower() == "le cap éternité"))
        self.assertNotIn(self.balloon, results)
        self.assertNotIn(self.carol, results)
        self.assertNotIn(self.miserables, results)
        self.assertNotIn(self.hunchback, results)
        self.assertNotIn(self.bellew, results)
        self.assertNotIn(self.amor, results)
        self.assertIn(self.eternity, results)

    def test_contains(self):
        """Test the contains filter."""
        results = list(Book.select(Book.title.contains("Le")))
        self.assertNotIn(self.balloon, results)
        self.assertNotIn(self.carol, results)
        self.assertIn(self.miserables, results)
        self.assertNotIn(self.hunchback, results)
        self.assertNotIn(self.bellew, results)
        self.assertNotIn(self.amor, results)
        self.assertIn(self.eternity, results)

        # Combine with lower()
        results = list(Book.select(Book.title.lower().contains("le")))
        self.assertNotIn(self.balloon, results)
        self.assertNotIn(self.carol, results)
        self.assertIn(self.miserables, results)
        self.assertNotIn(self.hunchback, results)
        self.assertIn(self.bellew, results)
        self.assertNotIn(self.amor, results)
        self.assertIn(self.eternity, results)
