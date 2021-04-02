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

"""Test the ID mapper."""

from test.base import BaseTest

from pygasus import IDMapper, Field, Model
from pygasus.exceptions import *

class Car(Model):

    """A simple car."""

    name = Field(str)
    price = Field(int)

class TestIDMapper(BaseTest):

    """Test the transaction API."""

    def setUp(self):
        super().setUp()
        self.db.id_mapper = IDMapper(self)
        self.db.bind((Car, ))

    def test_create_and_get(self):
        """Create several cars, checking their identities."""
        ford = Car.create(name="Ford", price=10000)
        peugeot = Car.create(name="Peugeot", price=8000)
        pygasus = Car.create(name="Pygasus", price=15000)
        self.assertIs(ford, Car.get(id=ford.id))
        self.assertIs(peugeot, Car.get(id=peugeot.id))
        self.assertIs(pygasus, Car.get(id=pygasus.id))

    def test_create_in_transaction(self):
        """Create and get in transactions."""
        # Create within a first transaction, no error.
        with self.db.transaction:
            ford = Car.create(name="Ford", price=10000)
            peugeot = Car.create(name="Peugeot", price=8000)
            pygasus = Car.create(name="Pygasus", price=15000)

        # No error has occurred, so the result should be the same as above.
        self.assertIs(ford, Car.get(id=ford.id))
        self.assertIs(peugeot, Car.get(id=peugeot.id))
        self.assertIs(pygasus, Car.get(id=pygasus.id))

        # Create an inner transaction with an error.
        with self.db.transaction:
            ford = Car.create(name="Ford", price=10000)
            peugeot = Car.create(name="Peugeot", price=8000)

            try:
                with self.db.transaction:
                    pygasus = Car.create(name="Pygasus", price=15000)
                    raise InterruptedError
            except InterruptedError:
                pass

        # An error has occurred in the inner transaction, so the first
        # and second car should be fine, but there shouldn't be any third car.
        self.assertIs(ford, Car.get(id=ford.id))
        self.assertIs(peugeot, Car.get(id=peugeot.id))
        self.assertIsNone(Car.get(id=pygasus.id))

    def test_update(self):
        """Test to update fields from cars."""
        # Updating without a transaction should behave as expected.
        ford = Car.create(name="Ford", price=10000)
        peugeot = Car.create(name="Peugeot", price=8000)
        pygasus = Car.create(name="Pygasus", price=15000)
        pygasus.price = 12000

        # Check the price in instance (ID mapped).
        self.assertEqual(ford.price, 10000)
        self.assertEqual(peugeot.price, 8000)
        self.assertEqual(pygasus.price, 12000)

        # Get the cars from the database.  The price should be the same.
        ford = Car.get(id=ford.id)
        peugeot = Car.get(id=peugeot.id)
        pygasus = Car.get(id=pygasus.id)
        self.assertEqual(ford.price, 10000)
        self.assertEqual(peugeot.price, 8000)
        self.assertEqual(pygasus.price, 12000)

        # Now, modify within a transaction without error.
        with self.db.transaction:
            pygasus.price = 14000
            self.assertEqual(pygasus.price, 14000)

        self.assertEqual(pygasus.price, 14000)
        self.assertEqual(Car.get(id=pygasus.id).price, 14000)

        # Make two modifications.  The one in the inner block should
        # be cancelled, because of an error.
        with self.db.transaction:
            pygasus.price = 17000
            self.assertEqual(pygasus.price, 17000)

            try:
                with self.db.transaction:
                    pygasus.price = 18000
                    self.assertEqual(pygasus.price, 18000)
                    raise InterruptedError
            except InterruptedError:
                pass

        self.assertEqual(pygasus.price, 17000)
        self.assertEqual(Car.get(id=pygasus.id).price, 17000)
