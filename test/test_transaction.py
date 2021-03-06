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

"""Test transactions."""

from test.base import BaseTest

from pygasus import Model
from pygasus.exceptions import *

class Product(Model):

    """A simple product."""

    name: str
    price: int


class TestTransactions(BaseTest):

    """Test the transaction API."""

    models = (Product, )

    def setUp(self):
        super().setUp()
        self.db.bind((Product, ))

    def test_create(self):
        """Create several instances."""
        # Try a transaction thqat should not fail.
        product_id = None
        with self.db.transaction:
            product = Product.create(name="apple", price=2)
            product_id = product.id

        # Right now, it should have been committed.
        self.assertIsNotNone(Product.get(id=product_id))

        # However, now try to add a product and cause a failure.
        try:
            with self.db.transaction:
                product = Product.create(name="pear", price=2)
                product_id = product.id
                raise InterruptedError
        except InterruptedError:
            pass

        # There should have been a rollback at this point, so
        # the product shouldn't be available.
        self.assertIsNone(Product.get(id=product_id))

    def test_update(self):
        """Test to update within a transaction."""
        product_id = None
        product = Product.create(name="apple", price=2)
        product_id = product.id

        # Try to change the price.
        with self.db.transaction:
            product.price = 3

        # Check that the product has been updated.
        product = Product.get(id=product_id)
        self.assertEqual(product.price, 3)

        # Now try a new update, but make sure it fails.
        try:
            with self.db.transaction:
                product.price = 4
                raise InterruptedError
        except InterruptedError:
            pass

        # There should have been a rollback, so the price
        # shouldn't have changed.
        self.assertEqual(product.price, 3)
        product = Product.get(id=product_id)
        self.assertEqual(product.price, 3)

        # Try again, to make sure double-transactions don't cause a crash.
        try:
            with self.db.transaction:
                product.price = 4
                raise InterruptedError
        except InterruptedError:
            pass

        # There should have been a rollback, so the price
        # shouldn't have changed.
        self.assertEqual(product.price, 3)
        product = Product.get(id=product_id)
        self.assertEqual(product.price, 3)
