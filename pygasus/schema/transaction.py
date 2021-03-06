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

"""Module containing the Transaction class, to handle database transactions."""

class Transaction:

    """
    Class representing an inner or outer transaction.
    """

    def __init__(self, database, parent=None):
        self.database = database
        self.engine = database._engine
        self.parent = parent
        self.objects = {}

    def __enter__(self):
        self.database._current_transaction = self
        self.engine.begin_transaction(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.database._current_transaction = self.parent
        if exc_type:
            # Restore transaction objects as they were.
            for obj, attrs in self.objects.items():
                obj._has_init = False
                for key, value in attrs.items():
                    setattr(obj, key, value)
                obj._has_init = True
            self.engine.rollback_transaction(self)
        else:
            self.engine.commit_transaction(self)
