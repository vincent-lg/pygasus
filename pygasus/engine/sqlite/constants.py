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

"""Constants for the Sqlite3Engine database engine."""

from textwrap import dedent

from pygasus.engine.generic.columns import (
        BlobColumn, DateColumn, IntegerColumn,
        RealColumn, TextColumn, TimestampColumn)

## Column types:
SQL_TYPES = {
        BlobColumn: "BLOB",
        DateColumn: "DATE",
        IntegerColumn: "INTEGER",
        RealColumn: "REAL",
        TextColumn: "TEXT",
        TimestampColumn: "TIMESTAMP",
}

## SQL queries:
CREATE_MIGRATION_TABLE_QUERY = dedent("""
    CREATE TABLE IF NOT EXISTS pygasus_migration (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        table_name TEXT UNIQUE NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        schema BLOB NOT NULL
    );
""".strip("\n"))

CREATE_TABLE_QUERY = dedent("""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    );
""".strip("\n"))

SELECT_QUERY = dedent("""
    SELECT {columns} FROM {table_name}
    {join}
    WHERE {filters};
""".strip("\n"))

INSERT_QUERY = dedent("""
    INSERT INTO {table_name} ({columns})
    VALUES ({values});
""".strip("\n"))

UPDATE_QUERY = dedent("""
    UPDATE {table_name}
    SET {column}=?
    WHERE {filters}
""".strip("\n"))

DELETE_QUERY = dedent("""
    DELETE FROM {table_name}
    WHERE {filters}
""".strip("\n"))
