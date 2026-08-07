#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import struct
from typing import IO

from pyspark.errors import PySparkAssertionError
from pyspark.logger.worker_io import capture_outputs
from pyspark.serializers import UTF8Deserializer, read_bool, read_int, write_int
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, _parse_datatype_json_string
from pyspark.sql.worker.data_source_pushdown_filters import deserializeFilter
from pyspark.sql.worker.plan_data_source_read import write_read_func_and_partitions
from pyspark.sql.worker.utils import worker_run
from pyspark.worker_util import (
    get_sock_file_to_executor,
    pickleSer,
    read_command,
)

utf8_deserializer = UTF8Deserializer()


def _main(infile: IO, outfile: IO) -> None:
    """
    Push down OFFSET to a Python data source during query planning.

    This process is invoked from `UserDefinedPythonDataSourceOffsetPushdownRunner` in the
    JVM. It receives a data source instance, schema, previously-pushed filters (to restore
    reader state), and an offset. It creates a reader, replays filter pushdown, calls
    pushOffset, and returns the read function, partitions, and whether the offset was accepted.

    Protocol (JVM -> Python):
      - pickled DataSource instance
      - schema JSON
      - serialized filters JSON (same format as data_source_pushdown_filters.py)
      - offset (int)
      - max_arrow_batch_size (int)
      - binary_as_bytes (bool)

    Protocol (Python -> JVM):
      - read func + partitions (via write_read_func_and_partitions)
      - accepted (bool)
    """
    data_source = read_command(pickleSer, infile)
    if not isinstance(data_source, DataSource):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "a Python data source instance of type 'DataSource'",
                "actual": f"'{type(data_source).__name__}'",
            },
        )

    schema_json = utf8_deserializer.loads(infile)
    schema = _parse_datatype_json_string(schema_json)
    if not isinstance(schema, StructType):
        raise PySparkAssertionError(
            errorClass="DATA_SOURCE_TYPE_MISMATCH",
            messageParameters={
                "expected": "an output schema of type 'StructType'",
                "actual": f"'{type(schema).__name__}'",
            },
        )

    filter_json_str = utf8_deserializer.loads(infile)
    filter_dicts = json.loads(filter_json_str)
    # Reconstruct the Filter objects that were previously pushed.
    supported_filters = [deserializeFilter(f) for f in filter_dicts]

    offset = read_int(infile)
    max_arrow_batch_size = read_int(infile)
    assert max_arrow_batch_size > 0, (
        f"The maximum arrow batch size should be greater than 0, but got '{max_arrow_batch_size}'"
    )
    binary_as_bytes = read_bool(infile)

    with capture_outputs():
        reader = data_source.reader(schema=schema)
        if not isinstance(reader, DataSourceReader):
            raise PySparkAssertionError(
                errorClass="DATA_SOURCE_TYPE_MISMATCH",
                messageParameters={
                    "expected": "an instance of DataSourceReader",
                    "actual": f"'{type(reader).__name__}'",
                },
            )

        # Replay previously-accepted filter pushdown so the reader has the correct state
        # before we call pushOffset. We pass only the accepted filters (not the full
        # original set) because we want to restore reader state, not re-run acceptance
        # logic. The return value is ignored -- we already know which filters were
        # accepted in the earlier planning step.
        if supported_filters:
            reader.pushFilters(supported_filters)

        accepted = reader.pushOffset(offset)

        write_read_func_and_partitions(
            outfile,
            reader=reader,
            data_source=data_source,
            schema=schema,
            max_arrow_batch_size=max_arrow_batch_size,
            binary_as_bytes=binary_as_bytes,
        )

        # Send whether the offset was accepted (Java DataInputStream.readBoolean reads 1 byte).
        outfile.write(struct.pack("!?", accepted))
        outfile.flush()


def main(infile: IO, outfile: IO) -> None:
    worker_run(_main, infile, outfile)


if __name__ == "__main__":
    with get_sock_file_to_executor() as sock_file:
        main(sock_file, sock_file)
