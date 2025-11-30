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

import datetime
import warnings
from typing import Any, Union

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark.sql.types import BooleanType, StringType, TimeType
from pyspark.sql.utils import pyspark_column_op
from pyspark.pandas._typing import Dtype, IndexOpsLike, SeriesOrIndex
from pyspark.pandas.base import IndexOpsMixin
from pyspark.pandas.data_type_ops.base import (
    DataTypeOps,
    _as_categorical_type,
    _as_other_type,
    _as_string_type,
    _sanitize_list_like,
)
from pyspark.pandas.typedef import pandas_on_spark_type


class TimeOps(DataTypeOps):
    """
    The class for binary operations of pandas-on-Spark objects with
    spark type: TimeType.

    This class provides pandas-style operations for Time columns
    including:
    - Comparison operations (lt, le, gt, ge, eq, ne)
    - Subtraction (time - time returns an interval in nanoseconds)
    - Type conversions (astype)
    - Pandas interoperability

    Examples
    --------
    >>> import pandas as pd
    >>> import pyspark.pandas as ps
    >>> from datetime import time
    >>>
    >>> # Create a time series
    >>> s = ps.Series([time(10, 30, 0), time(14, 45, 30), time(9, 0, 0)])
    >>>
    >>> # Comparison operations
    >>> s > time(10, 0, 0)
    >>>
    >>> # Convert to string
    >>> s.astype(str)
    """

    @property
    def pretty_name(self) -> str:
        return "times"

    def sub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        """
        Subtract operation for Time type.

        Time - Time returns the difference in nanoseconds as a long
        integer. This is to maintain consistency with Spark's behavior.

        Parameters
        ----------
        left : IndexOpsLike
            Left operand (Time series)
        right : Any
            Right operand (Time series or datetime.time)

        Returns
        -------
        SeriesOrIndex
            The difference in nanoseconds as a long integer

        Raises
        ------
        TypeError
            If right operand is not a Time series or datetime.time
        """
        _sanitize_list_like(right)
        msg = (
            "Note that time subtraction returns an integer representing "
            "the difference in nanoseconds."
        )

        if isinstance(right, IndexOpsMixin) and isinstance(right.spark.data_type, TimeType):
            warnings.warn(msg, UserWarning)
            # Time subtraction in Spark returns interval, we'll
            # represent as nanoseconds
            from pyspark.pandas.base import column_op
            from pyspark.sql import functions as F

            return column_op(lambda l, r: F.time_diff("nanosecond", r, l))(left, right)
        elif isinstance(right, datetime.time):
            warnings.warn(msg, UserWarning)
            from pyspark.pandas.base import column_op
            from pyspark.sql import functions as F

            return column_op(lambda l: F.time_diff("nanosecond", F.lit(right), l))(left)
        else:
            raise TypeError("Time subtraction can only be applied to time series.")

    def rsub(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        """
        Reverse subtract operation for Time type.

        Parameters
        ----------
        left : IndexOpsLike
            Left operand (Time series)
        right : Any
            Right operand (datetime.time)

        Returns
        -------
        SeriesOrIndex
            The difference in nanoseconds as a long integer

        Raises
        ------
        TypeError
            If right operand is not a datetime.time
        """
        _sanitize_list_like(right)
        msg = (
            "Note that time subtraction returns an integer representing "
            "the difference in nanoseconds."
        )

        if isinstance(right, datetime.time):
            warnings.warn(msg, UserWarning)
            from pyspark.pandas.base import column_op
            from pyspark.sql import functions as F

            return column_op(lambda l: F.time_diff("nanosecond", l, F.lit(right)))(left)
        else:
            raise TypeError("Time subtraction can only be applied to time series.")

    def lt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        """Less than comparison."""
        _sanitize_list_like(right)
        return pyspark_column_op("__lt__", left, right)

    def le(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        """Less than or equal comparison."""
        _sanitize_list_like(right)
        return pyspark_column_op("__le__", left, right)

    def ge(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        """Greater than or equal comparison."""
        _sanitize_list_like(right)
        return pyspark_column_op("__ge__", left, right)

    def gt(self, left: IndexOpsLike, right: Any) -> SeriesOrIndex:
        """Greater than comparison."""
        _sanitize_list_like(right)
        return pyspark_column_op("__gt__", left, right)

    def prepare(self, col: pd.Series) -> pd.Series:
        """
        Prepare column when from_pandas.

        Parameters
        ----------
        col : pd.Series
            Pandas Series to prepare

        Returns
        -------
        pd.Series
            Prepared series
        """
        return col

    def astype(self, index_ops: IndexOpsLike, dtype: Union[str, type, Dtype]) -> IndexOpsLike:
        """
        Cast Time column to another data type.

        Parameters
        ----------
        index_ops : IndexOpsLike
            The index or series to cast
        dtype : Union[str, type, Dtype]
            Target data type

        Returns
        -------
        IndexOpsLike
            Casted index or series

        Raises
        ------
        TypeError
            If trying to cast to boolean type
        """
        dtype, spark_type = pandas_on_spark_type(dtype)

        if isinstance(dtype, CategoricalDtype):
            return _as_categorical_type(index_ops, dtype, spark_type)
        elif isinstance(spark_type, BooleanType):
            raise TypeError("cannot astype a %s to [bool]" % self.pretty_name)
        elif isinstance(spark_type, StringType):
            return _as_string_type(index_ops, dtype, null_str=str(pd.NaT))
        else:
            return _as_other_type(index_ops, dtype, spark_type)
