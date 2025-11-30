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

import pandas as pd
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.pandas.tests.data_type_ops.testing_utils import OpsTestBase


class TimeOpsTestsMixin:
    """Test mixin for Time type operations in pandas-on-Spark."""

    @property
    def pser(self):
        """Create a pandas Series with time values."""
        return pd.Series(
            [datetime.time(10, 30, 0), datetime.time(14, 45, 30), datetime.time(9, 0, 0)]
        )

    @property
    def psser(self):
        """Create a pandas-on-Spark Series from pser."""
        return ps.from_pandas(self.pser)

    @property
    def time_pdf(self):
        """Create a pandas DataFrame with multiple time columns."""
        psers = {
            "this": self.pser,
            "that": pd.Series(
                [datetime.time(8, 15, 0), datetime.time(16, 30, 45), datetime.time(12, 0, 0)]
            ),
        }
        return pd.concat(psers, axis=1)

    @property
    def time_psdf(self):
        """Create a pandas-on-Spark DataFrame from time_pdf."""
        return ps.from_pandas(self.time_pdf)

    @property
    def some_time(self):
        """A sample time value for testing."""
        return datetime.time(12, 0, 0)

    def test_add(self):
        """Test that addition is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser + "x")
        self.assertRaises(TypeError, lambda: self.psser + 1)
        self.assertRaises(TypeError, lambda: self.psser + self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        """Test subtraction operations for Time type."""
        # String and integer subtraction should fail
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        # Time - Time subtraction should work (returns nanoseconds)
        # Note: We can't directly compare with pandas because pandas
        # doesn't support time type in the same way, so we just verify
        # the operation works
        result = self.psser - self.some_time
        self.assertEqual(len(result), len(self.psser))

        # Test DataFrame column subtraction
        pdf, psdf = self.time_pdf, self.time_psdf
        result = psdf["this"] - psdf["that"]
        self.assertEqual(len(result), len(pdf))

    def test_mul(self):
        """Test that multiplication is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)
        self.assertRaises(TypeError, lambda: self.psser * self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        """Test that true division is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)
        self.assertRaises(TypeError, lambda: self.psser / self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        """Test that floor division is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)
        self.assertRaises(TypeError, lambda: self.psser // self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        """Test that modulo is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)
        self.assertRaises(TypeError, lambda: self.psser % self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        """Test that power operation is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser**1)
        self.assertRaises(TypeError, lambda: self.psser**self.some_time)

        for psser in self.pssers:
            self.assertRaises(TypeError, lambda: self.psser**psser)

    def test_radd(self):
        """Test that reverse addition is not supported for Time type."""
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)
        self.assertRaises(TypeError, lambda: self.some_time + self.psser)

    def test_rsub(self):
        """Test reverse subtraction for Time type."""
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)

        # Time - Time subtraction should work
        result = self.some_time - self.psser
        self.assertEqual(len(result), len(self.psser))

    def test_rmul(self):
        """Test that reverse multiplication is not supported for Time type."""
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: 1 * self.psser)
        self.assertRaises(TypeError, lambda: self.some_time * self.psser)

    def test_rtruediv(self):
        """Test that reverse true division is not supported for Time type."""
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: 1 / self.psser)
        self.assertRaises(TypeError, lambda: self.some_time / self.psser)

    def test_rfloordiv(self):
        """Test that reverse floor division is not supported for Time type."""
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: 1 // self.psser)
        self.assertRaises(TypeError, lambda: self.some_time // self.psser)

    def test_rmod(self):
        """Test that reverse modulo is not supported for Time type."""
        self.assertRaises(TypeError, lambda: 1 % self.psser)
        self.assertRaises(TypeError, lambda: self.some_time % self.psser)

    def test_rpow(self):
        """Test that reverse power operation is not supported for Time type."""
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: 1**self.psser)
        self.assertRaises(TypeError, lambda: self.some_time**self.psser)

    def test_and(self):
        """Test that bitwise AND is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser & True)
        self.assertRaises(TypeError, lambda: self.psser & False)
        self.assertRaises(TypeError, lambda: self.psser & self.psser)

    def test_rand(self):
        """Test that reverse bitwise AND is not supported for Time type."""
        self.assertRaises(TypeError, lambda: True & self.psser)
        self.assertRaises(TypeError, lambda: False & self.psser)

    def test_or(self):
        """Test that bitwise OR is not supported for Time type."""
        self.assertRaises(TypeError, lambda: self.psser | True)
        self.assertRaises(TypeError, lambda: self.psser | False)
        self.assertRaises(TypeError, lambda: self.psser | self.psser)

    def test_ror(self):
        """Test that reverse bitwise OR is not supported for Time type."""
        self.assertRaises(TypeError, lambda: True | self.psser)
        self.assertRaises(TypeError, lambda: False | self.psser)

    def test_from_to_pandas(self):
        """Test conversion between pandas and pandas-on-Spark."""
        data = [datetime.time(10, 30, 0), datetime.time(14, 45, 30), datetime.time(9, 0, 0)]
        pser = pd.Series(data)
        psser = ps.Series(data)
        self.assert_eq(pser, psser._to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        """Test isnull for Time type."""
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        """Test type conversion for Time type."""
        pser = self.pser
        psser = self.psser

        # Convert to string
        self.assert_eq(pser.astype(str), psser.astype(str))

        # Boolean conversion should fail
        self.assertRaises(TypeError, lambda: psser.astype(bool))

        # Categorical conversion
        cat_type = CategoricalDtype(categories=["a", "b", "c"])
        self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))

    def test_neg(self):
        """Test that negation is not supported for Time type."""
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        """Test that absolute value is not supported for Time type."""
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        """Test that bitwise inversion is not supported for Time type."""
        self.assertRaises(TypeError, lambda: ~self.psser)

    def test_eq(self):
        """Test equality comparison for Time type."""
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] == pdf["that"], psdf["this"] == psdf["that"])
        self.assert_eq(pdf["this"] == pdf["this"], psdf["this"] == psdf["this"])

    def test_ne(self):
        """Test inequality comparison for Time type."""
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] != pdf["that"], psdf["this"] != psdf["that"])
        self.assert_eq(pdf["this"] != pdf["this"], psdf["this"] != psdf["this"])

    def test_lt(self):
        """Test less than comparison for Time type."""
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] < pdf["that"], psdf["this"] < psdf["that"])
        self.assert_eq(pdf["this"] < pdf["this"], psdf["this"] < psdf["this"])

    def test_le(self):
        """Test less than or equal comparison for Time type."""
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] <= pdf["that"], psdf["this"] <= psdf["that"])
        self.assert_eq(pdf["this"] <= pdf["this"], psdf["this"] <= psdf["this"])

    def test_gt(self):
        """Test greater than comparison for Time type."""
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] > pdf["that"], psdf["this"] > psdf["that"])
        self.assert_eq(pdf["this"] > pdf["this"], psdf["this"] > psdf["this"])

    def test_ge(self):
        """Test greater than or equal comparison for Time type."""
        pdf, psdf = self.time_pdf, self.time_psdf
        self.assert_eq(pdf["this"] >= pdf["that"], psdf["this"] >= psdf["that"])
        self.assert_eq(pdf["this"] >= pdf["this"], psdf["this"] >= psdf["this"])


class TimeOpsTests(
    TimeOpsTestsMixin,
    OpsTestBase,
    PandasOnSparkTestCase,
):
    """Test suite for Time type operations."""

    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_time_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
