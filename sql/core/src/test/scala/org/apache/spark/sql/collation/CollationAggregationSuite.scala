/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.collation

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class CollationAggregationSuite
  extends SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("group by collated column doesn't work with obj hash aggregate") {
    val tblName = "grp_by_tbl"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('hello', 1), ('HELLO', 2), ('HeLlO', 3)")

      // Result is correct without forcing object hash aggregate.
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tblName GROUP BY c1"),
        Seq(Row(3)))

      withSQLConf("spark.sql.test.forceApplyObjectHashAggregate" -> true.toString) {
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName GROUP BY c1"),
          Seq(Row(1), Row(1), Row(1)))

        checkAnswer(
          sql(s"SELECT COLLECT_LIST(c2) AS c3 FROM $tblName GROUP BY c1 ORDER BY c3"),
          Seq(Row(Seq(1)), Row(Seq(2)), Row(Seq(3))))
      }
    }
  }

  test("imperative aggregate fn does not use objectHashAggregate when group by collated column") {
    val tblName = "imp_agg"
    Seq(true, false).foreach { useObjHashAgg =>
      withTable(tblName) {
        withSQLConf("spark.sql.execution.useObjectHashAggregateExec" -> useObjHashAgg.toString) {
          sql(
            s"""
               |CREATE TABLE $tblName (
               |  c1 STRING COLLATE UTF8_LCASE,
               |  c2 INT
               |) USING PARQUET
               |""".stripMargin)
          sql(s"INSERT INTO $tblName VALUES ('HELLO', 1), ('hello', 2), ('HeLlO', 3)")

          val df = sql(s"SELECT COLLECT_LIST(c2) as list FROM $tblName GROUP BY c1")
          val executedPlan = df.queryExecution.executedPlan

          // Plan should not have any hash aggregate nodes.
          collectFirst(executedPlan) {
            case _: ObjectHashAggregateExec => fail("ObjectHashAggregateExec should not be used.")
            case _: HashAggregateExec => fail("HashAggregateExec should not be used.")
          }

          // Plan should have a [[SortAggregateExec]] node.
          assert(collectFirst(executedPlan) {
            case _: SortAggregateExec => true
          }.nonEmpty)

          checkAnswer(
            // Sort the values to get deterministic output.
            df.selectExpr("array_sort(list)"),
            Seq(Row(Seq(1, 2, 3)))
          )
        }
      }
    }
  }

  // collect_set is non-deterministic in which representative of a collation-equal group it keeps,
  // so tests assert on a collation-collapsed form (lower(...)) rather than a fixed case.

  test("collect_set dedups collation-equal strings (UTF8_LCASE)") {
    val tblName = "collect_set_lcase"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), ('FoO'), ('bar'), ('BAR')")

      // 'foo'/'FOO'/'FoO' collapse to one group and 'bar'/'BAR' to another under UTF8_LCASE.
      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(c1), x -> lower(x))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set dedups collation-equal strings (UNICODE_CI)") {
    val tblName = "collect_set_unicode_ci"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UNICODE_CI) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('cafe'), ('CAFE'), ('Cafe'), ('bar')")

      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(c1), x -> lower(x))) FROM $tblName"),
        Seq(Row(Seq("bar", "cafe"))))
    }
  }

  test("collect_set is collation-aware for collated strings nested in struct/array") {
    val tblName = "collect_set_nested"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, c2 INT) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo', 1), ('FOO', 1), ('bar', 1)")

      // Nested in a struct: named_struct('s', c1) dedups on c1's collation key.
      checkAnswer(
        sql(s"SELECT size(collect_set(named_struct('s', c1))) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(named_struct('s', c1)), " +
          s"x -> lower(x.s))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))

      // Nested in an array: array(c1) dedups on the element's collation key.
      checkAnswer(sql(s"SELECT size(collect_set(array(c1))) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(array(c1)), x -> lower(x[0]))) " +
          s"FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set collation and float normalization compose for nested structs") {
    val tblName = "collect_set_nested_float"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE, d DOUBLE) USING PARQUET")
      // ('foo', 0.0) and ('FOO', -0.0) must dedup to a single entry, which requires BOTH the
      // collation key ('foo'/'FOO' equal under UTF8_LCASE) AND float normalization (0.0/-0.0
      // collapsed) to fire on the same nested key. If either did not, they would stay distinct
      // and the set size would be 3 instead of 2.
      sql(s"INSERT INTO $tblName VALUES ('foo', 0.0), ('FOO', -0.0), ('bar', 1.0)")

      checkAnswer(
        sql(s"SELECT size(collect_set(named_struct('s', c1, 'd', d))) FROM $tblName"),
        Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(named_struct('s', c1, 'd', d)), " +
          s"x -> lower(x.s))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set IGNORE NULLS (default) drops nulls while deduping collated values") {
    val tblName = "collect_set_ignore_nulls"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), (NULL), ('bar'), (NULL)")

      // Default IGNORE NULLS: nulls are dropped, and 'foo'/'FOO' still collapse under UTF8_LCASE,
      // so the set is {<foo group>, <bar group>} -> size 2 with no null element.
      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(2)))
      checkAnswer(
        sql(s"SELECT array_sort(transform(collect_set(c1), x -> lower(x))) FROM $tblName"),
        Seq(Row(Seq("bar", "foo"))))
    }
  }

  test("collect_set still rejects maps even when they carry collated strings") {
    // The collation relaxation must not open the map gate: a HashSet cannot dedup maps, so a map
    // with a collated-string key (which would otherwise take the collation-key path) must still
    // fail checkInputDataTypes with UNSUPPORTED_INPUT_TYPE.
    val collatedMap = spark.sql(
      "SELECT map(CAST(k AS STRING COLLATE UTF8_LCASE), v) AS m " +
        "FROM VALUES ('a', 1), ('A', 2) AS t(k, v)")
    checkError(
      exception = intercept[AnalysisException](collatedMap.select(collect_set(col("m")))),
      condition = "DATATYPE_MISMATCH.UNSUPPORTED_INPUT_TYPE",
      parameters = Map(
        "functionName" -> "`collect_set`",
        "dataType" -> "\"MAP\"",
        "sqlExpr" -> "\"collect_set(m)\""),
      context = ExpectedContext(
        fragment = "collect_set", callSitePattern = getCurrentClassCallSitePattern))

    // A map nested alongside a collated string in a struct is still rejected: existsRecursively
    // finds the MapType before the collation-key path is ever considered.
    val nestedMap = spark.sql(
      "SELECT named_struct('s', CAST(k AS STRING COLLATE UTF8_LCASE), 'm', map(k, v)) AS a " +
        "FROM VALUES ('a', 1), ('A', 2) AS t(k, v)")
    checkError(
      exception = intercept[AnalysisException](nestedMap.select(collect_set(col("a")))),
      condition = "DATATYPE_MISMATCH.UNSUPPORTED_INPUT_TYPE",
      parameters = Map(
        "functionName" -> "`collect_set`",
        "dataType" -> "\"MAP\"",
        "sqlExpr" -> "\"collect_set(a)\""),
      context = ExpectedContext(
        fragment = "collect_set", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("collect_set RESPECT NULLS keeps one null alongside collation-deduped values") {
    val tblName = "collect_set_respect_nulls"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING COLLATE UTF8_LCASE) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), (NULL), ('bar'), (NULL)")

      // RESPECT NULLS keeps a single null; 'foo'/'FOO' still collapse under UTF8_LCASE, so the
      // set is {null, <foo group>, <bar group>} -> size 3. sort_array orders null first.
      checkAnswer(
        sql(s"SELECT size(collect_set(c1) RESPECT NULLS) FROM $tblName"), Seq(Row(3)))
      checkAnswer(
        sql(s"SELECT sort_array(transform(collect_set(c1) RESPECT NULLS, x -> lower(x))) " +
          s"FROM $tblName"),
        Seq(Row(Seq(null, "bar", "foo"))))
    }
  }

  test("collect_set collation-aware dedup survives the merge path across partitions") {
    Seq("UTF8_LCASE", "UNICODE_CI").foreach { collation =>
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
        val values = (0 until 200).map(i => if (i % 2 == 0) "foo" else "FOO")
        val df = values.toDF("c")
          .select(col("c").cast(s"string collate $collation").as("c"))
          .repartition(8)

        // All 200 values are collation-equal, so after partial aggregation on 8 partitions and
        // the final merge (serialize/deserialize + union), the set has a single element.
        val result = df.agg(collect_set(col("c")).as("s"))
        checkAnswer(result.select(size(col("s"))), Seq(Row(1)))
        checkAnswer(result.select(transform(col("s"), x => lower(x))), Seq(Row(Seq("foo"))))
      }
    }
  }

  test("collect_set on UTF8_BINARY strings is unchanged (case-sensitive)") {
    val tblName = "collect_set_binary"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (c1 STRING) USING PARQUET")
      sql(s"INSERT INTO $tblName VALUES ('foo'), ('FOO'), ('foo'), ('bar')")

      // Default UTF8_BINARY collation is case-sensitive: 'foo' and 'FOO' stay distinct.
      checkAnswer(sql(s"SELECT size(collect_set(c1)) FROM $tblName"), Seq(Row(3)))
      checkAnswer(
        sql(s"SELECT array_sort(collect_set(c1)) FROM $tblName"),
        Seq(Row(Seq("FOO", "bar", "foo"))))
    }
  }
}
