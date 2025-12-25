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

package org.apache.spark.sql.catalyst.expressions

import org.apache.datasketches.kll.{KllDoublesSketch, KllFloatsSketch, KllLongsSketch}
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Cast.{toSQLExpr, toSQLId, toSQLType}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, DataType, DoubleType, FloatType, LongType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns human readable summary information about this sketch.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(_FUNC_(kll_sketch_agg_bigint(col))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchToStringBigint(child: Expression) extends KllSketchToStringBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchToStringBigint =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_to_string_bigint"
  override def nullSafeEval(input: Any): Any = {
    try {
      val buffer = input.asInstanceOf[Array[Byte]]
      val sketch = KllLongsSketch.heapify(Memory.wrap(buffer))
      UTF8String.fromString(sketch.toString())
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns human readable summary information about this sketch.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(_FUNC_(kll_sketch_agg_float(col))) > 0 FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)), (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchToStringFloat(child: Expression) extends KllSketchToStringBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchToStringFloat =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_to_string_float"
  override def nullSafeEval(input: Any): Any = {
    try {
      val buffer = input.asInstanceOf[Array[Byte]]
      val sketch = KllFloatsSketch.heapify(Memory.wrap(buffer))
      UTF8String.fromString(sketch.toString())
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns human readable summary information about this sketch.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(_FUNC_(kll_sketch_agg_double(col))) > 0 FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)), (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchToStringDouble(child: Expression) extends KllSketchToStringBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchToStringDouble =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_to_string_double"
  override def nullSafeEval(input: Any): Any = {
    try {
      val buffer = input.asInstanceOf[Array[Byte]]
      val sketch = KllDoublesSketch.heapify(Memory.wrap(buffer))
      UTF8String.fromString(sketch.toString())
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }
}

/** This is a base class for the above expressions to reduce boilerplate. */
abstract class KllSketchToStringBase
    extends UnaryExpression
        with CodegenFallback
        with ImplicitCastInputTypes {
  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def nullIntolerant: Boolean = true
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the number of items collected in the sketch.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_bigint(col)) FROM VALUES (1), (2), (3), (4), (5) tab(col);
       5
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetNBigint(child: Expression) extends KllSketchGetNBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchGetNBigint =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_get_n_bigint"
  override protected def kllUtilsMethod: String = "getKllLongsSketchN"
  override def nullSafeEval(input: Any): Any = {
    KllSketchUtils.getKllLongsSketchN(input, prettyName)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the number of items collected in the sketch.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_float(col)) FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)), (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)) tab(col);
       5
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetNFloat(child: Expression) extends KllSketchGetNBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchGetNFloat =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_get_n_float"
  override protected def kllUtilsMethod: String = "getKllFloatsSketchN"
  override def nullSafeEval(input: Any): Any = {
    KllSketchUtils.getKllFloatsSketchN(input, prettyName)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the number of items collected in the sketch.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_double(col)) FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)), (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)) tab(col);
       5
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetNDouble(child: Expression) extends KllSketchGetNBase {
  override protected def withNewChildInternal(newChild: Expression): KllSketchGetNDouble =
    copy(child = newChild)
  override def prettyName: String = "kll_sketch_get_n_double"
  override protected def kllUtilsMethod: String = "getKllDoublesSketchN"
  override def nullSafeEval(input: Any): Any = {
    KllSketchUtils.getKllDoublesSketchN(input, prettyName)
  }
}

/** This is a base class for the above expressions to reduce boilerplate. */
abstract class KllSketchGetNBase
    extends UnaryExpression
        with ImplicitCastInputTypes {
  override def dataType: DataType = LongType
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
  override def nullIntolerant: Boolean = true

  protected def kllUtilsMethod: String

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val kllUtilsClassName = KllSketchUtils.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, input => {
      s"""
        |${ev.value} = $kllUtilsClassName.$kllUtilsMethod($input, "$prettyName");
      """.stripMargin
    })
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Merges two sketch buffers together into one.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_bigint(_FUNC_(kll_sketch_agg_bigint(col), kll_sketch_agg_bigint(col)))) > 0 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchMergeBigint(left: Expression, right: Expression) extends KllSketchMergeBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_merge_bigint"
  override def nullSafeEval(left: Any, right: Any): Any = {
    try {
      val leftBuffer = left.asInstanceOf[Array[Byte]]
      val rightBuffer = right.asInstanceOf[Array[Byte]]
      val leftSketch = KllLongsSketch.heapify(Memory.wrap(leftBuffer))
      val rightSketch = KllLongsSketch.wrap(Memory.wrap(rightBuffer))
      leftSketch.merge(rightSketch)
      leftSketch.toByteArray
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Merges two sketch buffers together into one.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_float(_FUNC_(kll_sketch_agg_float(col), kll_sketch_agg_float(col)))) > 0 FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)), (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchMergeFloat(left: Expression, right: Expression) extends KllSketchMergeBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_merge_float"
  override def nullSafeEval(left: Any, right: Any): Any = {
    try {
      val leftBuffer = left.asInstanceOf[Array[Byte]]
      val rightBuffer = right.asInstanceOf[Array[Byte]]
      val leftSketch = KllFloatsSketch.heapify(Memory.wrap(leftBuffer))
      val rightSketch = KllFloatsSketch.wrap(Memory.wrap(rightBuffer))
      leftSketch.merge(rightSketch)
      leftSketch.toByteArray
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Merges two sketch buffers together into one.
  """,
  examples = """
    Examples:
      > SELECT LENGTH(kll_sketch_to_string_double(_FUNC_(kll_sketch_agg_double(col), kll_sketch_agg_double(col)))) > 0 FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)), (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchMergeDouble(left: Expression, right: Expression) extends KllSketchMergeBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_merge_double"
  override def nullSafeEval(left: Any, right: Any): Any = {
    try {
      val leftBuffer = left.asInstanceOf[Array[Byte]]
      val rightBuffer = right.asInstanceOf[Array[Byte]]
      val leftSketch = KllDoublesSketch.heapify(Memory.wrap(leftBuffer))
      val rightSketch = KllDoublesSketch.wrap(Memory.wrap(rightBuffer))
      leftSketch.merge(rightSketch)
      leftSketch.toByteArray
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchIncompatibleMergeError(prettyName, e.getMessage)
    }
  }
}

/** This is a base class for the above expressions to reduce boilerplate. */
abstract class KllSketchMergeBase
    extends BinaryExpression
        with CodegenFallback
        with ImplicitCastInputTypes {
  override def dataType: DataType = BinaryType
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)
  override def nullIntolerant: Boolean = true
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired quantile given the input rank. The desired quantile can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_bigint(col), 0.5) > 1 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetQuantileBigint(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_quantile_bigint"
  override def outputDataType: DataType = LongType
  override protected def kllUtilsQuantileMethod: String = "getKllLongsSketchQuantile"
  override protected def kllUtilsQuantilesMethod: String = "getKllLongsSketchQuantiles"
  override protected def kllSketchGetQuantile(sketchInput: Any, rankInput: Any): Any =
    KllSketchUtils.getKllLongsSketchQuantile(sketchInput, rankInput, prettyName)
  override protected def kllSketchGetQuantiles(sketchInput: Any, ranksInput: Any): Any =
    KllSketchUtils.getKllLongsSketchQuantiles(sketchInput, ranksInput, prettyName)
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired quantile given the input rank. The desired quantile can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_float(col), 0.5) > 1 FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)), (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetQuantileFloat(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_quantile_float"
  override def outputDataType: DataType = FloatType
  override protected def kllUtilsQuantileMethod: String = "getKllFloatsSketchQuantile"
  override protected def kllUtilsQuantilesMethod: String = "getKllFloatsSketchQuantiles"
  override protected def kllSketchGetQuantile(sketchInput: Any, rankInput: Any): Any =
    KllSketchUtils.getKllFloatsSketchQuantile(sketchInput, rankInput, prettyName)
  override protected def kllSketchGetQuantiles(sketchInput: Any, ranksInput: Any): Any =
    KllSketchUtils.getKllFloatsSketchQuantiles(sketchInput, ranksInput, prettyName)
}

@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired quantile given the input rank. The desired quantile can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_double(col), 0.5) > 1 FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)), (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetQuantileDouble(left: Expression, right: Expression)
    extends KllSketchGetQuantileBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_quantile_double"
  override def outputDataType: DataType = DoubleType
  override protected def kllUtilsQuantileMethod: String = "getKllDoublesSketchQuantile"
  override protected def kllUtilsQuantilesMethod: String = "getKllDoublesSketchQuantiles"
  override protected def kllSketchGetQuantile(sketchInput: Any, rankInput: Any): Any =
    KllSketchUtils.getKllDoublesSketchQuantile(sketchInput, rankInput, prettyName)
  override protected def kllSketchGetQuantiles(sketchInput: Any, ranksInput: Any): Any =
    KllSketchUtils.getKllDoublesSketchQuantiles(sketchInput, ranksInput, prettyName)
}

/**
 * This is a base class for the above expressions to reduce boilerplate.
 * Each implementor is expected to define three methods: one to specify the output data type,
 * one to compute the quantile of an input sketch buffer given a single input rank,
 * and one to compute multiple quantiles given an array of ranks (batch API for performance).
 */
abstract class KllSketchGetQuantileBase
    extends BinaryExpression
        with ImplicitCastInputTypes {
  /** The output data type for a single value (not array) */
  protected def outputDataType: DataType

  /** The KllSketchUtils method name for single quantile computation */
  protected def kllUtilsQuantileMethod: String

  /** The KllSketchUtils method name for multiple quantiles computation */
  protected def kllUtilsQuantilesMethod: String

  /**
   * Computes a single quantile for the given rank.
   * Each subclass delegates to the appropriate KllSketchUtils method.
   */
  protected def kllSketchGetQuantile(sketchInput: Any, rankInput: Any): Any

  /**
   * Computes multiple quantiles for the given ranks array.
   * Each subclass delegates to the appropriate KllSketchUtils method.
   */
  protected def kllSketchGetQuantiles(sketchInput: Any, ranksInput: Any): Any

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val kllUtilsClassName = KllSketchUtils.getClass.getName.stripSuffix("$")

    right.dataType match {
      case ArrayType(_, _) =>
        // Array of ranks case
        nullSafeCodeGen(ctx, ev, (sketch, ranks) => {
          s"""
            |${ev.value} = (${CodeGenerator.javaType(dataType)}) $kllUtilsClassName.$kllUtilsQuantilesMethod($sketch, $ranks, "$prettyName");
          """.stripMargin
        })
      case _ =>
        // Single rank case
        val cast = outputDataType match {
          case LongType => "(Long)"
          case FloatType => "(Float)"
          case DoubleType => "(Double)"
          case _ => s"(${CodeGenerator.javaType(outputDataType)})"
        }
        nullSafeCodeGen(ctx, ev, (sketch, rank) => {
          s"""
            |${ev.value} = $cast $kllUtilsClassName.$kllUtilsQuantileMethod($sketch, $rank, "$prettyName");
          """.stripMargin
        })
    }
  }

  // The rank argument must be foldable (compile-time constant).
  override def checkInputDataTypes(): TypeCheckResult = {
    if (!right.foldable) {
      TypeCheckResult.DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("rank"),
          "inputType" -> toSQLType(right.dataType),
          "inputExpr" -> toSQLExpr(right)))
    } else {
      super.checkInputDataTypes()
    }
  }

  override def nullIntolerant: Boolean = true
  override def inputTypes: Seq[AbstractDataType] =
    Seq(
      BinaryType,
      TypeCollection(
        DoubleType,
        ArrayType(DoubleType, containsNull = false)))

  override def dataType: DataType = {
    right.dataType match {
      case ArrayType(_, _) => ArrayType(outputDataType, false)
      case _ => outputDataType
    }
  }

  override def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    rightInput match {
      case null => null
      case _: Double =>
        // Single value case
        kllSketchGetQuantile(leftInput, rightInput)
      case _: ArrayData =>
        // Array case - use batch API for better performance
        kllSketchGetQuantiles(leftInput, rightInput)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired rank given the input quantile. The desired rank can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_bigint(col), 3) > 0.3 FROM VALUES (1), (2), (3), (4), (5) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetRankBigint(left: Expression, right: Expression)
    extends KllSketchGetRankBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_rank_bigint"
  override def inputDataType: DataType = LongType
  override def kllSketchGetRank(memory: Memory, quantile: Any): Double = {
    withRankErrorHandling {
      KllLongsSketch.wrap(memory).getRank(quantile.asInstanceOf[Long])
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired rank given the input quantile. The desired rank can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_float(col), 3.0) > 0.3 FROM VALUES (CAST(1.0 AS FLOAT)), (CAST(2.0 AS FLOAT)), (CAST(3.0 AS FLOAT)), (CAST(4.0 AS FLOAT)), (CAST(5.0 AS FLOAT)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetRankFloat(left: Expression, right: Expression)
    extends KllSketchGetRankBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_rank_float"
  override def inputDataType: DataType = FloatType
  override def kllSketchGetRank(memory: Memory, quantile: Any): Double = {
    withRankErrorHandling {
      KllFloatsSketch.wrap(memory).getRank(quantile.asInstanceOf[Float])
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(left, right) - Extracts a single value from the quantiles sketch representing the
    desired rank given the input quantile. The desired rank can either be a single value
    or an array. In the latter case, the function will return an array of results of equal
    length to the input array.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(kll_sketch_agg_double(col), 3.0) > 0.3 FROM VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE)), (CAST(4.0 AS DOUBLE)), (CAST(5.0 AS DOUBLE)) tab(col);
       true
  """,
  group = "misc_funcs",
  since = "4.1.0")
case class KllSketchGetRankDouble(left: Expression, right: Expression)
    extends KllSketchGetRankBase {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
  override def prettyName: String = "kll_sketch_get_rank_double"
  override def inputDataType: DataType = DoubleType
  override def kllSketchGetRank(memory: Memory, quantile: Any): Double = {
    withRankErrorHandling {
      KllDoublesSketch.wrap(memory).getRank(quantile.asInstanceOf[Double])
    }
  }
}

/**
 * This is a base class for the above expressions to reduce boilerplate.
 * Each implementor is expected to define two methods, one to specify the input argument data type,
 * and another to compute the rank of an input sketch buffer given the input quantile.
 */
abstract class KllSketchGetRankBase
    extends BinaryExpression
        with CodegenFallback
        with ImplicitCastInputTypes {
  /**
   * Helper method to wrap rank operations with consistent error handling.
   * @param operation The operation to execute
   * @return The result of the operation
   */
  protected def withRankErrorHandling[T](operation: => T): T = {
    try {
      operation
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }

  protected def inputDataType: DataType

  /**
   * This method accepts a KLL quantiles Memory segment, wraps it with the corresponding
   * Kll*Sketch.wrap method, and then calls getRank on the result.
   * @param memory The input KLL quantiles sketch buffer to extract the rank from
   * @param quantile The input quantile to use to compute the rank
   * @return The result rank
   */
  protected def kllSketchGetRank(memory: Memory, quantile: Any): Double

  // The quantile argument must be foldable (compile-time constant).
  override def checkInputDataTypes(): TypeCheckResult = {
    if (!right.foldable) {
      TypeCheckResult.DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("quantile"),
          "inputType" -> toSQLType(right.dataType),
          "inputExpr" -> toSQLExpr(right)))
    } else {
      super.checkInputDataTypes()
    }
  }

  override def nullIntolerant: Boolean = true
  override def inputTypes: Seq[AbstractDataType] = {
    Seq(
      BinaryType,
      TypeCollection(
        inputDataType,
        ArrayType(inputDataType, containsNull = false)))
  }
  override def dataType: DataType = {
    right.dataType match {
      case ArrayType(_, _) => ArrayType(DoubleType, false)
      case _ => DoubleType
    }
  }

  override def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    val buffer: Array[Byte] = leftInput.asInstanceOf[Array[Byte]]
    val memory: Memory = Memory.wrap(buffer)

    rightInput match {
      case null => null
      case value if !value.isInstanceOf[ArrayData] =>
        // Single value case
        kllSketchGetRank(memory, value)
      case arrayData: ArrayData =>
        // Array case - use direct iteration to avoid multiple array allocations
        val numElements = arrayData.numElements()
        val results = new Array[Double](numElements)
        var i = 0
        inputDataType match {
          case LongType =>
            while (i < numElements) {
              results(i) = kllSketchGetRank(memory, arrayData.getLong(i))
              i += 1
            }
          case FloatType =>
            while (i < numElements) {
              results(i) = kllSketchGetRank(memory, arrayData.getFloat(i))
              i += 1
            }
          case DoubleType =>
            while (i < numElements) {
              results(i) = kllSketchGetRank(memory, arrayData.getDouble(i))
              i += 1
            }
        }
        new GenericArrayData(results)
    }
  }
}

object KllSketchUtils {
  /**
   * Helper method to wrap quantile operations with consistent error handling.
   * @param rankForError The rank value to include in error messages
   * @param prettyName The function name for error messages
   * @param operation The operation to execute
   * @return The result of the operation
   */
  @inline
  private def withQuantileErrorHandling[T](
      rankForError: Double,
      prettyName: String)(operation: => T): T = {
    try {
      operation
    } catch {
      case e: org.apache.datasketches.common.SketchesArgumentException =>
        if (e.getMessage.contains("normalized rank")) {
          throw QueryExecutionErrors.kllSketchInvalidQuantileRangeError(prettyName, rankForError)
        } else {
          throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
        }
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }

  @inline
  private def getSketchN(
      input: Any,
      prettyName: String,
      extractN: Memory => Long): Long = {
    try {
      val bytes = input.asInstanceOf[Array[Byte]]
      extractN(Memory.wrap(bytes))
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.kllSketchInvalidInputError(prettyName, e.getMessage)
    }
  }

  def getKllLongsSketchN(input: Any, prettyName: String): Long =
    getSketchN(input, prettyName, mem => KllLongsSketch.heapify(mem).getN())

  def getKllFloatsSketchN(input: Any, prettyName: String): Long =
    getSketchN(input, prettyName, mem => KllFloatsSketch.heapify(mem).getN())

  def getKllDoublesSketchN(input: Any, prettyName: String): Long =
    getSketchN(input, prettyName, mem => KllDoublesSketch.heapify(mem).getN())

  @inline
  private def getSketchQuantile(
      sketchInput: Any,
      rankInput: Any,
      prettyName: String,
      extractQuantile: (Memory, Double) => Any): Any = {
    val bytes = sketchInput.asInstanceOf[Array[Byte]]
    val rank = rankInput.asInstanceOf[Double]
    withQuantileErrorHandling(rank, prettyName) {
      extractQuantile(Memory.wrap(bytes), rank)
    }
  }

  @inline
  private def getSketchQuantiles(
      sketchInput: Any,
      ranksInput: Any,
      prettyName: String,
      extractQuantiles: (Memory, Array[Double]) => Array[Any]): Any = {
    val bytes = sketchInput.asInstanceOf[Array[Byte]]
    val ranks = ranksInput.asInstanceOf[ArrayData].toDoubleArray()
    val rankForError = if (ranks.length > 0) ranks(0) else 0.0
    withQuantileErrorHandling(rankForError, prettyName) {
      val results = extractQuantiles(Memory.wrap(bytes), ranks)
      new GenericArrayData(results)
    }
  }

  def getKllLongsSketchQuantile(sketchInput: Any, rankInput: Any, prettyName: String): Any =
    getSketchQuantile(sketchInput, rankInput, prettyName,
      (mem, rank) => KllLongsSketch.wrap(mem).getQuantile(rank))

  def getKllLongsSketchQuantiles(sketchInput: Any, ranksInput: Any, prettyName: String): Any =
    getSketchQuantiles(sketchInput, ranksInput, prettyName,
      (mem, ranks) => KllLongsSketch.wrap(mem).getQuantiles(ranks).map(_.asInstanceOf[Any]))

  def getKllFloatsSketchQuantile(sketchInput: Any, rankInput: Any, prettyName: String): Any =
    getSketchQuantile(sketchInput, rankInput, prettyName,
      (mem, rank) => KllFloatsSketch.wrap(mem).getQuantile(rank))

  def getKllFloatsSketchQuantiles(sketchInput: Any, ranksInput: Any, prettyName: String): Any =
    getSketchQuantiles(sketchInput, ranksInput, prettyName,
      (mem, ranks) => KllFloatsSketch.wrap(mem).getQuantiles(ranks).map(_.asInstanceOf[Any]))

  def getKllDoublesSketchQuantile(sketchInput: Any, rankInput: Any, prettyName: String): Any =
    getSketchQuantile(sketchInput, rankInput, prettyName,
      (mem, rank) => KllDoublesSketch.wrap(mem).getQuantile(rank))

  def getKllDoublesSketchQuantiles(sketchInput: Any, ranksInput: Any, prettyName: String): Any =
    getSketchQuantiles(sketchInput, ranksInput, prettyName,
      (mem, ranks) => KllDoublesSketch.wrap(mem).getQuantiles(ranks).map(_.asInstanceOf[Any]))
}
