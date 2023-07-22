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

package org.apache.spark.sql.execution

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.metric.SQLMetric

class GenerateEvaluatorFactory(
    childOutput: Seq[Attribute],
    sameOutputSet: Boolean,
    requiredChildOutput: Seq[Attribute],
    output: Seq[Attribute],
    boundGenerator: Generator,
    schemaLength: Int,
    outer: Boolean,
    numOutputRows: SQLMetric)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new GenerateEvaluator

  private class GenerateEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      // boundGenerator.terminate() should be triggered after all of the rows in the partition
      val iter = inputs.head
      val generatorNullRow = new GenericInternalRow(schemaLength)
      val rows = if (requiredChildOutput.nonEmpty) {

        val pruneChildForResult: InternalRow => InternalRow =
          if (sameOutputSet) {
            identity
          } else {
            UnsafeProjection.create(requiredChildOutput, childOutput)
          }

        val joinedRow = new JoinedRow
        iter.flatMap { row =>
          // we should always set the left (required child output)
          joinedRow.withLeft(pruneChildForResult(row))
          val outputRows = boundGenerator.eval(row)
          if (outer && outputRows.isEmpty) {
            joinedRow.withRight(generatorNullRow) :: Nil
          } else {
            outputRows.toIterator.map(joinedRow.withRight)
          }
        } ++ LazyIterator(() => boundGenerator.terminate()).map { row =>
          // we leave the left side as the last element of its child output
          // keep it the same as Hive does
          joinedRow.withRight(row)
        }
      } else {
        iter.flatMap { row =>
          val outputRows = boundGenerator.eval(row)
          if (outer && outputRows.isEmpty) {
            Seq(generatorNullRow)
          } else {
            outputRows
          }
        } ++ LazyIterator(() => boundGenerator.terminate())
      }

      // Convert the rows to unsafe rows.
      val proj = UnsafeProjection.create(output, output)
      proj.initialize(partitionIndex)
      rows.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }
}
