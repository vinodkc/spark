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

class ExpandEvaluatorFactory(
    projections: Seq[Seq[Expression]],
    childOutput: Seq[Attribute],
    numOutputRows: SQLMetric)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new ExpandEvaluator

  private class ExpandEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {

    private[this] val projection =
      (exprs: Seq[Expression]) => UnsafeProjection.create(exprs, childOutput)

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val iter = inputs.head
      val groups = projections.map(projection).toArray
      new Iterator[InternalRow] {
        private[this] var result: InternalRow = _
        private[this] var idx = -1 // -1 means the initial state
        private[this] var input: InternalRow = _

        override final def hasNext: Boolean = (-1 < idx && idx < groups.length) || iter.hasNext

        override final def next(): InternalRow = {
          if (idx <= 0) {
            // in the initial (-1) or beginning(0) of a new input row, fetch the next input tuple
            input = iter.next()
            idx = 0
          }

          result = groups(idx)(input)
          idx += 1

          if (idx == groups.length && iter.hasNext) {
            idx = 0
          }

          numOutputRows += 1
          result
        }
      }
    }
  }
}
