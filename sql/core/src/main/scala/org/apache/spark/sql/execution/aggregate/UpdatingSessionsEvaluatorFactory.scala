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

package org.apache.spark.sql.execution.joins

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.aggregate.UpdatingSessionsIterator

class UpdatingSessionsEvaluatorFactory(
    groupingExpression: Seq[Attribute],
    sessionExpression: Attribute,
    childOutput: Seq[Attribute],
    inMemoryThreshold: Int,
    spillThreshold: Int)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new UpdatingSessionsEvaluator

  private class UpdatingSessionsEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val iter = inputs(0)
      new UpdatingSessionsIterator(
        iter,
        groupingExpression,
        sessionExpression,
        childOutput,
        inMemoryThreshold,
        spillThreshold)
    }
  }
}
