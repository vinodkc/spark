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
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow

class CollectMetricsEvaluatorFactory(collector: AggregatingAccumulator)
    extends PartitionEvaluatorFactory[InternalRow, InternalRow] {
  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new CollectMetricsEvaluator

  private class CollectMetricsEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val rows = inputs.head
      // Only publish the value of the accumulator when the task has completed. This is done by
      // updating a task local accumulator ('updater') which will be merged with the actual
      // accumulator as soon as the task completes. This avoids the following problems during the
      // heartbeat:
      // - Correctness issues due to partially completed/visible updates.
      // - Performance issues due to excessive serialization.
      val updater = collector.copyAndReset()
      TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        if (collector.isZero) {
          collector.setState(updater)
        } else {
          collector.merge(updater)
        }
      }

      rows.map { r =>
        updater.add(r)
        r
      }
    }
  }
}
