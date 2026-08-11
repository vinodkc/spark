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

package org.apache.spark

import java.io.StringWriter
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.deploy.history.BasicEventFilter
import org.apache.spark.internal.config.LONG_RDD_IDS_ENABLED
import org.apache.spark.scheduler.SparkListenerUnpersistRDD
import org.apache.spark.storage._
import org.apache.spark.util.JsonProtocol

/**
 * SPARK-41246: Systematic coverage of Long RDD IDs across every touched component.
 *
 * Scenarios per component:
 *   A) flag=false, id within Int range      (baseline - must not regress)
 *   B) flag=false, id at/after Int.MaxValue (overflow -> IllegalStateException)
 *   C) flag=true,  id within Int range      (early IDs on the 64-bit counter)
 *   D) flag=true,  id crossing Int.MaxValue (ids > 2^31-1, the main fix)
 */
class LongRddIdSuite extends SparkFunSuite with LocalSparkContext {

  // -- helpers --------------------------------------------------------------

  private def withSc(flag: Boolean)(f: SparkContext => Unit): Unit = {
    sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("LongRddIdSuite")
        .set(LONG_RDD_IDS_ENABLED, flag))
    try f(sc) finally { sc.stop(); sc = null }
  }

  private def forceIntCounter(sc: SparkContext, value: Int): Unit = {
    val f = classOf[SparkContext].getDeclaredField("nextRddId")
    f.setAccessible(true)
    f.get(sc).asInstanceOf[AtomicInteger].set(value)
  }

  private def forceLongCounter(sc: SparkContext, value: Long): Unit = {
    val f = classOf[SparkContext].getDeclaredField("nextLongRddId")
    f.setAccessible(true)
    f.get(sc).asInstanceOf[AtomicLong].set(value)
  }

  // ==========================================================================
  // 1. SparkContext / RDD.id + RDD.longId
  // ==========================================================================

  test("SparkContext/RDD - A: flag=false, id within Int range") {
    withSc(flag = false) { sc =>
      val rdd = sc.parallelize(1 to 2)
      assert(rdd.longId >= 0L)
      assert(rdd.longId <= Int.MaxValue.toLong)
      assert(rdd.id === rdd.longId.toInt)
    }
  }

  test("SparkContext/RDD - B: flag=false, last valid id then overflow throws") {
    withSc(flag = false) { sc =>
      forceIntCounter(sc, Int.MaxValue)
      val rddAtMax = sc.parallelize(1 to 2)
      assert(rddAtMax.longId === Int.MaxValue.toLong)
      assert(rddAtMax.id === Int.MaxValue)

      val ex = intercept[IllegalStateException] { sc.parallelize(1 to 2) }
      assert(ex.getMessage.contains(LONG_RDD_IDS_ENABLED.key))
    }
  }

  test("SparkContext/RDD - C: flag=true, id within Int range") {
    withSc(flag = true) { sc =>
      val rdd = sc.parallelize(1 to 2)
      assert(rdd.longId >= 0L)
      assert(rdd.id === -1)
    }
  }

  test("SparkContext/RDD - D: flag=true, longId crosses Int.MaxValue without overflow") {
    withSc(flag = true) { sc =>
      forceLongCounter(sc, Int.MaxValue.toLong)
      val rddAtMax = sc.parallelize(1 to 2)
      val rddOver = sc.parallelize(1 to 2)
      assert(rddAtMax.longId === Int.MaxValue.toLong)
      assert(rddOver.longId === Int.MaxValue + 1L)
      assert(rddOver.longId > 0)
      assert(rddOver.id === -1)
      assert(Set(rddAtMax.longId, rddOver.longId).size === 2)
    }
  }

  // ==========================================================================
  // 2. SparkContext.persistentRdds (Long-keyed map)
  // ==========================================================================

  test("persistentRdds - A: flag=false, cache/unpersist within Int range") {
    withSc(flag = false) { sc =>
      val rdd = sc.parallelize(1 to 2).cache()
      assert(sc.persistentRdds.contains(rdd.longId))
      rdd.unpersist(blocking = true)
      assert(!sc.persistentRdds.contains(rdd.longId))
    }
  }

  test("persistentRdds - C: flag=true, cache/unpersist within Int range") {
    withSc(flag = true) { sc =>
      val rdd = sc.parallelize(1 to 2).cache()
      assert(sc.persistentRdds.contains(rdd.longId))
      rdd.unpersist(blocking = true)
      assert(!sc.persistentRdds.contains(rdd.longId))
    }
  }

  test("persistentRdds - D: flag=true, cache/unpersist with id beyond Int.MaxValue") {
    withSc(flag = true) { sc =>
      forceLongCounter(sc, Int.MaxValue + 1L)
      val rdd = sc.parallelize(1 to 2).cache()
      assert(rdd.longId === Int.MaxValue + 1L)
      assert(sc.persistentRdds.contains(rdd.longId))
      rdd.unpersist(blocking = true)
      assert(!sc.persistentRdds.contains(rdd.longId))
    }
  }

  // ==========================================================================
  // 3. StorageUtils._rddBlocks / diskUsedByRdd
  // ==========================================================================

  private def makeStorageStatus(): StorageStatus =
    new StorageStatus(BlockManagerId("exec-1", "host", 1), 1000L, Some(1000L), Some(0L))

  test("StorageUtils - A: addBlock/diskUsedByRdd within Int range") {
    val st = makeStorageStatus()
    val blockId = RDDBlockId(3, 0)
    st.addBlock(blockId, BlockStatus(StorageLevel.DISK_ONLY, 0L, 200L))
    assert(st.diskUsedByRdd(3L) === 200L)
    assert(st.diskUsedByRdd(4L) === 0L)
  }

  test("StorageUtils - A: getBlock within Int range") {
    val st = makeStorageStatus()
    val blockId = RDDBlockId(7, 1)
    st.addBlock(blockId, BlockStatus(StorageLevel.MEMORY_ONLY, 512L, 0L))
    assert(st.getBlock(blockId).isDefined)
  }

  test("StorageUtils - C: addBlock/diskUsedByRdd at Int.MaxValue boundary") {
    val st = makeStorageStatus()
    val blockId = RDDBlockId(Int.MaxValue.toLong, 0)
    st.addBlock(blockId, BlockStatus(StorageLevel.DISK_ONLY, 0L, 400L))
    assert(st.diskUsedByRdd(Int.MaxValue.toLong) === 400L)
    assert(st.diskUsedByRdd(Int.MaxValue.toLong - 1L) === 0L)
  }

  test("StorageUtils - D: addBlock/diskUsedByRdd beyond Int.MaxValue") {
    val bigId = Int.MaxValue.toLong + 1L
    val st = makeStorageStatus()
    val blockId = RDDBlockId(bigId, 0)
    st.addBlock(blockId, BlockStatus(StorageLevel.DISK_ONLY, 0L, 500L))
    assert(st.diskUsedByRdd(bigId) === 500L)
    assert(st.diskUsedByRdd(bigId - 1L) === 0L)
  }

  test("StorageUtils - A: removing block reduces diskUsed") {
    val st = makeStorageStatus()
    val blockId = RDDBlockId(5, 0)
    st.addBlock(blockId, BlockStatus(StorageLevel.DISK_ONLY, 0L, 300L))
    assert(st.diskUsedByRdd(5L) === 300L)
    st.addBlock(blockId, BlockStatus(StorageLevel.NONE, 0L, 0L))
    assert(st.diskUsedByRdd(5L) === 0L)
  }

  // ==========================================================================
  // 4. SparkListenerUnpersistRDD - rddId field is Long
  // ==========================================================================

  test("SparkListenerUnpersistRDD - A: rddId within Int range") {
    val ev = SparkListenerUnpersistRDD(42L)
    assert(ev.rddId === 42L)
  }

  test("SparkListenerUnpersistRDD - C: rddId at Int.MaxValue") {
    val ev = SparkListenerUnpersistRDD(Int.MaxValue.toLong)
    assert(ev.rddId === Int.MaxValue.toLong)
  }

  test("SparkListenerUnpersistRDD - D: rddId beyond Int.MaxValue") {
    val bigId = Int.MaxValue.toLong + 99L
    val ev = SparkListenerUnpersistRDD(bigId)
    assert(ev.rddId === bigId)
  }

  // ==========================================================================
  // 5. JsonProtocol - unpersistRDDToJson / unpersistRDDFromJson
  // ==========================================================================

  private val mapper = new ObjectMapper()

  private def roundTripUnpersist(rddId: Long): SparkListenerUnpersistRDD = {
    val sw = new StringWriter
    val g = mapper.createGenerator(sw)
    JsonProtocol.unpersistRDDToJson(SparkListenerUnpersistRDD(rddId), g)
    g.flush()
    JsonProtocol.unpersistRDDFromJson(mapper.readTree(sw.toString))
  }

  test("JsonProtocol unpersistRDD - A: round-trip within Int range") {
    assert(roundTripUnpersist(12345L).rddId === 12345L)
  }

  test("JsonProtocol unpersistRDD - C: round-trip at Int.MaxValue") {
    assert(roundTripUnpersist(Int.MaxValue.toLong).rddId === Int.MaxValue.toLong)
  }

  test("JsonProtocol unpersistRDD - D: round-trip beyond Int.MaxValue") {
    val bigId = Int.MaxValue.toLong + 1L
    assert(roundTripUnpersist(bigId).rddId === bigId)
  }

  test("JsonProtocol unpersistRDD - backward compat: old Int-valued JSON parses as Long") {
    val json = mapper.readTree("""{"Event":"SparkListenerUnpersistRDD","RDD ID":999}""")
    assert(JsonProtocol.unpersistRDDFromJson(json).rddId === 999L)
  }

  // ==========================================================================
  // 6. BasicEventFilter - acceptFn for SparkListenerUnpersistRDD
  // ==========================================================================

  private def filterWithLiveRDDs(ids: Set[Long]): BasicEventFilter =
    new BasicEventFilter(null, Set.empty, Set.empty, Set.empty, ids, Set.empty)

  test("BasicEventFilter - A: within Int range accepts event for live RDD") {
    val accept = filterWithLiveRDDs(Set(10L)).acceptFn().lift
    assert(accept(SparkListenerUnpersistRDD(10L)) === Some(true))
    assert(accept(SparkListenerUnpersistRDD(99L)) === Some(false))
  }

  test("BasicEventFilter - C: at Int.MaxValue matches live RDD") {
    val accept = filterWithLiveRDDs(Set(Int.MaxValue.toLong)).acceptFn().lift
    assert(accept(SparkListenerUnpersistRDD(Int.MaxValue.toLong)) === Some(true))
  }

  test("BasicEventFilter - D: beyond Int.MaxValue accepted when in live set") {
    val bigId = Int.MaxValue.toLong + 1L
    val accept = filterWithLiveRDDs(Set(bigId)).acceptFn().lift
    assert(accept(SparkListenerUnpersistRDD(bigId)) === Some(true))
    assert(accept(SparkListenerUnpersistRDD(bigId + 1L)) === Some(false))
  }

  // ==========================================================================
  // 7. ContextCleaner.doCleanupRDD
  // ==========================================================================

  test("ContextCleaner.doCleanupRDD - A: flag=false, cleans up within Int range") {
    withSc(flag = false) { sc =>
      val rdd = sc.parallelize(1 to 2).cache()
      val rddId = rdd.longId
      assert(sc.persistentRdds.contains(rddId))
      sc.cleaner.foreach(_.doCleanupRDD(rddId, blocking = true))
      assert(!sc.persistentRdds.contains(rddId))
    }
  }

  test("ContextCleaner.doCleanupRDD - C: flag=true, cleans up within Int range") {
    withSc(flag = true) { sc =>
      val rdd = sc.parallelize(1 to 2).cache()
      val rddId = rdd.longId
      sc.cleaner.foreach(_.doCleanupRDD(rddId, blocking = true))
      assert(!sc.persistentRdds.contains(rddId))
    }
  }

  test("ContextCleaner.doCleanupRDD - D: flag=true, cleans up beyond Int.MaxValue") {
    withSc(flag = true) { sc =>
      forceLongCounter(sc, Int.MaxValue + 1L)
      val rdd = sc.parallelize(1 to 2).cache()
      assert(rdd.longId === Int.MaxValue + 1L)
      val rddId = rdd.longId
      assert(sc.persistentRdds.contains(rddId))
      sc.cleaner.foreach(_.doCleanupRDD(rddId, blocking = true))
      assert(!sc.persistentRdds.contains(rddId))
    }
  }

  // ==========================================================================
  // 8. BlockManagerMessages.RemoveRdd
  // ==========================================================================

  test("BlockManagerMessages.RemoveRdd - A: carries Int-range rddId as Long") {
    val msg = BlockManagerMessages.RemoveRdd(42L)
    assert(msg.rddId === 42L)
  }

  test("BlockManagerMessages.RemoveRdd - C: carries Int.MaxValue as Long") {
    val msg = BlockManagerMessages.RemoveRdd(Int.MaxValue.toLong)
    assert(msg.rddId === Int.MaxValue.toLong)
  }

  test("BlockManagerMessages.RemoveRdd - D: carries beyond-Int rddId as Long") {
    val bigId = Int.MaxValue.toLong + 1L
    val msg = BlockManagerMessages.RemoveRdd(bigId)
    assert(msg.rddId === bigId)
  }
}
