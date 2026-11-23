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

package org.apache.spark.sql.catalyst.util

import java.security.SecureRandom
import java.util.UUID

import org.apache.spark.unsafe.types.UTF8String

/**
 * This class is used to generate time-ordered UUIDs (UUIDv7) as defined in RFC 9562,
 * section 5.7 "UUID Version 7".
 *
 * Each UUID embeds a 48-bit Unix epoch timestamp in milliseconds in its most
 * significant bits, followed by the version bits, a 12-bit counter, and 62
 * random bits. The counter is randomly initialized for every new millisecond
 * and incremented for each UUID generated within that millisecond (see RFC
 * 9562, section 6.2 "Monotonicity and Counters", method 1), so UUIDs obtained
 * from a single generator instance are monotonically increasing. If the
 * counter is exhausted within a millisecond, generation borrows the next
 * millisecond. A backwards-moving clock never rewinds the ordering.
 *
 * Ordering across generator instances, for example across Spark partitions or
 * executors, is not guaranteed.
 *
 * For the algorithm, see RFC 9562: Universally Unique IDentifiers (UUIDs),
 * section 5.7 and 6.2.
 */
class UUIDv7Generator {

  /** Version bits for UUIDv7 (0111) at their position in the most significant bits. */
  private val VersionBits: Long = 0x7000L

  /** Largest value of the 12-bit counter stored in rand_a. */
  private val MaxCounter: Int = 0xFFF

  /**
   * Upper bound (exclusive) for the random initial value of the counter. Keeping the
   * initial value in the lower half of the range leaves at least 2048 increments
   * available before the counter is exhausted within one millisecond.
   */
  private val CounterStride: Int = 0x800

  private val random = new SecureRandom()

  /** Timestamp (Unix epoch milliseconds) of the most recently generated UUID. */
  private var lastTimestamp: Long = _

  /** 12-bit counter stored in rand_a, incremented within the same millisecond. */
  private var counter: Int = _

  def getNextUUID(): UUID = {
    var timestamp = System.currentTimeMillis()
    // Never let a backwards-moving clock rewind the ordering.
    if (timestamp < lastTimestamp) {
      timestamp = lastTimestamp
    }
    if (timestamp == lastTimestamp) {
      if (counter == MaxCounter) {
        // The counter is exhausted within this millisecond, borrow the next one.
        timestamp += 1
        counter = nextCounter()
      } else {
        counter += 1
      }
    } else {
      counter = nextCounter()
    }
    lastTimestamp = timestamp

    // 48-bit timestamp | 4-bit version (0111) | 12-bit counter in rand_a
    val mostSigBits = (timestamp << 16) | VersionBits | counter
    // 2-bit variant (10) followed by 62 random bits in rand_b
    val leastSigBits = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L
    new UUID(mostSigBits, leastSigBits)
  }

  def getNextUUIDUTF8String(): UTF8String = UTF8String.fromString(getNextUUID().toString())

  private def nextCounter(): Int = random.nextInt(CounterStride)
}
