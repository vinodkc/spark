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

import org.apache.spark.SparkFunSuite

class UUIDv7GeneratorSuite extends SparkFunSuite {
  test("UUIDv7Generator should generate version 7, variant 2 UUIDs") {
    val generator = new UUIDv7Generator
    for (_ <- 0 to 100) {
      val uuid = generator.getNextUUID()
      assert(uuid.version() == 7)
      assert(uuid.variant() == 2)
    }
  }

  test("UUIDs from a single UUIDv7Generator should be monotonically increasing and unique") {
    val generator = new UUIDv7Generator
    val before = System.currentTimeMillis()
    // Generate enough UUIDs to span multiple milliseconds and counters.
    val uuids = for (_ <- 0 until 10000) yield generator.getNextUUID()
    val after = System.currentTimeMillis()

    assert(uuids.distinct.size == 10000)
    assert(uuids == uuids.sorted)
    // The embedded 48-bit timestamps must fall within the generation window,
    // allowing for milliseconds borrowed on counter exhaustion.
    uuids.foreach { uuid =>
      val timestamp = uuid.getMostSignificantBits >>> 16
      assert(timestamp >= before && timestamp <= after + 1000)
    }
  }

  test("Get UTF8String UUID") {
    val generator = new UUIDv7Generator
    val utf8StringUUID = generator.getNextUUIDUTF8String()
    val uuid = java.util.UUID.fromString(utf8StringUUID.toString)
    assert(uuid.version() == 7 && uuid.variant() == 2 && utf8StringUUID.toString == uuid.toString)
  }
}
