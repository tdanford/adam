/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd

import org.apache.spark.SparkContext._
import org.bdgenomics.adam.models.{ ReferenceRegionContext, ReferenceRegion }
import org.bdgenomics.adam.util.SparkFunSuite
import ADAMContext._
import ReferenceRegionContext._
import RegionRDDFunctions._

class RegionRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("joinByOverlap on an overlapping pair returns one pair") {
    val r1: ReferenceRegion = ReferenceRegion("chr1", 1000, 2000)
    val r2: ReferenceRegion = ReferenceRegion("chr1", 1500, 3000)

    val rdd1 = sc.parallelize(Seq(r1))
    val rdd2 = sc.parallelize(Seq(r2))

    val joined = rdd1.joinByOverlap(rdd2)

    assert(joined.collect() === Array((r1, r2)))
  }

  sparkTest("joinByRange on a nearby pair returns one pair") {
    val r1: ReferenceRegion = ReferenceRegion("chr1", 1000, 2000)
    val r2: ReferenceRegion = ReferenceRegion("chr1", 3000, 4000)

    val rdd1 = sc.parallelize(Seq(r1))
    val rdd2 = sc.parallelize(Seq(r2))

    val joined = rdd1.joinWithinRange(rdd2, 1001L)
    assert(joined.collect() === Array((r1, r2)))

    val joined2 = rdd1.joinWithinRange(rdd2, 500L)
    assert(joined2.collect().isEmpty)
  }
}

class RegionKeyedRDDFunctionsSuite extends SparkFunSuite {
  sparkTest("joinByOverlap on an overlapping pair returns one pair") {
    val r1: ReferenceRegion = ReferenceRegion("chr1", 100, 200)
    val r2: ReferenceRegion = ReferenceRegion("chr1", 150, 300)

    val rdd1 = sc.parallelize(Seq(r1 -> "A"))
    val rdd2 = sc.parallelize(Seq(r2 -> "B"))

    val joined = rdd1.joinByOverlap(rdd2)

    assert(joined.collect() === Array(((r1, r2), ("A", "B"))))

    // Make sure that we're checking the referenceName
    val r3: ReferenceRegion = ReferenceRegion("chr2", 150, 300)
    val rdd3 = sc.parallelize(Seq(r3 -> "C"))

    assert(rdd1.joinByOverlap(rdd3).collect().isEmpty)
  }

  sparkTest("groupByOverlap produces the right groupings on a small input set") {
    val r1_base: ReferenceRegion = ReferenceRegion("chr1", 125, 126)
    val r2_base: ReferenceRegion = ReferenceRegion("chr1", 400, 401)

    val rA: ReferenceRegion = ReferenceRegion("chr1", 100, 200)
    val rB: ReferenceRegion = ReferenceRegion("chr1", 120, 300)
    val rC: ReferenceRegion = ReferenceRegion("chr1", 350, 450)
    val rD: ReferenceRegion = ReferenceRegion("chr2", 0, 1000)

    val rdd1 = sc.parallelize(Seq(r1_base -> "A", r2_base -> "B"))
    val rdd2 = sc.parallelize(Seq(rA -> "a", rB -> "b", rC -> "c", rD -> "d"))

    val grouped = rdd1.groupByOverlap(rdd2)
    assert(grouped.collect() === Array(("A", Seq("a", "b")), ("B", Seq("c"))))
  }
}
