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
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegionWithOrientation, ReferenceRegion }
import org.bdgenomics.adam.models.ReferenceRegionContext._
import org.bdgenomics.adam.rich.ReferenceMappingContext.ReferenceRegionReferenceMapping
import scala.reflect.ClassTag
import scala.math.max

/**
 * Functions (joins, filters, groups, counts) that are often invoked on RDD[R] where R is a
 * type which extends ReferenceRegion (and is often just ReferenceRegion directly).
 *
 * @param rdd The argument RDD
 * @param kt The type of the argument RDD must be explicit
 * @tparam Region The type of the argument RDD
 */
class RegionRDDFunctions[Region <: ReferenceRegion](rdd: RDD[Region])(implicit kt: ClassTag[Region])
    extends Serializable {

  import RegionRDDFunctions._

  private def regionToReferenceRegion(region: Region): Option[ReferenceRegion] = Option(region)

  private def expander(range: Long)(r: Region): ReferenceRegion =
    ReferenceRegion(r.referenceName, max(0, r.start - range), r.end + range)

  /**
   * Performs an 'overlap join' on the input RDD, returning pairs whose first element is a member of the
   * input RDD and whose second element is a member of the argument RDD which overlaps the first element
   * of the pair.
   *
   * @param that The argument RDD
   * @param kt2 The type of the values in the argument RDD must be explicit.
   * @tparam R2 The explicit type of the regions in the argument RDD
   * @return The RDD of pairs (f, s) where f comes from the input RDD and s comes from 'that', the argument
   *         RDD.
   */
  def joinByOverlap[R2 <: ReferenceRegion](that: RDD[R2])(implicit kt2: ClassTag[R2]): RDD[(Region, R2)] =
    RegionJoin.partitionAndJoin[Region, R2](rdd, that, r => regionToReferenceRegion(r), (r: R2) => Option(r))

  /**
   * Performs a 'range join' on the input RDD, returning pairs whose first element is a member of the
   * input RDD and whose second element is a member of the argument RDD which is within 'range' bases
   * of the first element of the pair.
   *
   * @param that The argument RDD
   * @param range A long integer; if two regions are within 'range' of each other (along the same chromosome),
   *              then they are present in the returned RDD.
   * @param kt2 The type of the values in the argument RDD must be explicit.
   * @tparam R2 The explicit type of the regions in the argument RDD
   * @return The RDD of pairs (f, s) where f comes from the input RDD and s comes from 'that', the argument
   *         RDD.
   */
  def joinWithinRange[R2 <: ReferenceRegion](that: RDD[R2], range: Long)(implicit kt2 : ClassTag[R2]): RDD[(Region, R2)] =
    rdd.keyBy(expander(range))
      .joinByOverlap(that.keyBy(r => r))
      .map(_._2)

  /**
   * Performs a 'filter by overlap' on the input RDD, returning only the subset of values in the input
   * RDD which overlap at least one region in the argument RDD.
   *
   * @param that the argument RDD
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return The subset of values from the input RDD which overlap at least one element of the
   *         argument RDD.
   */
  def filterByOverlap[R2 <: ReferenceRegion](that: RDD[R2])(implicit kt2: ClassTag[R2]): RDD[Region] =
    rdd.joinByOverlap(that).map(_._1).distinct()

  /**
   * Performs a 'filter by range' on the input RDD, returning only the subset of values in the input
   * RDD which are within a fixed number of basepairs of at least one region in the argument RDD.
   *
   * @param that the argument RDD
   * @param range a long integer; defines the window within which an input region must be of an argument
   *              region, in order to be reported in the output RDD
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return The subset of values from the input RDD which overlap at least one element of the
   *         argument RDD.
   */
  def filterWithinRange[R2 <: ReferenceRegion](that: RDD[R2], range: Long)(implicit kt2: ClassTag[R2]): RDD[Region] =
    rdd.joinWithinRange(that, range).map(_._1).distinct()

  /**
   * Performs the equivalent of a joinByOverlap followed by a groupByKey; in effect, this returns
   * an Iterable of the regions in the argument RDD that overlap a region from the input RDD, _for each_
   * region of the input RDD.
   *
   * @param that the argument RDD
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return An RDD of pairs, where the first element is a region from the input RDD and the second
   *         is an Iterable of regions from the argument RDD which overlap the first element.
   */
  def groupByOverlap[R2 <: ReferenceRegion](that: RDD[R2])(implicit kt2: ClassTag[R2]): RDD[(Region, Iterable[R2])] =
    rdd.joinByOverlap(that).groupByKey()

  /**
   * Performs the equivalent of a joinByRange followed by a groupByKey; in effect, this returns
   * an Iterable of the regions in the argument RDD that overlap a region from the input RDD, _for each_
   * region of the input RDD, where 'overlap' is expanded to include a range around each region
   * in the argument RDD.
   *
   * @param that the argument RDD
   * @param range a long integer; pairs of regions within this range of each other are considered to
   *              overlap
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return An RDD of pairs, where the first element is a region from the input RDD and the second
   *         is an Iterable of regions from the argument RDD which overlaps (within the range) the
   *         first element.
   */
  def groupByWithinRange[R2 <: ReferenceRegion](that: RDD[R2], range: Long)(implicit kt2: ClassTag[R2]): RDD[(Region, Iterable[R2])] =
    rdd.joinWithinRange(that, range).groupByKey()

  /**
   * Calculates windows of a fixed width that tile the entire genome, and then counts how many
   * elements of the input RDD overlap each window.
   *
   * @param seqDict The dictionary which defines the sequences and lengths of the genome, used for
   *                defining the set of windows over which counts are to be calculated.
   * @param windowSize The size of the window (in base-pairs)
   * @return An RDD of (window, count) pairs
   */
  def windowCounts(seqDict: SequenceDictionary, windowSize: Long): RDD[(ReferenceRegion, Int)] =
    new Coverage(windowSize).getAllWindows(rdd.sparkContext, seqDict)
      .joinByOverlap(rdd)
      .groupByKey()
      .map {
        case (window: ReferenceRegion, values: Iterable[Region]) =>
          (window, values.size)
      }

}

class OrientedRegionRDDFunctions[Region <: ReferenceRegionWithOrientation](rdd: RDD[Region])(implicit kt: ClassTag[Region]) extends Serializable {

  def extend(targetLength: Long): RDD[ReferenceRegionWithOrientation] =
    rdd.map(r => r.extend(targetLength))
}

class RegionKeyedRDDFunctions[R <: ReferenceRegion, V](rdd: RDD[(R, V)])(implicit rt: ClassTag[R]) extends Serializable {

  import RegionRDDFunctions._

  def joinByOverlap[R2 <: ReferenceRegion, V2](that: RDD[(R2, V2)]): RDD[((R, R2), (V, V2))] =
    RegionJoin.partitionAndJoin(rdd, that, (p: (R, V)) => Option(p._1), (p2: (R2, V2)) => Option(p2._1))
      .map {
        case pp: ((R, V), (R2, V2)) => ((pp._1._1, pp._2._1), (pp._1._2, pp._2._2))
      }

  def filterWithinRange[R2 <: ReferenceRegion, V2](range: Long, that: RDD[(R2, V2)]): RDD[(R, V)] =
    rdd.keyBy(r => ReferenceRegion(r._1.referenceName, max(0, r._1.start - range), r._1.end + range))
      .joinByOverlap(that.keyBy(r => r._1.asInstanceOf[ReferenceRegion]))
      .map(_._2._1).distinct()
}

object RegionRDDFunctions extends Serializable {
  implicit def rddToRegionRDDFunctions[R <: ReferenceRegion](rdd: RDD[R])(implicit kt: ClassTag[R]): RegionRDDFunctions[R] =
    new RegionRDDFunctions[R](rdd)
  implicit def rddToOrientedRegionRDDFunctions[R <: ReferenceRegionWithOrientation](rdd: RDD[R])(implicit kt: ClassTag[R]): OrientedRegionRDDFunctions[R] =
    new OrientedRegionRDDFunctions[R](rdd)
  implicit def rddToRegionKeyedRDDFunctions[R <: ReferenceRegion, V](rdd: RDD[(R, V)])(implicit kt: ClassTag[R]): RegionKeyedRDDFunctions[R, V] =
    new RegionKeyedRDDFunctions[R, V](rdd)
}
