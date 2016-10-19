/**

students: please put your implementation in this file!
  **/
package edu.gatech.cse8803.jaccard

import edu.gatech.cse8803.model._
import edu.gatech.cse8803.model.{EdgeProperty, VertexProperty}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Jaccard {

  def jaccardSimilarityOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long): List[Long] = {
    /** 
    Given a patient ID, compute the Jaccard similarity w.r.t. to all other patients. 
    Return a List of top 10 patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay. The given patientID should be excluded from the result.
    */
    val tE= graph.collectNeighborIds(EdgeDirection.Out)
    tE.cache
    val tN= tE.filter(e => e._1 != patientID && e._1 <= 1000) // patientIDs are between 1 and 1000 inclusive
    val tP= tE.filter(e => e._1==patientID).flatMap(e => e._2).collect.toSet // create a Set of vertexIDs for the given patientID
    tN.cache
    val tNP= tN.map(e => (e._1, jaccard(tP,e._2.toSet))).sortBy(_._2, false) // jaccard the given patientID's vertexIDs with each other patientID's vertexIDs
    // tNP.foreach(println)

    val ans= tNP.take(10)
    // ans.foreach(println)
    
    tE.unpersist(blocking=false)
    tN.unpersist(blocking=false)
    ans.map(e => e._1).toList
  }

  def jaccardSimilarityAllPatients(graph: Graph[VertexProperty, EdgeProperty]): RDD[(Long, Long, Double)] = {
    /**
    Given a patient, med, diag, lab graph, calculate pairwise similarity between all
    patients. Return a RDD of (patient-1-id, patient-2-id, similarity) where 
    patient-1-id < patient-2-id to avoid duplications
    */
    val tE= graph.collectNeighborIds(EdgeDirection.Out)
    val tN= tE.filter(e => e._1 <= 1000) // patientIDs are between 1 and 1000 inclusive
    tN.cache
    // cartesian returns all pairs of elements in two RDDs
    val ans= tN.cartesian(tN).filter(e => e._1._1 < e._2._1).map(e => (e._1._1, e._2._1, jaccard(e._1._2.toSet, e._2._2.toSet)))
    // ans.foreach(println)
    tN.unpersist(blocking=false)
    ans
  }

  def jaccard[A](a: Set[A], b: Set[A]): Double = {
    /** 
    Given two sets, compute its Jaccard similarity and return its result.
    */
    if (a.isEmpty || b.isEmpty){return 0.0} // either A or B is empty, jaccard is 0.0
    a.intersect(b).size/(a.union(b).size.toDouble) // |A intersect B|/ |A union B|
  }
}
