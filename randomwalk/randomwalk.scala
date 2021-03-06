package edu.gatech.cse8803.randomwalk

import edu.gatech.cse8803.model.{PatientProperty, EdgeProperty, VertexProperty}
import org.apache.spark.graphx._

object RandomWalk {

  def randomWalkOneVsAll(graph: Graph[VertexProperty, EdgeProperty], patientID: Long, numIter: Int = 100, alpha: Double = 0.15): List[Long] = {
    /** 
    Given a patient ID, compute the random walk probability w.r.t. to all other patients. 
    Return a List of patient IDs ordered by the highest to the lowest similarity.
    For ties, random order is okay
    */
    
    // This code is a modified version of Spark's PageRank runWithOptions:
    // https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala
    // The original is subject to the Apache License: http://www.apache.org/licenses/LICENSE-2.0
    
    val src: VertexId= patientID

    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute resetProb.
    // When running personalized pagerank, only the source vertex
    // has an attribute resetProb. All others are set to 0.
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) =>
        if (!(id != src)) alpha else 0.0
      }

    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      prevRankGraph = rankGraph
      val rPrb = (src: VertexId, id: VertexId) => alpha * delta(src, id)

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - alpha) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices

      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }
    val ans= rankGraph.vertices.filter(e => e._1 != patientID && e._1 <= 1000).sortBy(_._2, false)
    // ans.take(15).foreach(println)
    
    ans.map(_._1).take(10).toList
  }
}
