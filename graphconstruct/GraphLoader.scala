/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.graphconstruct

import edu.gatech.cse8803.model._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object GraphLoader {
  /** Generate Bipartite Graph using RDDs
    *
    * @input: RDDs for Patient, LabResult, Medication, and Diagnostic
    * @return: Constructed Graph
    *
    * */
  def load(patients: RDD[PatientProperty], labResults: RDD[LabResult],
           medications: RDD[Medication], diagnostics: RDD[Diagnostic]): Graph[VertexProperty, EdgeProperty] = {

    /** HINT: See Example of Making Patient Vertices Below */
    val vertexPatient: RDD[(VertexId, VertexProperty)] = patients
      .map(patient => (patient.patientID.toLong, patient.asInstanceOf[VertexProperty]))
      
    val num_patients= patients.map(p => p.patientID).max().toLong+2
    val idxDiagnostics= diagnostics.map(d => d.icd9code).distinct.zipWithIndex

    val vertexDiag: RDD[(VertexId, VertexProperty)]= idxDiagnostics
      .map{case(code,idx) => (idx+num_patients, DiagnosticProperty(code))}.asInstanceOf[RDD[(VertexId, VertexProperty)]]
      
    val num_diagnostics= idxDiagnostics.map{case(code,idx) => idx+num_patients}.max().toLong+1
    val idxLabs= labResults.map(r => r.labName).distinct.zipWithIndex
      
    val vertexLab: RDD[(VertexId, VertexProperty)]= idxLabs
      .map{case(name,idx) => (idx+num_diagnostics, LabResultProperty(name))}.asInstanceOf[RDD[(VertexId, VertexProperty)]]
 
    val num_labs= idxLabs.map{case(name,idx) => idx+num_diagnostics}.max().toLong+1
    val idxMedications= medications.map(m => m.medicine).distinct.zipWithIndex
      
    val vertexMed: RDD[(VertexId, VertexProperty)]= idxMedications
      .map{case(med,idx) => (idx+num_labs, MedicationProperty(med))}.asInstanceOf[RDD[(VertexId, VertexProperty)]]
    
    // vertexPatient.foreach(println)
    // vertexDiag.foreach(println)
    // vertexLab.foreach(println)
    // vertexMed.foreach(println)
      
    /** HINT: See Example of Making PatientPatient Edges Below */
    val sc= diagnostics.sparkContext
    val scDiag= sc.broadcast(vertexDiag.collect.toMap.map{_.swap})
    val scLab= sc.broadcast(vertexLab.collect.toMap.map{_.swap})
    val scMed= sc.broadcast(vertexMed.collect.toMap.map{_.swap})

    val edgePatientDiag:RDD[Edge[EdgeProperty]]= diagnostics.map(d => ((d.patientID, d.icd9code), d.date, d.sequence)).keyBy(d => d._1).reduceByKey((x,y) => if (x._2 > y._2) x else y)
      .map(d => (d._1._1, d._1._2, d._2._2, d._2._3))
      .map(d => Edge(d._1.toLong, scDiag.value(DiagnosticProperty(d._2)).toLong, PatientDiagnosticEdgeProperty(Diagnostic(d._1, d._3, d._2, d._4))))
    val biDiag= edgePatientDiag.map(d => Edge(d.dstId, d.srcId, d.attr)) // bidirectional Edges
    
    val edgePatientLab:RDD[Edge[EdgeProperty]]= labResults.map(d => ((d.patientID, d.labName), d.date, d.value)).keyBy(d => d._1).reduceByKey((x,y) => if (x._2 > y._2) x else y)
      .map(d => (d._1._1, d._1._2, d._2._2, d._2._3))
      .map(d => Edge(d._1.toLong, scLab.value(LabResultProperty(d._2)).toLong, PatientLabEdgeProperty(LabResult(d._1, d._3, d._2, d._4))))
    val biLab= edgePatientLab.map(d => Edge(d.dstId, d.srcId, d.attr)) // bidirectional Edges

    val edgePatientMed:RDD[Edge[EdgeProperty]]= medications.map(d => ((d.patientID, d.medicine), d.date)).keyBy(d => d._1).reduceByKey((x,y) => if (x._2 > y._2) x else y)
      .map(d => (d._1._1, d._1._2, d._2._2))
      .map(d => Edge(d._1.toLong, scMed.value(MedicationProperty(d._2)).toLong, PatientMedicationEdgeProperty(Medication(d._1, d._3, d._2))))
    val biMed= edgePatientMed.map(d => Edge(d.dstId, d.srcId, d.attr)) // bidirectional Edges
    
    // Making Graph
    val v= sc.union(vertexPatient,vertexDiag,vertexLab,vertexMed)
    val e= sc.union(edgePatientDiag,biDiag,edgePatientLab,biLab,edgePatientMed,biMed)
    
    val graph: Graph[VertexProperty, EdgeProperty] = Graph(v,e)
    graph
  }
}
