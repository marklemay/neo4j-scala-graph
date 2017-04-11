package neo4j_scala_graph

import collection.JavaConverters._
import org.neo4j.driver.v1.Record
import org.neo4j.driver.v1.types._
import org.neo4j.driver.v1.Session
import scalax.collection.Graph
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._
import scalax.collection.edge.LDiEdge // labeled directed edge
import scalax.collection.edge.Implicits._ // shortcuts
import org.neo4j.driver.v1.StatementResult


object NeoData {
  
  sealed trait NeoData
  //  a scala version of org.neo4j.driver.v1.types.Node
  case class NeoNode(id: Long, labels: Set[String], properties: Map[String, Object]) extends NeoData
  case class NeoRel(id: Long, `type`: String, properties: Map[String, Object]) extends NeoData

  def run(session: Session)(query: String): Graph[NeoNode, LDiEdge] = {
    storeRecord(session)(session.run(query))
  }

  def getNode(session: Session)(id: Long): NeoNode = {

    val out = session.run(
      raw"""MATCH (s)
       WHERE ID(s) = $id
       RETURN s""")
    val node = out.single().get("s").asNode()

    NeoNode(node.id(), node.labels().asScala.toSet, node.asMap().asScala.toMap)
  }

  // TODO still wish there was a type safe LDiEdge
  def storeRecord(session: Session)(rs: StatementResult): Graph[NeoNode, LDiEdge] = {

    var nodeIdMap = Map[Long, NeoNode]()

    //TODO: faster with the mutable graph?
    var g = Graph[NeoNode, LDiEdge]()

    for (rec <- rs.list().asScala) {
      for ((_, o) <- rec.asMap().asScala) {

        if (o.isInstanceOf[Node]) {
          val node = o.asInstanceOf[Node]

          val nn = NeoNode(node.id(), node.labels().asScala.toSet, node.asMap().asScala.toMap)
          nodeIdMap += nn.id -> nn

          g += nn

        } else if (o.isInstanceOf[Relationship]) {
          val rel = o.asInstanceOf[Relationship]

          //make sure both ends are added before the edge is added
          if (!nodeIdMap.contains(rel.startNodeId())) {
            val nn = getNode(session)(rel.startNodeId())

            nodeIdMap += nn.id -> nn
            g += nn
          }

          if (!nodeIdMap.contains(rel.endNodeId())) {
            val nn = getNode(session)(rel.endNodeId())

            nodeIdMap += nn.id -> nn
            g += nn
          }

          // assume directed from start to end, TODO: this is not a safe assumption!
          val rr = NeoRel(rel.id(), rel.`type`(), rel.asMap().asScala.toMap)

          g += (nodeIdMap(rel.startNodeId()) ~+> nodeIdMap(rel.endNodeId()))(rr)

        } else if (o.isInstanceOf[Path]) {
          //don't handle paths yet
          ???
        }
      }
    }
    g
  }

  def toDiGraph(g: Graph[NeoNode, LDiEdge]): Graph[NeoData, DiEdge] = {
    val oldnodes = g.nodes.map(_.value)
    val oldEdges = g.edges
    var newG = Graph[NeoData, DiEdge]()

    for (n <- oldnodes) {
      newG += n
    }

    for (e <- oldEdges) {
      val rel = e.label.asInstanceOf[NeoRel]

      newG += e._1.value ~> rel
      
      newG += rel ~> e._2.value
    }

    newG
  }
}