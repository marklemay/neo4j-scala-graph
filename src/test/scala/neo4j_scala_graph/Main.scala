package neo4j_scala_graph

import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.AuthTokens
import NeoData._

object Main {

  def main(args: Array[String]): Unit = {

    val driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "n"))

    val session = driver.session();

    val nodes = fullGraph(session)

    val Some(cycle) = nodes.findCycle
    println(cycle.mkString("\n"))
  }
}