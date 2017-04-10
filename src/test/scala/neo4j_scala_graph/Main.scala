package neo4j_scala_graph

import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.AuthTokens
import NeoData._

object Main {



  def main(args: Array[String]): Unit = {

    val driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "n"))

    val session = driver.session();

    val g = toDiGraph(run(session)("MATCH (n) OPTIONAL MATCH (n)-[r]-() RETURN n,r LIMIT 10;"))

    println(g.mkString("\n"))
    
  }
}