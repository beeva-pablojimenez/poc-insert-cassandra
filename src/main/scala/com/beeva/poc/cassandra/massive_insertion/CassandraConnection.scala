package com.beeva.poc.cassandra.massive_insertion

import java.net.InetSocketAddress
import com.datastax.driver.core.Session
import com.typesafe.config.Config
import akka.actor.Actor
import com.datastax.driver.core.Cluster
import akka.actor.ActorLogging


//conexion a base de datos cassandra
class CassandraConnection(config: Config) extends Actor with ActorLogging {
  val nodes = scala.collection.JavaConversions.asScalaBuffer(config.getStringList("nodes")).toList
  val port = config.getInt("port")
  val createKeyspace = config.getString("create")
  val session: Session = {
    var list = new java.util.ArrayList[InetSocketAddress]()
    for (n <- nodes) {
      list.add(new java.net.InetSocketAddress(n, port))
    }
    val cluster = Cluster.builder().addContactPointsWithPorts(list).build()
    val session = cluster.connect();
    session.execute(createKeyspace)
    session
  }
  
  // ejecuta sentencias insert de modo asincrono no bloqueante
  def insert(insert: String) {
    this.session.executeAsync(insert)
  }
  
  def receive ={
    case Messages.Insert(statement) => 
      insert(statement)
      sender ! Messages.InsertDone
    case _ => 
      log.debug("Message not valid")
  }
  
  override def postStop():Unit = {
    session.close()
  }
}