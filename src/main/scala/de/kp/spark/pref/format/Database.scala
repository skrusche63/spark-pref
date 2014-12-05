package de.kp.spark.pref.format
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Pref project
* (https://github.com/skrusche63/spark-pref).
* 
* Spark-Pref is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Pref is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Pref. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.pref.Configuration
import de.kp.spark.pref.util.EventScoreBuilder

abstract class DBObject extends Serializable {

  val (host,port) = Configuration.redis
  val client = new RedisDB(host,port.toInt)
  
  def exists(req:ServiceRequest,topic:String):Boolean = {
    
    val k = topic + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME)
    client.exists(k)
    
  }
  
  def get(req:ServiceRequest):Dict
  
}

object Events extends DBObject {
  
  def exists(req:ServiceRequest):Boolean = super.exists(req,"event")
  
  override def get(req:ServiceRequest):Dict = {    
    /*
     * Check whether the events are already registered for this site;
     * otherwise we have to build them from a local XML file and put
     * them to the Redis database
     */
    if (exists(req) == false) {

      val eventScores = EventScoreBuilder.get
      val events = eventScores.filter(x => x.event > 0)
      
      events.foreach(x => client.addEvent(req, x.event, x.desc))
 
    }
   
    new Dict().build(client.events(req))
    
  }
  
}

object Items extends DBObject {
  
  override def get(req:ServiceRequest):Dict = new Dict().build(client.items(req))
 
  def exists(req:ServiceRequest):Boolean = super.exists(req,"item")
  def put(req:ServiceRequest,iid:String) = client.addItem(req,iid)

}

object Users extends DBObject {
  
  override def get(req:ServiceRequest):Dict = new Dict().build(client.users(req))
 
  def exists(req:ServiceRequest):Boolean = super.exists(req,"user")
  def put(req:ServiceRequest,uid:String) = client.addUser(req,uid)

}


object Ratings extends DBObject {
  
  override def get(req:ServiceRequest):Dict = null
 
  def exists(req:ServiceRequest):Boolean = super.exists(req,"rating")
  def put(req:ServiceRequest,rating:String) = client.addRating(req,rating)

}