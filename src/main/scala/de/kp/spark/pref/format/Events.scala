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

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisClient

import de.kp.spark.pref.Configuration

import de.kp.spark.pref.model._
import de.kp.spark.pref.util.EventScoreBuilder

import scala.collection.JavaConversions._

object Events {

  val (host,port) = Configuration.redis
  val client = RedisClient(host,port.toInt)
  
  private def buildFromRedis(req:ServiceRequest):Seq[String] = {
        
    val k = "events:" + req.data("site")
    val data = client.zrange(k, 0, -1)

    val events = if (data.size() == 0) {
      List.empty[String]
    
    } else {
      
      data.map(record => {
        /* format = timestamp:id:name */
        val Array(timestamp,id,name) = record.split(":")
        name
        
      }).toList
      
    }
    
    events
  
  }
  
  def get(req:ServiceRequest):Dict = {
    
    /*
     * Check whether the events are already registered for this site;
     * otherwise we have to build them from a local XML file and put
     * them to the Redis instance
     */
    val k = "events:" + req.data("site")
    if (client.exists(k) == false) {

      val eventScores = EventScoreBuilder.get
      val events = eventScores.filter(x => x.event > 0)
    
      val now = new java.util.Date()
      val timestamp = now.getTime()
      
      events.foreach(x => {
    
        val v = "" + timestamp + ":" + x.event + ":" + x.desc
        client.zadd(k,timestamp,v)       
      
      })

    }
   
    /**
     * Actually the user database is retrieved from
     * Redis instance
     */
    new Dict().build(buildFromRedis(req))
    
  }
  
}