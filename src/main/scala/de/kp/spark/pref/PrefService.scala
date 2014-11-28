package de.kp.spark.pref
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

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.core.SparkService
import de.kp.spark.pref.actor.PrefMaster

object PrefService {

  def main(args: Array[String]) {
    
    val name:String = "rating-server"
    val conf:String = "server.conf"

    val server = new PrefService(conf, name)
    while (true) {}
    
    server.shutdown
      
  }

}

class PrefService(conf:String, name:String) extends SparkService {

  val system = ActorSystem(name, ConfigFactory.load(conf))
  sys.addShutdownHook(system.shutdown)
  
  /* Create Spark context */
  private val sc = createCtxLocal("PrefContext",Configuration.spark)      

  val master = system.actorOf(Props(new PrefMaster(sc)), name="rating-master")

  def shutdown = system.shutdown()
  
}