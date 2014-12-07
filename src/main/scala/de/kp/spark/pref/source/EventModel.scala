package de.kp.spark.pref.source
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.pref.spec.Fields

class EventModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElasticExplicit(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[(String,String,String,Int,Double,Long)] = {
 
    val spec = sc.broadcast(Fields.get(req))
    rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val item  = data(spec.value("item")._1)

      val event = data(spec.value("event")._1).toInt
      val score = data(spec.value("score")._1).toDouble
      
      (site,user,item,event,score,timestamp)
      
    })
    
  }

  def buildElasticImplicit(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[(String,String,String,Int,Long)] = {
 
    val spec = sc.broadcast(Fields.get(req))
    val rating = sc.broadcast(req.data(Names.REQ_RATING))
    
    rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val item  = data(spec.value("item")._1)

      val event = data(spec.value("event")._1).toInt
     
      (site,user,item,event,timestamp)
      
    })
    
  }
  
  /**
   * The raw data are retrieved from a pre processed web log file;
   * the format expected is: site,user,item,time,event,score
   */
  def buildFileExplicit(req:ServiceRequest,rawset:RDD[String]):RDD[(String,String,String,Int,Double,Long)] = {
      
    rawset.map(line => {
      val Array(site,user,item,time,event,score) = line.split(",")
      (site,user,item,event.toInt,score.toDouble,time.toLong)
    })
  
  }
   /**
   * The raw data are retrieved from a pre processed web log file;
   * the format expected is: site,user,item,time,event
   */
  def buildFileImplicit(req:ServiceRequest,rawset:RDD[String]):RDD[(String,String,String,Int,Long)] = {
       
    rawset.map(line => {
      val Array(site,user,item,time,event) = line.split(",")
      (site,user,item,event.toInt,time.toLong)
    })
  
  }
 
  def buildJDBCExplicit(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,String,String,Int,Double,Long)] = {
        
    val fieldspec = Fields.get(req)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]

      val user = data(spec.value("user")._1).asInstanceOf[String] 
      val item  = data(spec.value("item")._1).asInstanceOf[String]

      val event = data(spec.value("event")._1).asInstanceOf[Int]
      val score = data(spec.value("score")._1).asInstanceOf[Double]
      
      (site,user,item,event,score,timestamp)
     
    })

  }
  def buildJDBCImplicit(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,String,String,Int,Long)] = {
        
    val fieldspec = Fields.get(req)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]

      val user = data(spec.value("user")._1).asInstanceOf[String] 
      val item  = data(spec.value("item")._1).asInstanceOf[String]

      val event = data(spec.value("event")._1).asInstanceOf[Int]
      
      (site,user,item,event,timestamp)
     
    })

  }
  
}