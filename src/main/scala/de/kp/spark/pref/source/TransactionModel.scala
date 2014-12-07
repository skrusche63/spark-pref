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

import de.kp.spark.core.model._

import de.kp.spark.pref.model._
import de.kp.spark.pref.spec.Fields

import scala.collection.mutable.ArrayBuffer

class TransactionModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElasticExplicit(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[(String,String,Int,Double,Long)] = {
 
    val spec = sc.broadcast(Fields.get(req))
    rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val item  = data(spec.value("item")._1).toInt
      
      val score = data(spec.value("score")._1).toDouble
       
      (site,user,item,score,timestamp)
      
    })
    
  }
  
  def buildElasticImplicit(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[(String,String,List[(Long,List[Int])])] = {
 
    val spec = sc.broadcast(Fields.get(req))
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val group = data(spec.value("group")._1)

      val item  = data(spec.value("item")._1)
      
      (site,user,group,timestamp,item)
      
    })
    
    buildTransactions(dataset)
    
  }
  
  def buildFileExplicit(req:ServiceRequest,rawset:RDD[String]):RDD[(String,String,Int,Double,Long)] = {
    
    rawset.map(valu => {
      val Array(site,timestamp,user,item,score) = valu.split(",")  
      (site,user,item.toInt,score.toDouble,timestamp.toLong)
    })
    
  }
 
  def buildFileImplicit(req:ServiceRequest,rawset:RDD[String]):RDD[(String,String,List[(Long,List[Int])])] = {
    
    val dataset = rawset.map(valu => {
      val Array(site,timestamp,user,group,item) = valu.split(",")  
      (site,user,group,timestamp.toLong,item)
    })
    
    buildTransactions(dataset)
    
  }
  
  def buildJDBCExplicit(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,String,Int,Double,Long)] = {
        
    val fieldspec = Fields.get(req)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]

      val user = data(spec.value("user")._1).asInstanceOf[String] 
      val item = data(spec.value("item")._1).asInstanceOf[Int]

      val score = data(spec.value("score")._1).asInstanceOf[Double]
      
      (site,user,item,score,timestamp)
      
    })

  }
  
  def buildJDBCImplicit(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[(String,String,List[(Long,List[Int])])] = {
        
    val fieldspec = Fields.get(req)
    val fields = fieldspec.map(kv => kv._2._1).toList    

    val spec = sc.broadcast(fieldspec)
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]

      val user = data(spec.value("user")._1).asInstanceOf[String] 
      val group = data(spec.value("group")._1).asInstanceOf[String]
      
      val item  = data(spec.value("item")._1).asInstanceOf[String]
      
      (site,user,group,timestamp,item)
      
    })
    
    buildTransactions(dataset)

  }

  private def buildTransactions(dataset:RDD[(String,String,String,Long,String)]):RDD[(String,String,List[(Long,List[Int])])] = {
    /*
     * Group dataset by site & user and aggregate all items of a
     * certain group and all groups into a time-ordered sequence
     */
    val sequences = dataset.groupBy(v => (v._1,v._2)).map(data => {
     
      val (site,user) = data._1
      /*
       * Aggregate all items of a certain group onto a single
       * line thereby sorting these items in ascending order.
       * 
       * And then, sort these items by timestamp in ascending
       * order.
       */
      val groups = data._2.groupBy(_._3).map(group => {

        val timestamp = group._2.head._4
        val items = group._2.map(_._5.toInt).toList

        (timestamp,items)
        
      }).toList.sortBy(_._1)

      (site,user,groups)     
      
    })
    
    sequences
    
  }
}