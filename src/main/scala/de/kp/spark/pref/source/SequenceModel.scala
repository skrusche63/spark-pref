package de.kp.spark.pref.source

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.pref.model._

import de.kp.spark.pref.spec.Fields

import scala.collection.mutable.ArrayBuffer

class SequenceModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(uid:String,rawset:RDD[Map[String,String]]):RDD[(String,String,List[(Long,List[Int])])] = {
 
    val spec = sc.broadcast(Fields.get(uid))
    val dataset = rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val group = data(spec.value("group")._1)

      val item  = data(spec.value("item")._1)
      
      (site,user,group,timestamp,item)
      
    })
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