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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.io.ParquetWriter

import de.kp.spark.pref.format.{Items,Users,Ratings}
import de.kp.spark.pref.model._

import scala.collection.mutable.Buffer

/**
 * The NPrefBuilder provides a mechanism to compute
 * implicit preferences of a certain user for a specific
 * item
 */
class NPrefBuilder(@transient sc:SparkContext) extends Serializable {

  def ratingsExplicit(req:ServiceRequest,rawset:RDD[(String,String,Int,Double,Long)]) {

    val ratings = rawset.map(x => {
      val (site,user,item,score,timestamp) = x
      (site,user,item,score.toInt)
    })
    
    req.data(Names.REQ_SINK) match {
  
      case Sinks.FILE    => ratingsToFile(req,ratings)
      case Sinks.PARQUET => ratingsToParquet(req,ratings)
        
      case Sinks.REDIS => ratingsToRedis(req,ratings)
      
      case _ => {/* do nothing */}

    }
  
  }
  
  /**
   * The NPrefBuilder supports the following input format: (site,user,groups).
   * 
   * 'site' refers to a certain tenant or website (ecommerce store etc.) and 
   * 'user' is a unique user identifier with respect to the considered 'site'.
   * 
   * 'groups' is time-ordered list, List(timestamp,items), of items for which
   * the user preference has to be computed; a single group may be interpreted
   * as an ecommerce order or transaction etc.
   * 
   */  
  def ratingsImplicit(req:ServiceRequest,rawset:RDD[(String,String,List[(Long,List[Int])])]) {
    
    val ratings = buildRatings(rawset)
    req.data(Names.REQ_SINK) match {
  
      case Sinks.FILE    => ratingsToFile(req,ratings)
      case Sinks.PARQUET => ratingsToParquet(req,ratings)
        
      case Sinks.REDIS => ratingsToRedis(req,ratings)
      
      case _ => {/* do nothing */}

    }
    
  }

  private def ratingsToFile(req:ServiceRequest,ratings:RDD[(String,String,Int,Int)]) {
    /*
     * Check whether users already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Users.exists(req) == false) {
      val busers = sc.broadcast(Users)
      ratings.foreach(x => busers.value.put(req,x._2))
    
    } 
    /*
     * Check whether items already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Items.exists(req) == false) {
      val bitems = sc.broadcast(Items)
      ratings.foreach(x => bitems.value.put(req,x._3.toString))    
    } 

    val path = Configuration.output(1)    
    ratings.map(x => List(x._1,x._2,x._3.toString,x._4.toString).mkString(",")).saveAsTextFile(path)
    
  }
  
  private def ratingsToRedis(req:ServiceRequest,ratings:RDD[(String,String,Int,Int)]) {
    /*
     * Check whether users already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Users.exists(req) == false) {
      val busers = sc.broadcast(Users)
      ratings.foreach(x => busers.value.put(req,x._2))
    
    } 
    /*
     * Check whether items already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Items.exists(req) == false) {
      val bitems = sc.broadcast(Items)
      ratings.foreach(x => bitems.value.put(req,x._3.toString))    
    } 

    val bratings = sc.broadcast(Ratings)
    ratings.foreach(x => bratings.value.put(req,List(x._1,x._2,x._3.toString,x._4.toString).mkString(",")))
    
  }
  
  private def ratingsToParquet(req:ServiceRequest,ratings:RDD[(String,String,Int,Int)]) {
    /*
     * Check whether users already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Users.exists(req) == false) {
      val busers = sc.broadcast(Users)
      ratings.foreach(x => busers.value.put(req,x._2))
    
    } 
    /*
     * Check whether items already exist for the referenced mining or building
     * task and associated model or matrix name
     */
    if (Items.exists(req) == false) {
      val bitems = sc.broadcast(Items)
      ratings.foreach(x => bitems.value.put(req,x._3.toString))    
    } 

    val store = Configuration.output(1)    
    val dataset = ratings.map(x => ItemScoreObject(x._1,x._2,x._3,x._4))
    
    val writer = new ParquetWriter(sc)
    writer.writeScoredItems(store, dataset)
    
  }
 
  private def buildRatings(rawset:RDD[(String,String,List[(Long,List[Int])])]):RDD[(String,String,Int,Int)] = {
 
    val userItems = rawset.map(data => {
     
      val (site,user,groups) = data
      /* 
       * Total number of groups per (site,user); in an ecommerce 
       * environment a group is equal to a transaction 
       */
      val total = groups.size
      
      val itemset = List.empty[Int]
      groups.foreach(group => itemset ++ group._2)
      
      (site,user,total,itemset)
      
    })

    /*
     * Compute the item support on a per (site,user) basis; then from this support
     * and the total number of groups, the item preference is computed as follows:
     * 
     * pref = Math.log(1 + supp.toDouble / total.toDouble)
     * 
     */
    val userItemPrefs = userItems.map(data => {
      
      val (site,user,total,itemset) = data
      val itemSupport = itemset.map(item => (item,1)).groupBy(_._1).map(group => (group._1,group._2.size))

      val itemPref = itemSupport.map(entry => {
        
        val (item,supp) = entry
        val pref = Math.log(1 + supp.toDouble / total.toDouble)
        
        (item,pref)
        
      })

      (site,user,itemPref.toList)
      
    })
   
    /*
     * The user-item preferences are solely based on the purchase data of a particular
     * user so far; the respective value, however, is far from representing a real-life 
     * value, as it only takes the purchase frequency into account.
     * 
     * The frequency is quite different depending on the item price, item lifetime, 
     * and the like. For example, since expensive items or items with long lifespan,
     * such as jewelry or electronic home appliances, are purchased infrequently.
     * 
     * So the preferences of users cannot be higher than for these cheap items or
     * those with a short lifespan such as hand creams or tissues. Also, when a 
     * user u purchases item i four times out of ten transactions, we may think that
     * he does not prefer item i if other users purchased the same item eight times
     * out of ten transactions.
     * 
     * It is therefore necessary to define a relative preference so it is comparable 
     * among all users. We therefore proceed to compute the maximum item preference
     * for all users and use this value to normalize the user-item preference derived
     * above.  
     */
    val itemMaxPref = userItemPrefs.flatMap(data => data._3).groupBy(_._1).map(group => {
       
      def max(pref1:Double, pref2:Double):Double = if (pref1 > pref2) pref1 else pref2
        
      val item = group._1
      val mpref = group._2.map(_._2).reduceLeft(max)

      (item,mpref)
      
    }).collect().toMap

    val bcprefs = sc.broadcast(itemMaxPref)
    userItemPrefs.flatMap(data => {
      
      val (site,user,prefs) = data
      prefs.map(data => {
        
        val (item,pref) = data
        val mpref = bcprefs.value(item)
        
        val npref = Math.round( 5* (pref.toDouble / mpref.toDouble) ).toInt
        (site,user,item,npref)
        
      })
      
    })
    
  }
 
}