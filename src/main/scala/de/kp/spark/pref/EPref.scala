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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import de.kp.spark.core.model._
import de.kp.spark.pref.sink.RedisSink

import de.kp.spark.pref.model._

import de.kp.spark.pref.format.FMFormatter
import de.kp.spark.pref.util.EventScoreBuilder

object EPrefBuilder extends Serializable {

  def buildToFile(req:ServiceRequest,rawset:RDD[(String,String,String,Int,Long)]) {
    
    val ratings = buildRatings(rawset)
    if (req.data.contains("format")) {
       
      val format = req.data("format")
      if (Formats.isFormat(format)) {
        
        val formatted = FMFormatter.format(req,ratings)
        val stringified = formatted.map(record => {
          
          val target = record._1
          val features = record._2
          
          "" + target + "," + features.mkString(" ")
          
        })
    
        val path = Configuration.output("event")
        stringified.saveAsTextFile(path)
        
      }
      
    } else {
      
      val stringified = ratings.map(record => {
        
        val (site,user,item,rating,timestamp,event) = record
        List(site,user,item,rating.toString,timestamp.toString,event.toString).mkString(",")
        
      })
    
      val path = Configuration.output("event")
      stringified.saveAsTextFile(path)

    }
    
  }
  
  def buildToRedis(req:ServiceRequest,rawset:RDD[(String,String,String,Int,Long)]) {
    
    val ratings = buildRatings(rawset)
    if (req.data.contains("format")) {
       
      val format = req.data("format")
      if (Formats.isFormat(format)) {
        
        val formatted = FMFormatter.format(req,ratings)
        formatted.foreach(record => {
          
          val target = record._1
          val features = record._2
          
          val text = "" + target + "," + features.mkString(" ")
          RedisSink.addRating(req.data("uid"), text)          
        
        })
      }
      
    } else {
      
      ratings.foreach(record => {
        
        val (site,user,item,rating,timestamp,event) = record
        val text = List(site,user,item,rating.toString,timestamp.toString,event.toString).mkString(",")
        
        RedisSink.addRating(req.data("uid"), text)

      })
    
    }
  
  }
  
  private def buildRatings(rawset:RDD[(String,String,String,Int,Long)]):RDD[(String,String,String,Int,Long,Int)] = {
   
    /* Group extracted data by site, user and item */
    val grouped = rawset.groupBy(x => (x._1,x._2,x._3))
    grouped.map(x => {
      
      val (site,user,item) = x._1
      val events = x._2
      
      /*
       * Events may have positive or negative numbers; as an example,
       * the following descriptions may be used:
       * 
       *  1	Purchased item
       *  2	Joined checkout
       *  3	Placed item in shopping cart
       *  4	Placed item in wish list
       *  5	Browsed item from search result
       *  6	Browsed item from recommendation list
       *  7	Browsed item
       * -1	Returned item
       * -2	Left checkout
       * -3	Removed item from shopping cart
       * -4	Removed item from wish list
       * 
       */
      
      /*
       * Split dataset with respect to event type into positives and negatives
       */      
      val positives = events.filter(x => x._4 > 0)
      val negatives = events.filter(x => x._4 < 0)
      
      /*
       * Determine support for positive & negative event types
       */
      val neg_support = negatives.map(x => (x._4,1)).groupBy(_._1).map(x => (x._1,x._2.size))
      val pos_support = positives.map(x => (x._4,1)).groupBy(_._1).map(x => (x._1,x._2.size))
      /*
       * Some events have negative values indicating negative actions on the
       * part of the user e.g., removing an item from the shopping cart. 
       * 
       * For each such negative event, a corresponding positive event is removed 
       * from the event sequence, before calculating rating.
       * 
       */      
      val support = pos_support.map(x => {
        
        val (event,count) = x        
        val diff_count = if (neg_support.contains(-event)) {

          val difference = count - neg_support(-event)
          if (difference > 0) difference else 0
          
        } else count
        
        (event,diff_count)
        
      }).filter(x => x._2 > 0)
      
      val ratings = support.map(x => {
        
        val (event,support) = x
        val rating = getRating(event,support)
        
        (event,rating)
        
      }).toList.sortBy(x => -x._2)
      
      /* Determine maximum engagement event and respective rating */
      val (event,rating) = ratings.head      
      /* Determine latest timestamp for the maximum engagement event */
      val latest = positives.filter(x => x._4 == event).toList.sortBy(x => -x._5).head._5
      
      (site,user,item,rating,latest,event)
      
    })
    
  }
  
  private def getRating(event:Int,support:Int):Int = {
    
    val eventScores = EventScoreBuilder.get().filter(x => x.event == event)
    val eventScore = if (eventScores.size > 0) eventScores(0) else null

    if (eventScore == null) throw new Exception("Event is unknown.")

    val scores = eventScore.scores
    if (support <= scores.length) scores(support-1) else scores(scores.size -1)
     
  }
    
}