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

class EventModel(@transient sc:SparkContext) extends Serializable {
  
  /**
   * The raw data are retrieved from a pre processed web log file;
   * the format expected is: site|timestamp|user|event|item
   */
  def buildFile(uid:String,rawset:RDD[String]):RDD[(String,String,String,Int,Long)] = {
    
    rawset.map(valu => {
      
      val Array(site,timestamp,user,event,item) = valu.split("\\|")        
      (site,user,item,event.toInt,timestamp.toLong)
    
    })

  }
}