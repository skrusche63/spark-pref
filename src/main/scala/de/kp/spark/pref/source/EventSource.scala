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
import de.kp.spark.core.source.{ElasticSource,FileSource,JdbcSource}

import de.kp.spark.pref.Configuration
import de.kp.spark.pref.model.Sources

import de.kp.spark.pref.spec.Fields

class EventSource(@transient sc:SparkContext) {

  private val model = new EventModel(sc)
  
  def eventDS(req:ServiceRequest):RDD[(String,String,String,Int,Long)] = {
    
    val source = req.data("source")
    source match {

      case Sources.ELASTIC => {

        val rawset = new ElasticSource(sc).connect(req.data)
        model.buildElastic(req,rawset)
      
      }

      case Sources.FILE => {
        
        val path = Configuration.file()._1
        
        val rawset = new FileSource(sc).connect(req.data,path)
        model.buildFile(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
         
        val rawset = new JdbcSource(sc).connect(req.data,fields)
        model.buildJDBC(req,rawset)
        
      }

      case Sources.PIWIK => {
        null
      }
            
      case _ => null
      
   }

  }

}