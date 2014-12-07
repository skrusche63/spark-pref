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

  private val config = Configuration
  private val model = new EventModel(sc)
   
  def explicitDS(req:ServiceRequest):RDD[(String,String,String,Int,Double,Long)] = {
    
    val source = req.data("source")
    source match {

      case Sources.ELASTIC => {

        val rawset = new ElasticSource(sc).connect(config,req)
        model.buildElasticExplicit(req,rawset)
      
      }

      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(config.file(0),req)
        model.buildFileExplicit(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
         
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        model.buildJDBCExplicit(req,rawset)
        
      }
            
      case _ => null
      
   }

  }
 
  def implicitDS(req:ServiceRequest):RDD[(String,String,String,Int,Long)] = {
    
    val source = req.data("source")
    source match {

      case Sources.ELASTIC => {

        val rawset = new ElasticSource(sc).connect(config,req)
        model.buildElasticImplicit(req,rawset)
      
      }

      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(config.file(0),req)
        model.buildFileImplicit(req,rawset)
        
      }

      case Sources.JDBC => {
    
        val fields = Fields.get(req).map(kv => kv._2._1).toList    
         
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        model.buildJDBCImplicit(req,rawset)
        
      }
            
      case _ => null
      
   }

  }

}