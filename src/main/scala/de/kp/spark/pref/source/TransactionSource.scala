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

import de.kp.spark.core.source.ElasticSource
import de.kp.spark.pref.model.Sources

/**
 * A TransactionSource is an abstraction layer on top of
 * different physical data source to retrieve a transaction
 * database compatible with the Top-K and Top-KNR algorithm
 */
class TransactionSource(@transient sc:SparkContext) {

  private val transactionModel = new TransactionModel(sc)
  
  def transDS(data:Map[String,String]):RDD[(String,String,List[(Long,List[Int])])] = {
    
    val uid = data("uid")
    
    val source = data("source")
    source match {

      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(data)
        transactionModel.buildElastic(uid,rawset)
        
      }

      case Sources.FILE => {
        
        val rawset = new FileSource(sc).connect(data)
        transactionModel.buildFile(uid,rawset)
        
      }

      case Sources.JDBC => {
        
        val rawset = new JdbcSource(sc).connect(data)
        transactionModel.buildJDBC(uid,rawset)
        
      }

      case Sources.PIWIK => {
        
        val rawset = new PiwikSource(sc).connect(data)
        transactionModel.buildPiwik(uid,rawset)
        
      }
            
      case _ => null
      
   }

  }

}