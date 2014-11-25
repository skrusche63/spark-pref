package de.kp.spark.pref.format
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

import de.kp.spark.pref.model._
import de.kp.spark.pref.util.EventScoreBuilder

object Events {

  private val events = build()
  
  private def build():Dict = {
    
    val eventScores = EventScoreBuilder.get
    val events = eventScores.filter(x => x.event > 0).map(x => x.desc)
    
    new Dict().build(events)

  }
  
  def get = events
  
}