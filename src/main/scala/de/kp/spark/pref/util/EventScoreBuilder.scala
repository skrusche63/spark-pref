package de.kp.spark.pref.util

import scala.xml._
import scala.collection.mutable.ArrayBuffer

import de.kp.spark.pref.model._

object EventScoreBuilder extends Serializable {
  
  private val path = "scores.xml"
  private val eventScores = build()

  private def build():List[EventScore] = {
 
    val buffer = ArrayBuffer.empty[EventScore]
    
    try {
 
      val root = XML.load(getClass.getClassLoader.getResource(path))     
      for (element <- root \ "EventScore") {
      
        val event = (element \ "type").text.toInt
        val desc  = (element \ "description").text.toString

        val scores = (element \ "scores").text.split(",").toList.map(_.toInt)
      
        buffer += new EventScore(event,desc,scores)
     }
      
    } catch {
      case e:Exception => {}
    }
    
    buffer.toList
    
  }

  def get() = eventScores
  
}