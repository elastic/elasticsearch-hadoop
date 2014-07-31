package org.elasticsearch.hadoop.spark.serialization

import scala.collection.Map
import scala.collection.immutable.Nil

import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter

class ScalaValueWriter(writeUnknownTypes: Boolean = false) extends JdkValueWriter(writeUnknownTypes) {
  
  def this() {
    this(false)
  }
  
  override def write(value: AnyRef, generator: Generator): Boolean = {
    if (value == null) {
      generator.writeNull();
    }
    
    value match {
      case None		  	   			=> generator.writeNull()
      case Unit	  	  	   			=> generator.writeNull()
      case Nil		   	   			=> generator.writeBeginArray(); generator.writeEndArray()
      
      case m: Map[_, AnyRef]  		=> {
         generator.writeBeginObject()
         for ((k,v) <- m) {
           generator.writeFieldName(k.toString())
           write(v, generator)
         }
         generator.writeEndObject()
      }
      
      case i: Traversable[AnyRef] 	=> {
         generator.writeBeginArray();
         for (v <- i) {
            write(v, generator);
         }
         generator.writeEndArray();
      }
      case _ 		       			=> return super.write(value, generator)
    }
     
    true
  }
}