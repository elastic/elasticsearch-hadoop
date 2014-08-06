package org.elasticsearch.spark.serialization

import scala.collection.Map
import scala.collection.immutable.Nil

import org.elasticsearch.hadoop.serialization.Generator
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter

class ScalaValueWriter(writeUnknownTypes: Boolean = false) extends JdkValueWriter(writeUnknownTypes) {
  
  def this() {
    this(false)
  }
  
  override def write(value: AnyRef, generator: Generator): Boolean = {
    value match {
      case None		  	   			=> generator.writeNull()
      case Unit	  	  	   			=> generator.writeNull()
      case Nil		   	   			=> generator.writeBeginArray(); generator.writeEndArray()
      
      case s: Some[AnyRef]			=> return write(s.get, generator) 
      
      case m: Map[_, AnyRef]  		=> {
         generator.writeBeginObject()
         for ((k,v) <- m) {
           generator.writeFieldName(k.toString())
           if (!write(v, generator)) {
             return false
           }
         }
         generator.writeEndObject()
      }
      
      case i: Traversable[AnyRef] 	=> {
         generator.writeBeginArray()
         for (v <- i) {
           if (!write(v, generator)) {
             return false
           }
         }
         generator.writeEndArray()
      }
      
      case p: Product				=> {
        generator.writeBeginArray()
        for (t <- p.productIterator) {
          if (!write(t.asInstanceOf[AnyRef], generator)) {
             return false
          }
        }
        generator.writeEndArray()
      }
      
      case _ 		       			=> return super.write(value, generator)
    }
     
    true
  }
}