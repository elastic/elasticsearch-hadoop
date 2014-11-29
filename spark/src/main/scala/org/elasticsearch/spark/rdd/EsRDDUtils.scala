// Utililty to work with complex ElasticSearch RDDs containing nested Map[String,Any]
package org.elasticsearch.spark.rdd

import scala.annotation.tailrec

object EsRDDUtils {

  implicit class MapWithOpt(a:Option[Any]) {
    def asOptDouble:Option[Double] = a match {
          case Some(x) => Some(x.asInstanceOf[Double])
          case _ => None
        }
    def asOptLong:Option[Long] = a match {
          case Some(x) => Some(x.asInstanceOf[Long])
          case _ => None
        }
    def asOptInt:Option[Int] = a match {
          case Some(x) => Some(x.asInstanceOf[Int])
          case _ => None
        }
    def asOptSeq:Option[Seq[Any]] = a match {
          case Some(x) => Some(x.asInstanceOf[Seq[Any]])
          case _ => None
        }
    // optString can handle most types as well as arrays such as Seq[Any] (a type of Buffer)
    def asOptString:Option[String] = a match {
      // it may be an Option, so check for Some or None
      case Some(x) => x match {
        case _: Seq[Any] => {
          // convert Seq of Any values to comma a separated string
          // compiler needs casting to Seq to allow map
          val s: Seq[String] = (x.asInstanceOf[Seq[Any]] map (_.toString))
          Some(s.mkString(","))
        }
        // the remaining types, Int, Double, Long all support toString without casting
        case _ => Some(x.toString)
      }
      case None => None
    }
  }

  implicit class MapWithWalker(a: scala.collection.Map[String,Any]) {
    def \ (field: String): Option[Any] = {
      // pass in a field string separated with periods '.' such as "person.address.city"
      // walks through a Map[String,Any] nested Key->Value hierarchy and returns the final value as Option[Any]
      // returns None if any of the Keys or values listed in the . seperated fields are not found
      @tailrec def walkMap(node: collection.Map[String,Any], fields: Array[String] ): Option[Any] = {
          val element = node.get(fields.head)
          if (fields.size > 1) {  // walk until we are on last node in tree 
            val rec = element match {
              case m: Option[Any] => {
                    m match {
                      case Some(n) => m map { a => a.asInstanceOf[collection.Map[String, Any]] }
                      case _ => return None  // found a missing node in tree, exit tail recursion
                    }
                  }
              case _ => None
            }
            walkMap(rec.get, fields.tail )
          } 
          else element   // return the final value found in the last Key -> Value pair
      }
      walkMap(a, field.split('.'))  
    }
  }
}
