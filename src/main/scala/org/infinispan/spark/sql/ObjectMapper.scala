package org.infinispan.spark.sql

import java.beans.Introspector

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRowWithSchema}
import org.apache.spark.sql.types.StructType


object ObjectMapper {

   def forBean(schema: StructType, beanClass: Class[_]): (AnyRef, Array[String]) => Row = {
      val beanInfo = Introspector.getBeanInfo(beanClass)
      val attrs = schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable)())
      val extractors = beanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)
      val methodsToConverts = extractors.zip(attrs).map { case (e, attr) =>
         (e, CatalystTypeConverters.createToCatalystConverter(attr.dataType))
      }
      (from: Any, columns: Array[String]) => {
         if (columns.nonEmpty) {
            from match {
               case f: Array[_] => new GenericRowWithSchema(from.asInstanceOf[Array[Any]], schema)
               case f: Any =>
                  val rowSchema = StructType(Array(schema(columns.head)))
                  new GenericRowWithSchema(Array(f), rowSchema)
            }
         } else {
            new GenericRowWithSchema(methodsToConverts.map { case (e, convert) =>
               val invoke: AnyRef = e.invoke(from)
               convert(invoke)
            }, schema)

         }
      }
   }

}
