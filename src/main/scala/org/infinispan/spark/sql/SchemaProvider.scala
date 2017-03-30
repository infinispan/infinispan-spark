package org.infinispan.spark.sql

import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types.StructType


object SchemaProvider {

   def fromJavaBean(clazz: Class[_]): StructType = JavaTypeInference.inferDataType(clazz)._1.asInstanceOf[StructType]

}
