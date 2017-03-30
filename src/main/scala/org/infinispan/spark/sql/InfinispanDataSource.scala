package org.infinispan.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}


class InfinispanDataSource extends RelationProvider with DataSourceRegister {

   override def shortName(): String = "infinispan"

   override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
      new InfinispanRelation(sqlContext, parameters)

}
