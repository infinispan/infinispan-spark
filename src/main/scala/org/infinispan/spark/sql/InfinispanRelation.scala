package org.infinispan.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.InfinispanRDD


class InfinispanRelation(context: SQLContext, val parameters: Map[String, String])
  extends BaseRelation with PrunedFilteredScan with Serializable {

   override def sqlContext: SQLContext = context

   lazy val props: ConnectorConfiguration = ConnectorConfiguration(parameters)

   val clazz = {
      val protoEntities = props.getProtoEntities
      val targetEntity = Option(props.getTargetEntity) match {
         case Some(p) => p
         case None => if (protoEntities.nonEmpty) protoEntities.head
         else
            throw new IllegalArgumentException(s"No target entity nor annotated protobuf entities found, check the configuration")
      }
      targetEntity
   }

   @transient lazy val mapper = ObjectMapper.forBean(schema, clazz)

   override def schema: StructType = SchemaProvider.fromJavaBean(clazz)

   override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
      val rdd: InfinispanRDD[AnyRef, AnyRef] = new InfinispanRDD(context.sparkContext, props)
      val serCtx = ProtoStreamMarshaller.getSerializationContext(rdd.remoteCache.getRemoteCacheManager)
      val message = serCtx.getMarshaller(clazz).getTypeName
      val projections = toIckle(requiredColumns)
      val predicates = toIckle(filters)

      val select = if (projections.nonEmpty) s"SELECT $projections" else ""
      val from = s"FROM $message"
      val where = if (predicates.nonEmpty) s"WHERE $predicates" else ""

      val query = s"$select $from $where"

      rdd.filterByQuery[AnyRef](query.trim).values.map(mapper(_, requiredColumns))
   }

   def toIckle(columns: Array[String]): String = columns.mkString(",")

   def toIckle(filters: Array[Filter]): String = filters.map(ToIckle).mkString(" AND ")

   private def ToIckle(f: Filter): String = {
      f match {
         case StringEndsWith(a, v) => s"$a LIKE '%$v'"
         case StringContains(a, v) => s"$a LIKE '%$a%'"
         case StringStartsWith(a, v) => s"$a LIKE '$v%'"
         case EqualTo(a, v) => s"$a = '$v'"
         case GreaterThan(a, v) => s"$a > $v"
         case GreaterThanOrEqual(a, v) => s"$a >= $v"
         case LessThan(a, v) => s"$a < $v"
         case LessThanOrEqual(a, v) => s"$a <= $v"
         case IsNull(a) => s"$a is null"
         case IsNotNull(a) => s"$a is not null"
         case In(a, vs) => s"$a IN (${vs.map(v => s"'$v'").mkString(",")})"
         case Not(filter) => s"NOT ${ToIckle(filter)}"
         case And(leftFilter, rightFilter) => s"${ToIckle(leftFilter)} AND ${ToIckle(rightFilter)}"
         case Or(leftFilter, rightFilter) => s"${ToIckle(leftFilter)} OR ${ToIckle(rightFilter)}"
      }
   }
}
