package org.infinispan.spark.rdd

import java.net.InetSocketAddress
import java.util.Properties

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.filter.Filters
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.protostream.annotations.ProtoSchemaBuilder
import org.infinispan.protostream.{BaseMarshaller, FileDescriptorSource}
import org.infinispan.query.dsl.Query
import org.infinispan.spark._

import scala.collection.JavaConversions._

/**
 * Infinispan RDD based on pre-filtered data in a cache, either by a Query or a filter previously deployed in the server
 *
 * @author gustavonalle
 */
class FilteredInfinispanRDD[K, V, R](parent: InfinispanRDD[K, V],
                                     filterQuery: Option[FilterQuery],
                                     filterFactory: Option[String],
                                     filterParams: AnyRef*)
        extends RDD[(K, R)](parent.sc, Nil) {

   def createBuilder(address: InetSocketAddress, config: Properties) = {
      val builder = parent.createBuilder(address, config)
      if (filterQuery.isDefined) builder.marshaller(new ProtoStreamMarshaller)
      builder
   }

   private def getIteratorSize[T](iterator: Iterator[T]) = iterator.size

   override def count() = {
      if (filterFactory.isDefined) parent.sc.runJob(this, getIteratorSize _).sum
      else
         filterQuery.fold(parent.count())(_.queryObj.getResultSize)
   }

   @DeveloperApi
   override def compute(split: Partition, context: TaskContext): Iterator[(K, R)] = {
      val (factory, params) = filterQuery.fold((filterFactory.orNull, filterParams))(q => (Filters.ITERATION_QUERY_FILTER_CONVERTER_FACTORY_NAME, Array(q.query)))
      parent.compute(split, context, (address, properties) => {
         val cm = new RemoteCacheManager(createBuilder(address, properties).build())
         buildSerializationContext(cm)
         cm
      }, factory, params.toArray)
   }

   private def newInstance[A](clazz: Class[A]) = clazz.newInstance()

   private def buildDescriptorSource(descriptors: java.util.Map[String, String]): FileDescriptorSource = {
      val fileDescriptorSource = new FileDescriptorSource
      descriptors.foldLeft(fileDescriptorSource) {
         case (fds, (fileName, contents)) => fds.addProtoFile(fileName, contents)
      }
      fileDescriptorSource
   }

   private def buildSerializationContext(cm: RemoteCacheManager) = {
      if (filterQuery.isDefined) {
         val serCtx = ProtoStreamMarshaller.getSerializationContext(cm)
         val cfg = parent.configuration

         val protoDescriptors = cfg.read[java.util.Map[String,String]](InfinispanRDD.ProtoFiles)
         val marshallers = cfg.read[Seq[Class[BaseMarshaller[_]]]](InfinispanRDD.Marshallers)
         protoDescriptors.foreach { descriptors =>
            serCtx.registerProtoFiles(buildDescriptorSource(descriptors))
         }
         marshallers.foreach {
            _.foreach { c => serCtx.registerMarshaller(newInstance(c)) }
         }

         if (protoDescriptors.isEmpty) {
            val protoSchemaBuilder = new ProtoSchemaBuilder
            filterQuery.foreach(_.entities.foreach { entity =>
               protoSchemaBuilder.fileName(entity.getName).addClass(entity).build(serCtx)
            })
         }
      }
   }

   override protected def getPartitions = parent.getPartitions

}

private[rdd] case class FilterQuery(@transient queryObj: Query, query: String, parameters: java.util.Map[String, AnyRef], entities: Class[_]*)
