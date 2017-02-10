package org.infinispan.spark.rdd

import java.net.InetSocketAddress
import java.util.Properties

import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.commons.marshall.Marshaller
import org.infinispan.protostream.annotations.ProtoSchemaBuilder
import org.infinispan.protostream.{BaseMarshaller, FileDescriptorSource}
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants
import org.infinispan.spark._

import scala.collection.JavaConversions._


/**
  * @author gustavonalle
  */
object RemoteCacheManagerBuilder {

   val ProtoProperties = Set(InfinispanRDD.ProtoEntities, InfinispanRDD.ProtoFiles)

   private def hasProtoFile(cfg: Properties) = ProtoProperties.exists(cfg.keySet().contains)

   /**
     * Create a remote cache manager with default marshaller using supplied configuration.
     */
   def create(cfg: Properties): RemoteCacheManager = create(cfg, None)

   /**
     * Create  remote cache manager with a balancer strategy that gives preference to a certain host.
     */
   def create(cfg: Properties, preferredAddress: InetSocketAddress): RemoteCacheManager = create(cfg, Some(preferredAddress))

   private def create(cfg: Properties, preferredAddress: Option[InetSocketAddress]) = {
      if (!hasProtoFile(cfg)) new RemoteCacheManager(createBuilder(cfg, preferredAddress, None).build())
      else
         createForQuery(cfg, preferredAddress)
   }

   private def createForQuery(cfg: Properties, preferredAddress: Option[InetSocketAddress]) = {
      val builder = createBuilder(cfg, preferredAddress, Some(new ProtoStreamMarshaller))
      val rcm = new RemoteCacheManager(builder.build())
      buildSerializationContext(cfg, rcm)
   }

   private def createBuilder(cfg: Properties, preferredAddress: Option[InetSocketAddress], marshaller: Option[Marshaller]) = {
      val configBuilder = new ConfigurationBuilder().withProperties(cfg)
      preferredAddress.foreach(a => configBuilder.balancingStrategy(new PreferredServerBalancingStrategy(a)))
      marshaller.foreach(m => configBuilder.marshaller(m))
      configBuilder
   }

   private def buildSerializationContext(cfg: Properties, cm: RemoteCacheManager) = {
      val metadataCache = cm.getCache[String, AnyRef](ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME)
      val autoRegister = cfg.read[Boolean](InfinispanRDD.RegisterSchemas).getOrElse(false)
      def buildDescriptorSource(descriptors: java.util.Map[String, String]): FileDescriptorSource = {
         val fileDescriptorSource = new FileDescriptorSource
         descriptors.foldLeft(fileDescriptorSource) {
            case (fds, (fileName, contents)) => fds.addProtoFile(fileName, contents)
         }
         fileDescriptorSource
      }
      val serCtx = ProtoStreamMarshaller.getSerializationContext(cm)

      val protoDescriptors = cfg.read[java.util.Map[String, String]](InfinispanRDD.ProtoFiles)
      val marshallers = cfg.read[Seq[Class[BaseMarshaller[_]]]](InfinispanRDD.Marshallers)
      val protoAnnotatedEntities = cfg.read[Seq[Class[_]]](InfinispanRDD.ProtoEntities)
      protoDescriptors.foreach { descriptors =>
         val descriptorSource = buildDescriptorSource(descriptors)
         if (autoRegister) descriptorSource.getFileDescriptors.foreach { case (name, contents) => metadataCache.put(name, contents) }
         serCtx.registerProtoFiles(descriptorSource)
      }
      marshallers.foreach {
         _.foreach { c => serCtx.registerMarshaller(c.newInstance()) }
      }

      if (protoDescriptors.isEmpty) {
         val protoSchemaBuilder = new ProtoSchemaBuilder
         protoAnnotatedEntities.foreach(entities => entities.foreach { e =>
            val contents = protoSchemaBuilder.fileName(e.getName).addClass(e).build(serCtx)
            if (autoRegister) metadataCache.put(s"${e.getName}.proto", contents)
         })
      }
      cm
   }

}
