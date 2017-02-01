package org.infinispan.spark.rdd

import java.net.InetSocketAddress
import java.util.Properties

import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.protostream.annotations.ProtoSchemaBuilder
import org.infinispan.protostream.{BaseMarshaller, FileDescriptorSource}
import org.infinispan.spark._

import scala.collection.JavaConversions._


/**
  * @author gustavonalle
  */
object RemoteCacheManagerBuilder {

   /**
     * Create a remote cache manager with default marshaller using supplied configuration.
     */
   def create(cfg: Properties) = new RemoteCacheManager(new ConfigurationBuilder().withProperties(cfg).build())

   /**
     * Create  remote cache manager with a balancer strategy that gives preference to a certain host.
     */
   def create(cfg: Properties, preferredAddress: InetSocketAddress) = {
      new RemoteCacheManager(createBuilder(cfg, preferredAddress).build())
   }

   /**
     * Create a remote cache manager suitable to do querying, with the correct marshaller and serialization context.
     */
   def createForQuery(cfg: Properties, preferredAddress: InetSocketAddress) = {
      val builder = createBuilder(cfg, preferredAddress)
      builder.marshaller(new ProtoStreamMarshaller)
      val rcm = new RemoteCacheManager(builder.build())
      buildSerializationContext(cfg, rcm)
   }

   def createForQuery(cfg: Properties) = {
      val rcm = new RemoteCacheManager(new ConfigurationBuilder().withProperties(cfg).marshaller(new ProtoStreamMarshaller).build())
      buildSerializationContext(cfg, rcm)
   }

   private def createBuilder(cfg: Properties, preferredAddress: InetSocketAddress) =
      new ConfigurationBuilder().withProperties(cfg)
         .balancingStrategy(new PreferredServerBalancingStrategy(preferredAddress))


   private def buildSerializationContext(cfg: Properties, cm: RemoteCacheManager) = {
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
         serCtx.registerProtoFiles(buildDescriptorSource(descriptors))
      }
      marshallers.foreach {
         _.foreach { c => serCtx.registerMarshaller(c.newInstance()) }
      }

      if (protoDescriptors.isEmpty) {
         val protoSchemaBuilder = new ProtoSchemaBuilder
         protoAnnotatedEntities.foreach(entities => entities.foreach { e =>
            protoSchemaBuilder.fileName(e.getName).addClass(e).build(serCtx)
         })
      }
      cm
   }

}
