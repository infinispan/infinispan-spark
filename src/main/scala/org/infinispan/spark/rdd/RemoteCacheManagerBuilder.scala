package org.infinispan.spark.rdd

import java.net.InetSocketAddress
import java.util.function.Supplier

import org.infinispan.client.hotrod.{FailoverRequestBalancingStrategy, RemoteCacheManager}
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.commons.marshall.Marshaller
import org.infinispan.protostream.annotations.ProtoSchemaBuilder
import org.infinispan.protostream.{BaseMarshaller, FileDescriptorSource}
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants
import org.infinispan.spark.config.ConnectorConfiguration

import scala.collection.JavaConversions._


/**
  * @author gustavonalle
  */
object RemoteCacheManagerBuilder {

   /**
     * Create a remote cache manager with default marshaller using supplied configuration.
     */
   def create(cfg: ConnectorConfiguration): RemoteCacheManager = create(cfg, None)

   /**
     * Create  remote cache manager with a balancer strategy that gives preference to a certain host.
     */
   def create(cfg: ConnectorConfiguration, preferredAddress: InetSocketAddress): RemoteCacheManager = create(cfg, Some(preferredAddress))

   private def create(cfg: ConnectorConfiguration, preferredAddress: Option[InetSocketAddress]) = {
      if (!cfg.usesProtobuf) new RemoteCacheManager(createBuilder(cfg, preferredAddress, None).build())
      else
         createForQuery(cfg, preferredAddress)
   }

   private def createForQuery(cfg: ConnectorConfiguration, preferredAddress: Option[InetSocketAddress]) = {
      val builder = createBuilder(cfg, preferredAddress, Some(new ProtoStreamMarshaller))
      val rcm = new RemoteCacheManager(builder.build())
      buildSerializationContext(cfg, rcm)
   }

   private def createBuilder(cfg: ConnectorConfiguration, preferredAddress: Option[InetSocketAddress], marshaller: Option[Marshaller]) = {
      val configBuilder = new ConfigurationBuilder().withProperties(cfg.getHotRodClientProperties)
      def balancingStrategyFactory(a: InetSocketAddress) = new Supplier[FailoverRequestBalancingStrategy] {
         override def get(): FailoverRequestBalancingStrategy = new PreferredServerBalancingStrategy(a)
      }
      preferredAddress.foreach(balancingStrategyFactory)
      marshaller.foreach(m => configBuilder.marshaller(m))
      configBuilder
   }

   private def buildSerializationContext(cfg: ConnectorConfiguration, cm: RemoteCacheManager) = {
      val metadataCache = cm.getCache[String, AnyRef](ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME)
      val autoRegister = cfg.getRegisterSchemas
      def buildDescriptorSource(descriptors: Map[String, String]): FileDescriptorSource = {
         val fileDescriptorSource = new FileDescriptorSource
         descriptors.foldLeft(fileDescriptorSource) {
            case (fds, (fileName, contents)) => fds.addProtoFile(fileName, contents)
         }
         fileDescriptorSource
      }
      val serCtx = ProtoStreamMarshaller.getSerializationContext(cm)

      val protoDescriptors = cfg.getProtoFiles
      val marshallers = cfg.getMarshallers
      val protoAnnotatedEntities = cfg.getProtoEntities
      val descriptorSource = buildDescriptorSource(protoDescriptors)
      if (autoRegister) {
         descriptorSource.getFileDescriptors.foreach { case (name, contents) => metadataCache.put(name, new String(contents)) }
      }
      serCtx.registerProtoFiles(descriptorSource)

      marshallers.foreach { c => serCtx.registerMarshaller(c.newInstance().asInstanceOf[BaseMarshaller[_]]) }

      if (protoDescriptors.isEmpty) {
         val protoSchemaBuilder = new ProtoSchemaBuilder
         protoAnnotatedEntities.foreach { e =>
            val contents = protoSchemaBuilder.fileName(e.getName).addClass(e).build(serCtx)
            if (autoRegister) metadataCache.put(s"${e.getName}.proto", contents)
         }
      }
      cm
   }

}
