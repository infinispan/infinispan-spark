package org.infinispan.spark.config


import java.util.Properties

import org.infinispan.client.hotrod.impl.ConfigurationProperties._
import org.infinispan.protostream.BaseMarshaller

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Configuration manager for the Infinispan Spark connector.
  */
class ConnectorConfiguration extends Serializable {

   private var cacheName: String = _
   private var readBatchSize = 10000
   private var writeBatchSize = 500
   private var serverPartitions = 2
   private var autoRegister = false
   private var targetEntity: Class[_] = _
   private val messageMarshallers = mutable.Set[Class[_ <: BaseMarshaller[_]]]()
   private val protoAnnotatedClasses = mutable.Set[Class[_]]()
   private val descriptors = mutable.Map[String, String]()
   private val hotRodClientProps = new Properties()

   def setCacheName(cacheName: String): ConnectorConfiguration = {
      this.cacheName = cacheName
      this
   }

   def setReadBatchSize(batchSize: Integer): ConnectorConfiguration = {
      this.readBatchSize = batchSize
      this
   }

   def setWriteBatchSize(batchSize: Integer): ConnectorConfiguration = {
      this.writeBatchSize = batchSize
      this
   }

   def setPartitions(partitions: Integer): ConnectorConfiguration = {
      this.serverPartitions = partitions
      this
   }

   def setAutoRegisterProto(): ConnectorConfiguration = {
      this.autoRegister = true
      this
   }

   def addProtoFile(name: String, contents: String): ConnectorConfiguration = {
      this.descriptors += name -> contents
      this
   }

   def addMessageMarshaller(clazz: Class[_ <: BaseMarshaller[_]]): ConnectorConfiguration = {
      this.messageMarshallers.add(clazz)
      this
   }

   def addProtoAnnotatedClass(clazz: Class[_]): ConnectorConfiguration = {
      this.protoAnnotatedClasses.add(clazz)
      this
   }

   def setEnableSSL(): ConnectorConfiguration = {
      addHotRodClientProperty(USE_SSL, "true")
   }

   def setKeyStoreFileName(name: String): ConnectorConfiguration = {
      addHotRodClientProperty(KEY_STORE_FILE_NAME, name)
      this
   }

   def setTrustStoreFileName(name: String): ConnectorConfiguration = {
      addHotRodClientProperty(TRUST_STORE_FILE_NAME, name)
      this
   }

   def setKeyStorePassword(value: String): ConnectorConfiguration = {
      addHotRodClientProperty(KEY_STORE_PASSWORD, value)
      this
   }

   def setTrustStorePassword(value: String): ConnectorConfiguration = {
      addHotRodClientProperty(TRUST_STORE_PASSWORD, value)
      this
   }

   def addHotRodClientProperty(key: String, value: AnyRef): ConnectorConfiguration = {
      hotRodClientProps.put(key, value)
      this
   }

   def setTargetEntity(clazz: Class[_]): ConnectorConfiguration = {
      this.targetEntity = clazz
      this
   }

   def setServerList(serversString: String): ConnectorConfiguration = {
      addHotRodClientProperty(SERVER_LIST, serversString)
      this
   }


   import ConnectorConfiguration._


   def toStringsMap: Map[String, String] = {
      val stringMap = mutable.Map[String, String]()

      stringMap += CacheName -> cacheName
      stringMap += ReadBatchSize -> readBatchSize.toString
      stringMap += WriteBatchSize -> writeBatchSize.toString
      stringMap += PartitionsPerServer -> serverPartitions.toString

      if (descriptors.nonEmpty) {
         stringMap += ProtoFiles -> descriptors.toSeq.map { case (k, v) => s"$k$EntrySeparator${v.toString}" }.mkString(SequenceSeparator)
      }
      if (messageMarshallers.nonEmpty) {
         stringMap += Marshallers -> messageMarshallers.map(_.getName).mkString(SequenceSeparator)
      }
      if (protoAnnotatedClasses.nonEmpty) {
         stringMap += ProtoEntities -> protoAnnotatedClasses.map(_.getName).mkString(SequenceSeparator)
      }
      stringMap += RegisterSchemas -> autoRegister.toString

      if (targetEntity != null)
         stringMap += TargetEntity -> targetEntity.getName

      hotRodClientProps.foreach { case (k, v) => stringMap += k -> v }

      stringMap.toMap
   }


   def getCacheName = cacheName

   def getReadBatchSize = readBatchSize

   def getWriteBatchSize = writeBatchSize

   def getServerPartitions = serverPartitions

   def getRegisterSchemas = autoRegister

   def usesProtobuf = getProtoEntities.nonEmpty || descriptors.nonEmpty

   def getProtoEntities = protoAnnotatedClasses

   def getProtoFiles = descriptors.toMap

   def getMarshallers = messageMarshallers

   def getTargetEntity = targetEntity

   def getHotRodClientProperties: Properties = hotRodClientProps

   def getServerList = getHotRodClientProperties.getProperty(SERVER_LIST)

}

object ConnectorConfiguration {


   private val SequenceSeparator = ","
   private val EntrySeparator = ":"

   private val HotRodClientConfigPrefix = "infinispan.client"

   val CacheName = "infinispan.rdd.cacheName"
   val ReadBatchSize = "infinispan.rdd.read_batch_size"
   val WriteBatchSize = "infinispan.rdd.write_batch_size"
   val PartitionsPerServer = "infinispan.rdd.number_server_partitions"
   val ProtoFiles = "infinispan.rdd.query.proto.protofiles"
   val ProtoEntities = "infinispan.rdd.query.proto.entities"
   val Marshallers = "infinispan.rdd.query.proto.marshallers"
   val RegisterSchemas = "infinispan.rdd.query.proto.autoregister"
   val TargetEntity = "infinispan.dataset.sql.target_entity"

   private def readSequence[E](s: String, converter: String => E): Seq[E] = s.split(SequenceSeparator).map(converter).toSeq

   private def readDescriptors(s: String): Map[String, String] = {
      s.split(SequenceSeparator).map { entry =>
         val Array(k, v) = entry.split(EntrySeparator)
         (k, v)
      }.toMap
   }

   def apply(m: Map[String, String]): ConnectorConfiguration = {
      val config = new ConnectorConfiguration
      m.get(CacheName).foreach(config.setCacheName)
      m.get(ReadBatchSize).foreach(b => config.setReadBatchSize(b.toInt))
      m.get(WriteBatchSize).foreach(b => config.setWriteBatchSize(b.toInt))
      m.get(PartitionsPerServer).foreach(p => config.setPartitions(p.toInt))
      m.get(ProtoFiles).foreach(p => readDescriptors(p))
      m.get(Marshallers).foreach(m => readSequence[Class[_]](m, Class.forName).foreach(c => config.addMessageMarshaller(c.asInstanceOf[Class[BaseMarshaller[_]]])))
      m.get(ProtoEntities).foreach(p => readSequence[Class[_]](p, Class.forName).foreach(config.addProtoAnnotatedClass))
      m.get(RegisterSchemas).foreach(r => if (r.toBoolean) config.setAutoRegisterProto())
      m.get(TargetEntity).foreach(t => config.setTargetEntity(Class.forName(t)))

      m.keySet.filter(_.startsWith(HotRodClientConfigPrefix)).foreach(k => config.addHotRodClientProperty(k, m(k)))

      config
   }

}

