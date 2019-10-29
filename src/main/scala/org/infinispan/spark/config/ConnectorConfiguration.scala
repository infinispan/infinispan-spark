package org.infinispan.spark.config


import java.util.Properties

import org.infinispan.client.hotrod.impl.ConfigurationProperties._
import org.infinispan.commons.marshall.Marshaller
import org.infinispan.protostream.BaseMarshaller
import scala.collection.JavaConverters._
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
   private var autoCreateCacheFromConfig: String = ""
   private var autoCreateCacheFromTemplate: String = ""
   private var keyMediaType: String = _
   private var valueMediaType: String = _
   private var keyMarshaller: Class[_ <: Marshaller] = _
   private var valueMarshaller: Class[_ <: Marshaller] = _

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

   def setAutoCreateCacheFromConfig(configuration: String): ConnectorConfiguration = {
      this.autoCreateCacheFromConfig = configuration
      this
   }

   def setAutoCreateCacheFromTemplate(template: String): ConnectorConfiguration = {
      this.autoCreateCacheFromTemplate = template
      this
   }

   def setKeyMediaType(keyMediaType: String): ConnectorConfiguration = {
      this.keyMediaType = keyMediaType
      this
   }

   def setValueMediaType(valueMediaType: String): ConnectorConfiguration = {
      this.valueMediaType = valueMediaType
      this
   }

   def setKeyMarshaller(marshaller: Class[_ <: Marshaller]): ConnectorConfiguration = {
      this.keyMarshaller = marshaller
      this
   }

   def setValueMarshaller(marshaller: Class[_ <: Marshaller]): ConnectorConfiguration = {
      this.valueMarshaller = marshaller
      this
   }

   def hasCustomFormat: Boolean = keyMarshaller != null || valueMarshaller != null || keyMediaType != null || valueMediaType != null

   import ConnectorConfiguration._


   def toStringsMap: Map[String, String] = {
      val stringMap = mutable.Map[String, String]()

      stringMap += CacheName -> cacheName
      stringMap += AutoCreateCacheConfig -> autoCreateCacheFromConfig
      stringMap += AutoCreateCacheTemplate -> autoCreateCacheFromTemplate
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

      hotRodClientProps.asScala.foreach { case (k, v) => stringMap += k -> v }

      stringMap.toMap
   }


   def getCacheName: String = cacheName

   def getAutoCreateCacheFromConfig: String = autoCreateCacheFromConfig

   def getAutoCreateCacheFromTemplate: String = autoCreateCacheFromTemplate

   def getReadBatchSize: Int = readBatchSize

   def getWriteBatchSize: Int = writeBatchSize

   def getServerPartitions: Int = serverPartitions

   def getRegisterSchemas: Boolean = autoRegister

   def usesProtobuf: Boolean = getProtoEntities.nonEmpty || descriptors.nonEmpty

   def getProtoEntities: mutable.Set[Class[_]] = protoAnnotatedClasses

   def getProtoFiles: Map[String, String] = descriptors.toMap

   def getMarshallers: mutable.Set[Class[_ <: BaseMarshaller[_]]] = messageMarshallers

   def getTargetEntity: Class[_] = targetEntity

   def getHotRodClientProperties: Properties = hotRodClientProps

   def getServerList: String = getHotRodClientProperties.getProperty(SERVER_LIST)

   def getKeyMediaType: String = keyMediaType

   def getValueMediaType: String = valueMediaType

   def getKeyMarshaller: Class[_ <: Marshaller] = keyMarshaller

   def getValueMarshaller: Class[_ <: Marshaller] = valueMarshaller


   override def toString = s"ConnectorConfiguration($cacheName, $readBatchSize, $writeBatchSize, $serverPartitions, $autoRegister, $targetEntity, $messageMarshallers, $protoAnnotatedClasses, $descriptors, $hotRodClientProps, $autoCreateCacheFromConfig, $autoCreateCacheFromTemplate, $keyMediaType, $valueMediaType, $keyMarshaller, $valueMarshaller)"
}

object ConnectorConfiguration {


   private val SequenceSeparator = ","
   private val EntrySeparator = ":"

   private val HotRodClientConfigPrefix = "infinispan.client"

   val CacheName = "infinispan.rdd.cacheName"
   val AutoCreateCacheConfig = "infinispan.rdd.auto_create_cache_cfg"
   val AutoCreateCacheTemplate = "infinispan.rdd.auto_create_cache_template"
   val ReadBatchSize = "infinispan.rdd.read_batch_size"
   val WriteBatchSize = "infinispan.rdd.write_batch_size"
   val PartitionsPerServer = "infinispan.rdd.number_server_partitions"
   val ProtoFiles = "infinispan.rdd.query.proto.protofiles"
   val ProtoEntities = "infinispan.rdd.query.proto.entities"
   val Marshallers = "infinispan.rdd.query.proto.marshallers"
   val RegisterSchemas = "infinispan.rdd.query.proto.autoregister"
   val TargetEntity = "infinispan.dataset.sql.target_entity"
   val KeyMediaType = "infinispan.rdd.key_media_type"
   val ValueMediaType = "infinispan.rdd.value_media_type"
   val KeyMarshaller = "infinispan.rdd.key_marshaller"
   val ValueMarshaller = "infinispan.rdd.value_marshaller"

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
      m.get(AutoCreateCacheConfig).foreach(config.setAutoCreateCacheFromConfig)
      m.get(AutoCreateCacheTemplate).foreach(config.setAutoCreateCacheFromTemplate)
      m.get(ReadBatchSize).foreach(b => config.setReadBatchSize(b.toInt))
      m.get(WriteBatchSize).foreach(b => config.setWriteBatchSize(b.toInt))
      m.get(PartitionsPerServer).foreach(p => config.setPartitions(p.toInt))
      m.get(ProtoFiles).foreach(p => readDescriptors(p))
      m.get(Marshallers).foreach(m => readSequence[Class[_]](m, Class.forName).foreach(c => config.addMessageMarshaller(c.asInstanceOf[Class[BaseMarshaller[_]]])))
      m.get(ProtoEntities).foreach(p => readSequence[Class[_]](p, Class.forName).foreach(config.addProtoAnnotatedClass))
      m.get(RegisterSchemas).foreach(r => if (r.toBoolean) config.setAutoRegisterProto())
      m.get(TargetEntity).foreach(t => config.setTargetEntity(Class.forName(t)))
      m.get(KeyMediaType).foreach(config.setKeyMediaType)
      m.get(ValueMediaType).foreach(config.setValueMediaType)
      m.get(KeyMarshaller).foreach(m => config.setKeyMarshaller(Class.forName(m).asInstanceOf[Class[_ <: Marshaller]]))
      m.get(ValueMarshaller).foreach(m => config.setValueMarshaller(Class.forName(m).asInstanceOf[Class[_ <: Marshaller]]))
      m.keySet.filter(_.startsWith(HotRodClientConfigPrefix)).foreach(k => config.addHotRodClientProperty(k, m(k)))

      config
   }

}

