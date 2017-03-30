package org.infinispan.spark.stream

import java.nio._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.annotation._
import org.infinispan.client.hotrod.event.{ClientCacheEntryCustomEvent, ClientEvent}
import org.infinispan.commons.io.UnsignedNumeric
import org.infinispan.spark._
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.RemoteCacheManagerBuilder

/**
  * @author gustavonalle
  */
class InfinispanInputDStream[K, V](@transient val ssc_ : StreamingContext, storage: StorageLevel,
                                   configuration: ConnectorConfiguration, includeState: Boolean = false)
  extends ReceiverInputDStream[(K, V, ClientEvent.Type)](ssc_) {
   override def getReceiver(): Receiver[(K, V, ClientEvent.Type)] = new EventsReceiver(storage, configuration, includeState)
}

private class EventsReceiver[K, V](storageLevel: StorageLevel, configuration: ConnectorConfiguration, includeState: Boolean)
  extends Receiver[(K, V, ClientEvent.Type)](storageLevel) {

   @transient private lazy val listener = if (includeState) new EventListenerWithState else new EventListenerWithoutState

   @transient private var cacheManager: RemoteCacheManager = _

   override def onStart(): Unit = {
      cacheManager = RemoteCacheManagerBuilder.create(configuration)
      val remoteCache = getCache[K, V](configuration, cacheManager)
      remoteCache.addClientListener(listener)
   }

   override def onStop(): Unit = {
      if (cacheManager != null) {
         cacheManager.stop()
         cacheManager = null
      }
   }

   private sealed trait EventListener {

      @ClientCacheEntryRemoved
      @ClientCacheEntryExpired
      def onRemove(event: ClientCacheEntryCustomEvent[Array[Byte]]) {
         emitEvent(event, ignoreValue = true)
      }

      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      def onAddModify(event: ClientCacheEntryCustomEvent[Array[Byte]]) {
         emitEvent(event, ignoreValue = false)
      }

      private def emitEvent(event: ClientCacheEntryCustomEvent[Array[Byte]], ignoreValue: Boolean) = {
         val marshaller = cacheManager.getMarshaller
         val eventData = event.getEventData
         val rawData = ByteBuffer.wrap(eventData)
         val rawKey = readElement(rawData)
         val key: K = marshaller.objectFromByteBuffer(rawKey).asInstanceOf[K]
         val value = if (!ignoreValue) {
            val rawValue = readElement(rawData)
            marshaller.objectFromByteBuffer(rawValue).asInstanceOf[V]
         } else null.asInstanceOf[V]

         store((key, value, event.getType))
      }

      private def readElement(in: ByteBuffer): Array[Byte] = {
         val length = UnsignedNumeric.readUnsignedInt(in)
         val element = new Array[Byte](length)
         in.get(element)
         element
      }
   }

   @ClientListener(converterFactoryName = "___eager-key-value-version-converter", useRawData = true, includeCurrentState = true)
   private class EventListenerWithState extends EventListener

   @ClientListener(converterFactoryName = "___eager-key-value-version-converter", useRawData = true, includeCurrentState = false)
   private class EventListenerWithoutState extends EventListener

}
