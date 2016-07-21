package org.infinispan.spark.serializer

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller

import scala.reflect.ClassTag

/**
 * A Spark Serializer based on JBoss Marshalling
 *
 * @author gustavonalle
 */
class JBossMarshallingSerializer extends Serializer with Externalizable {

   private val BufferSize = 512

   override def readExternal(in: ObjectInput) = new JBossMarshallingSerializer

   override def writeExternal(out: ObjectOutput) = {}

   override def newInstance(): SerializerInstance = new SerializerInstance {

      private val marshaller = new GenericJBossMarshaller()

      override def serializeStream(s: OutputStream): SerializationStream = new GenericMarshallerSerializationStream(s, marshaller)

      override def serialize[T: ClassTag](t: T): ByteBuffer = ByteBuffer.wrap(marshaller.objectToByteBuffer(t))

      override def deserializeStream(s: InputStream): DeserializationStream = new GenericMarshallerDeSerializationStream(s, marshaller)

      override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
         marshaller.objectFromByteBuffer(bytes.array()).asInstanceOf[T]

      override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
         new GenericJBossMarshaller(loader).objectFromByteBuffer(bytes.array()).asInstanceOf[T]

   }

   private[serializer] class GenericMarshallerSerializationStream(val out: OutputStream, val marshaller: GenericJBossMarshaller) extends SerializationStream {
      private[this] val objectOutput: ObjectOutput = marshaller.startObjectOutput(out, true, BufferSize)

      override def writeObject[T: ClassTag](t: T): SerializationStream = {
         marshaller.objectToObjectStream(t, objectOutput)
         this
      }

      override def flush(): Unit = marshaller.finishObjectOutput(objectOutput)

      override def close(): Unit = {
         objectOutput.close()
         out.close()
      }
   }

   private[serializer] class GenericMarshallerDeSerializationStream(val in: InputStream, val marshaller: GenericJBossMarshaller) extends DeserializationStream {
      private[this] val objectInput: ObjectInput = marshaller.startObjectInput(in, true)

      override def readObject[T: ClassTag](): T = marshaller.objectFromObjectStream(objectInput).asInstanceOf[T]

      override def close(): Unit = objectInput.close()
   }

}

