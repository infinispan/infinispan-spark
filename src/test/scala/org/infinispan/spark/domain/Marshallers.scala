package org.infinispan.spark.domain

import org.infinispan.protostream.MessageMarshaller
import org.infinispan.protostream.MessageMarshaller.{ProtoStreamReader, ProtoStreamWriter}

class PersonMarshaller extends MessageMarshaller[Person] {
   override def writeTo(writer: ProtoStreamWriter, person: Person): Unit = {
      writer.writeString("name", person.getName)
      writer.writeInt("age", person.getAge)
      writer.writeObject("address", person.getAddress, classOf[Address])
   }

   override def readFrom(reader: ProtoStreamReader): Person = {
      val name = reader.readString("name")
      val age = reader.readInt("age")
      val address = reader.readObject("address", classOf[Address])
      new Person(name, age, address)
   }

   override def getJavaClass = classOf[Person]

   override def getTypeName: String = getJavaClass.getName
}

class AddressMarshaller extends MessageMarshaller[Address] {

   def readFrom(reader: MessageMarshaller.ProtoStreamReader): Address = {
      val street = reader.readString("street")
      val number = reader.readInt("number")
      val country = reader.readString("country")
      new Address(street, number, country)
   }

   def writeTo(writer: MessageMarshaller.ProtoStreamWriter, address: Address) {
      writer.writeString("street", address.getStreet)
      writer.writeInt("number", address.getNumber)
      writer.writeString("country", address.getCountry)
   }

   def getJavaClass = classOf[Address]

   def getTypeName: String = getJavaClass.getName

}

