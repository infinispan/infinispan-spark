package org.infinispan.spark.domain;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author gustavonalle
 */
@SerializeWith(Person.PersonExternalizer.class)
public class Person {

   private final String name;
   private final Integer age;
   private final Address address;

   public Person(String name, Integer age, Address address) {
      this.name = name;
      this.age = age;
      this.address = address;
   }

   public String getName() {
      return name;
   }

   public Integer getAge() {
      return age;
   }

   public Address getAddress() {
      return address;
   }

   public static class PersonExternalizer implements Externalizer<Person> {

      @Override
      public void writeObject(ObjectOutput output, Person object) throws IOException {
         output.writeUTF(object.getName());
         UnsignedNumeric.writeUnsignedInt(output, object.getAge());
         output.writeObject(object.getAddress());
      }

      @Override
      public Person readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String name = input.readUTF();
         int age = UnsignedNumeric.readUnsignedInt(input);
         Address address = (Address) input.readObject();
         return new Person(name, age, address);
      }
   }

}
