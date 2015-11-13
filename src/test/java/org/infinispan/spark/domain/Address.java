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
@SerializeWith(Address.AddressExternalizer.class)
public class Address {

   private final String street;
   private final int number;
   private final String country;

   public Address(String street, int number, String country) {
      this.street = street;
      this.number = number;
      this.country = country;
   }

   public String getStreet() {
      return street;
   }

   public int getNumber() {
      return number;
   }

   public String getCountry() {
      return country;
   }

   public static class AddressExternalizer implements Externalizer<Address> {

      @Override
      public void writeObject(ObjectOutput output, Address object) throws IOException {
         output.writeUTF(object.street);
         UnsignedNumeric.writeUnsignedInt(output, object.number);
         output.writeUTF(object.country);
      }

      @Override
      public Address readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String street = input.readUTF();
         int number = UnsignedNumeric.readUnsignedInt(input);
         String country = input.readUTF();
         return new Address(street, number, country);
      }
   }
}
