package org.infinispan.spark.examples.twitter;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author gustavonalle
 */
@SerializeWith(Tweet.TweetExternalizer.class)
public class Tweet {
   private final Long id;
   private final String user;
   private final String country;
   private final Long retweet;
   private final String text;

   public Long getId() {
      return id;
   }

   public String getUser() {
      return user;
   }

   public String getCountry() {
      return country;
   }

   public Long getRetweet() {
      return retweet;
   }

   public String getText() {
      return text;
   }

   public Tweet(Long id, String user, String country, Long retweet, String text) {
      this.id = id;
      this.user = user;
      this.country = country;
      this.retweet = retweet;
      this.text = text;
   }

   public static class TweetExternalizer implements Externalizer<Tweet> {
      @Override
      public void writeObject(ObjectOutput output, Tweet object) throws IOException {
         UnsignedNumeric.writeUnsignedLong(output, object.id);
         output.writeUTF(object.user);
         output.writeUTF(object.country);
         UnsignedNumeric.writeUnsignedLong(output, object.retweet);
         output.writeUTF(object.text);
      }

      @Override
      public Tweet readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         long id = UnsignedNumeric.readUnsignedLong(input);
         String user = input.readUTF();
         String country = input.readUTF();
         long retweet = UnsignedNumeric.readUnsignedLong(input);
         String text = input.readUTF();
         return new Tweet(id, user, country, retweet, text);
      }
   }
}
