package org.infinispan.spark.domain;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

/**
 * @author gustavonalle
 */
@ProtoName("runner")
@SerializeWith(Runner.RunnerExternalizer.class)
public class Runner implements Serializable {

   private String name;

   private Boolean finished;

   private int finishTimeSeconds;

   private int age;

   public Runner() {
   }

   public Runner(String name, Boolean finished, int finishTimeSeconds, int age) {
      this.name = name;
      this.finished = finished;
      this.finishTimeSeconds = finishTimeSeconds;
      this.age = age;
   }

   @ProtoField(number = 1, required = true)
   public String getName() {
      return name;
   }

   @ProtoField(number = 2, required = true)
   public Boolean getFinished() {
      return finished;
   }

   @ProtoField(number = 3, required = true)
   public int getFinishTimeSeconds() {
      return finishTimeSeconds;
   }

   @ProtoField(number = 4, required = true)
   public int getAge() {
      return age;
   }

   public void setName(String name) {
      this.name = name;
   }

   public void setFinished(Boolean finished) {
      this.finished = finished;
   }

   public void setFinishTimeSeconds(int finishTimeSeconds) {
      this.finishTimeSeconds = finishTimeSeconds;
   }

   public void setAge(int age) {
      this.age = age;
   }

   @Override
   public String toString() {
      return "Runner{" +
            "name='" + name + '\'' +
            ", finished=" + finished +
            ", finishTimeSeconds=" + finishTimeSeconds +
            ", age=" + age +
            '}';
   }

   public static class RunnerExternalizer implements Externalizer<Runner> {
      @Override
      public void writeObject(ObjectOutput output, Runner object) throws IOException {
         output.writeUTF(object.name);
         output.writeBoolean(object.finished);
         output.writeInt(object.finishTimeSeconds);
         output.writeInt(object.age);
      }

      @Override
      public Runner readObject(ObjectInput input) throws IOException {
         String name = input.readUTF();
         boolean finished = input.readBoolean();
         int finishedSeconds = input.readInt();
         int age = input.readInt();
         return new Runner(name, finished, finishedSeconds, age);
      }
   }
}
