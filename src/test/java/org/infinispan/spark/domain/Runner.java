package org.infinispan.spark.domain;


import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

/**
 * @author gustavonalle
 */
@ProtoName("runner")
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
}
