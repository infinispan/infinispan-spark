package org.infinispan.spark.domain

import org.infinispan.protostream.annotations.{ProtoField, ProtoName}

import scala.annotation.meta.beanGetter
import scala.beans.BeanProperty

@ProtoName(value = "User")
class User(@(ProtoField@beanGetter)(number = 1, required = true) @BeanProperty var name: String,
           @(ProtoField@beanGetter)(number = 2, required = true) @BeanProperty var age: Int) {

   def this() = {
      this(name = "", age = -1)
   }
}
