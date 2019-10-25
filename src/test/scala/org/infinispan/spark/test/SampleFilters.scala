package org.infinispan.spark.test

import java.io.Serializable

import org.infinispan.filter._
import org.infinispan.metadata.Metadata
import org.infinispan.spark.domain.{Person, Runner}

@NamedFactory(name = "sample-filter-factory")
class SampleFilterFactory extends KeyValueFilterConverterFactory[Int, Runner, String] with Serializable {
   override def getFilterConverter = new SampleFilter

   class SampleFilter extends AbstractKeyValueFilterConverter[Int, Runner, String] with Serializable {
      override def filterAndConvert(k: Int, v: Runner, metadata: Metadata): String = if (k % 2 == 0) v.getName else null
   }

}

@NamedFactory(name = "sample-filter-factory-with-param")
class SampleFilterFactoryWithParam extends ParamKeyValueFilterConverterFactory[Int, Runner, String] with Serializable {
   override def getFilterConverter(params: Array[AnyRef]): KeyValueFilterConverter[Int, Runner, String] = new SampleFilterParam(params)

   class SampleFilterParam(params: Array[AnyRef]) extends AbstractKeyValueFilterConverter[Int, Runner, String] with Serializable {
      override def filterAndConvert(k: Int, v: Runner, metadata: Metadata): String = {
         val length = params(0).asInstanceOf[Int]
         v.getName.substring(0, length)
      }
   }

}

@NamedFactory(name = "age-filter")
class AgeFilterFactory extends ParamKeyValueFilterConverterFactory[Int, Person, Person] with Serializable {
   override def getFilterConverter(params: Array[AnyRef]): KeyValueFilterConverter[Int, Person, Person] =
      new AgeFilter(params(0).asInstanceOf[Int], params(1).asInstanceOf[Int])

   class AgeFilter(minimumAge: Int, maximumAge: Int) extends AbstractKeyValueFilterConverter[Int, Person, Person] with Serializable {
      override def filterAndConvert(key: Int, value: Person, metadata: Metadata): Person = {
         val age = value.getAge
         if (age >= minimumAge && age <= maximumAge) value else null
      }
   }

}

object FilterDefs {
   val list = List(
      new FilterDef(factoryClass = classOf[SampleFilterFactory], classes = Seq(classOf[SampleFilterFactory#SampleFilter])),
      new FilterDef(classOf[SampleFilterFactoryWithParam], classes = Seq(classOf[SampleFilterFactoryWithParam#SampleFilterParam])),
      new FilterDef(factoryClass = classOf[AgeFilterFactory], classes = Seq(classOf[AgeFilterFactory#AgeFilter]))
   )
}