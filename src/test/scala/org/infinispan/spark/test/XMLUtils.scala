package org.infinispan.spark.test

import java.io.File

import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.xml.{Elem, Node, XML}
import scala.language.postfixOps

object XMLUtils {

   private def addChildToNode(element: Node, elementName: String, attributeName: String, attributeValue: String, elementToAdd: Node) = {
      object Rule extends RewriteRule {
         override def transform(n: Node): Seq[Node] = n match {
            case Elem(prefix, en, att, scope, child@_*)
               if en == elementName && att.asAttrMap.exists(t => t._1 == attributeName && t._2 == attributeValue) =>
               Elem(prefix, en, att, scope, child.isEmpty, elementToAdd ++ child: _*)
            case other => other
         }
      }
      object Transform extends RuleTransformer(Rule)
      Transform(element)
   }

   /**
    * Adds a replicated cache configuration to the server config. Temporary until the REST API supports dynamic creation
    * of templates
    */
   def addCacheTemplate(cacheContainer: String, configFile: File): Unit = {
      val xmlFile = XML.loadFile(configFile)
      val exists = ((xmlFile \\ "cache-container").filter(n => n.attributes.asAttrMap.exists {
         case (k, v) => k.equals("name") && v.equals(cacheContainer)
      }) \ "replicated-cache-configuration" \ "@name" text) == "replicated"

      val cacheConfig = <replicated-cache-configuration name="replicated"/>

      if (!exists) {
         val newXML = XMLUtils.addChildToNode(xmlFile, "cache-container", "name", cacheContainer, cacheConfig)
         XML.save(configFile.getAbsolutePath, newXML, "UTF-8")
      }
   }
}
