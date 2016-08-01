package org.infinispan.spark.test

import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Random

/**
 * Trait to be mixed-in by tests requiring a cache populated with dictionary words.
 *
 * @author gustavonalle
 */
trait WordCache extends BeforeAndAfterAll {
   this: Suite with RemoteTest =>

   // https://github.com/bmarcot/haiku/blob/master/haiku.scala
   val adjs = List("autumn", "hidden", "bitter", "misty", "silent",
      "empty", "dry", "dark", "summer", "icy", "delicate", "quiet", "white", "cool",
      "spring", "winter", "patient", "twilight", "dawn", "crimson", "wispy",
      "weathered", "blue", "billowing", "broken", "cold", "damp", "falling",
      "frosty", "green", "long", "late", "lingering", "bold", "little", "morning",
      "muddy", "old", "red", "rough", "still", "small", "sparkling", "throbbing",
      "shy", "wandering", "withered", "wild", "black", "holy", "solitary",
      "fragrant", "aged", "snowy", "proud", "floral", "restless", "divine",
      "polished", "purple", "lively", "nameless", "puffy", "fluffy",
      "calm", "young", "golden", "avenging", "ancestral", "ancient", "argent",
      "reckless", "daunting", "short", "rising", "strong", "timber", "tumbling",
      "silver", "dusty", "celestial", "cosmic", "crescent", "double", "far", "half",
      "inner", "milky", "northern", "southern", "eastern", "western", "outer",
      "terrestrial", "huge", "deep", "epic", "titanic", "mighty", "powerful")

   val nouns = List("waterfall", "river", "breeze", "moon", "rain",
      "wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf",
      "dawn", "glitter", "forest", "hill", "cloud", "meadow", "glade",
      "bird", "brook", "butterfly", "bush", "dew", "dust", "field",
      "flower", "firefly", "feather", "grass", "haze", "mountain", "night", "pond",
      "darkness", "snowflake", "silence", "sound", "sky", "shape", "surf",
      "thunder", "violet", "wildflower", "wave", "water", "resonance",
      "sun", "wood", "dream", "cherry", "tree", "fog", "frost", "voice", "paper",
      "frog", "smoke", "star", "sierra", "castle", "fortress", "tiger", "day",
      "sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst",
      "protector", "drake", "dragon", "knight", "fire", "king", "jungle", "queen",
      "giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang",
      "cluster", "corona", "cosmos", "equinox", "horizon", "light", "nebula",
      "solstice", "spectrum", "universe", "magnitude", "parallax")

   protected def getNumEntries: Int

   private val random = new Random(System.currentTimeMillis())

   private def randomWordFrom(l: List[String]) = l(random.nextInt(l.size))

   private def pickNouns = (for (i <- 0 to random.nextInt(3)) yield randomWordFrom(nouns)).mkString(" ")

   lazy val wordsCache = getRemoteCache[Int,String]

   override protected def beforeAll(): Unit = {
      (1 to getNumEntries).par.foreach { i =>
         val contents = Seq(randomWordFrom(adjs), pickNouns).mkString(" ")
         wordsCache.put(i, contents)
      }
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      super.afterAll()
   }

}
