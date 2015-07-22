package org.infinispan.spark.rdd

import java.net.SocketAddress
import java.util.Properties

import org.apache.spark.Partition

/**
 * @author gustavonalle
 */
class Location(val address: SocketAddress) extends Serializable {
   override def toString = s"Location($address)"
}

object Location {
   def apply(a: SocketAddress) = new Location(a)
}

class InfinispanPartition(val idx: Int, val location: Location, val segments: Option[Set[Integer]], val properties: Properties) extends Partition {
   override def index: Int = idx

   override def toString = s"InfinispanPartition($idx, $location, $segments, $properties)"
}

class SingleServerPartition(server: SocketAddress, properties: Properties) extends InfinispanPartition(0, Location(server), None, properties)
