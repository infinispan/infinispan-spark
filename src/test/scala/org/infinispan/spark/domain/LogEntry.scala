package org.infinispan.spark.domain

import java.io.{ObjectInput, ObjectOutput}
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.infinispan.commons.io.UnsignedNumeric
import org.infinispan.commons.marshall._

/**
 * @author gustavonalle
 */
@SerializeWith(classOf[LogEntrySerializer])
class LogEntry(val date: LocalDate, var opCode: Int, val userAgent: String, val domain: String)

class LogEntrySerializer extends Externalizer[LogEntry] {
   override def writeObject(output: ObjectOutput, obj: LogEntry): Unit = {
      output.writeObject(obj.date)
      UnsignedNumeric.writeUnsignedInt(output, obj.opCode)
      output.writeUTF(obj.userAgent)
      output.writeUTF(obj.domain)
   }

   override def readObject(input: ObjectInput): LogEntry = {
      val date = input.readObject().asInstanceOf[LocalDate]
      val opCode = UnsignedNumeric.readUnsignedInt(input)
      val userAgent = input.readUTF()
      val domain = input.readUTF()
      new LogEntry(date, opCode, userAgent, domain)
   }
}

object EntryGenerator {

   val browser = Set("Firefox", "Chrome", "MSIE")
   val domainNames = Set("no-ip.org", "dnf.it", "google.com", "localhost")

   def generate(numEntries: Int, errorCondition: LogEntry => Boolean, startDate: LocalDate, endDate: LocalDate) = {
      val userAgentsIterator = circularIterator(browser)
      val domainNamesIterator = circularIterator(domainNames)
      val diffDays = ChronoUnit.DAYS.between(startDate, endDate)
      val daysPerEntry = diffDays.toFloat / numEntries.toFloat
      (1 to numEntries).map { i =>
         val browser = userAgentsIterator.next()
         val domain = domainNamesIterator.next()
         val dateTime = startDate.plusDays(Math.floor(daysPerEntry * i).toInt)
         val entry = new LogEntry(dateTime, 0, browser, domain)
         val op = if (errorCondition(entry)) 500 else 200
         entry.opCode = op
         entry
      }.toList
   }

   private def circularIterator[T](s: Set[T]) = Iterator.continually(s).flatten

}
