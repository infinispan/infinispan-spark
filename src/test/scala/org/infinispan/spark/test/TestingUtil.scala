package org.infinispan.spark.test

import java.util.function.BooleanSupplier

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object TestingUtil {

   val DefaultDuration = 60 seconds
   val waitBetweenRetries = 500

   def waitForCondition(command: () => Boolean, duration: Duration): Unit = {
      val NumTimes = duration.toMillis.toInt / waitBetweenRetries
      @tailrec
      def waitForCondition(numTimes: Int, sleep: Boolean): Unit = {
         if (sleep) Thread.sleep(waitBetweenRetries)
         Try(command.apply()) match {
            case Success(true) =>
            case Success(false) if numTimes == 0 => throw new Exception("Timeout waiting for condition.")
            case Failure(e) if numTimes == 0 => throw new Exception("Given up trying to execute command.", e)
            case _ => waitForCondition(numTimes - 1, sleep = true)
         }
      }
      waitForCondition(NumTimes, sleep = false)
   }

   def waitForCondition(command: () => Boolean): Unit = waitForCondition(command, DefaultDuration)

   def waitForCondition(command: BooleanSupplier): Unit = waitForCondition(toScala(command), DefaultDuration)

   private def toScala(f: BooleanSupplier) = new (() => Boolean) {
      override def apply() = f.getAsBoolean
   }
}
