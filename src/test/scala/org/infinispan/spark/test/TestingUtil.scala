package org.infinispan.spark.test

import java.util.function.BooleanSupplier

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object TestingUtil {

   def waitForCondition(command: () => Boolean, numTimes: Int = 60, waitBetweenRetries: Int = 1000): Unit = {
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
      waitForCondition(numTimes, sleep = false)
   }

   def waitForCondition(command: BooleanSupplier): Unit = waitForCondition(toScala(command))

   private def toScala(f: BooleanSupplier) = new (() => Boolean) {
      override def apply() = f.getAsBoolean
   }
}
