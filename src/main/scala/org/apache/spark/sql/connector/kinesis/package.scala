/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connector

import java.time.LocalDate
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle
import java.util.LinkedHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang3.StringUtils

import org.apache.spark.internal.Logging
import org.apache.spark.util.UninterruptibleThread
package object kinesis extends Logging {

  val DEFAULT_SHUTDOWN_WAIT_TIMEOUT = 300 // seconds
  
  @tailrec
  def throwableIsCausedBy[T: ClassTag](e: Throwable): Boolean = e match {
    case null => false
    case _: T => true
    case _ => throwableIsCausedBy[T](e.getCause)
  }

  def reportTimeTaken[T](operation: String)(body: => T): T = {
    val startTime = System.currentTimeMillis()
    val result = body
    val endTime = System.currentTimeMillis()
    val timeTaken = math.max(endTime - startTime, 0)

    logInfo(s"reportTimeTaken $operation took $timeTaken ms")
    result
  }

  def tryAndIgnoreError[T](message: String)(body: => T): Unit = {
    Try {
      body
    } match {
      case Failure(f) =>
        logWarning(s"${message} error: ", f)
      case Success(_) =>
    }
  }

  def getTimestampValue(timestampStr: String): Long = {

    val dateStr = timestampStr.substring(0, 10)
    val f = DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT)
    LocalDate.parse(dateStr, f)

    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
    val ts = ZonedDateTime.parse(timestampStr, format).toInstant.toEpochMilli
    ts
  }
  def getRegionNameByEndpoint(endpoint: String): String = {
    if (StringUtils.isEmpty(endpoint)) {
      throw new IllegalArgumentException(s"Empty endpoint url received. Cannot parse region")
    }

    val serviceNameEndIndex: Int = endpoint.indexOf(".")

    if (serviceNameEndIndex == -1) {
      throw new IllegalArgumentException(s"Invalid endpoint url received. Cannot parse region: $endpoint")
    }

    val regionEndIndex = endpoint.indexOf(".", serviceNameEndIndex + 1)

    if (regionEndIndex == -1) {
      throw new IllegalArgumentException(s"Invalid endpoint url received. Cannot parse region: $endpoint")
    }
    endpoint.substring(serviceNameEndIndex + 1, regionEndIndex)
  }

  def shutdownAndAwaitTermination(pool: ExecutorService, await_timeout: Int = DEFAULT_SHUTDOWN_WAIT_TIMEOUT): Unit = {
    if (!pool.isTerminated) {
      pool.shutdown() // Disable new tasks from being submitted

      try {
        // Wait a while for existing tasks to terminate
        if (!pool.awaitTermination(await_timeout, TimeUnit.SECONDS)) {
          pool.shutdownNow // Cancel currently executing tasks

          // Wait a while for tasks to respond to being cancelled
          if (!pool.awaitTermination(await_timeout, TimeUnit.SECONDS)) {
            logError(s"Thread pool did not stop properly: ${pool.toString}.")
          }
        }
      } catch {
        case _: InterruptedException =>
          // (Re-)Cancel if current thread also interrupted
          pool.shutdownNow
          // Preserve interrupt status
          Thread.currentThread.interrupt()
      }
    }
  }

  def uninterruptibleSingleThreadExecutor(threadName: String): ExecutorService = {
    Executors.newSingleThreadExecutor((r: Runnable) => {
      val t = new UninterruptibleThread(threadName) {
        override def run(): Unit = {
          r.run()
        }
      }
      t.setDaemon(true)
      t
    })
  }


  private val threadPoolCacheLock = new Object()

  // visible for test only
  val DEFAULT_THREAD_POOL_CACHE_MAXSIZE: Int = 1<<6

  // a LinkedHashMap with accessOrder = true to build a LRU cache
  private val threadPoolCache =
    new LinkedHashMap[String, (Int, ExecutorService)](DEFAULT_THREAD_POOL_CACHE_MAXSIZE, 1.0f, true)

  private val terminateThreadPoolExecutor = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("terminateThreadPoolExecutor").build()
  )



  def getFixedUninterruptibleThreadPoolFromCache(threadPoolName: String,
                                                 threadNumbers: Int): ExecutorService = threadPoolCacheLock.synchronized {
    if (threadPoolCache.containsKey(threadPoolName)) {
      val (tpThreadNumber, tpExecutorService) = threadPoolCache.get(threadPoolName)

      if (tpExecutorService.isShutdown) {
        threadPoolCache.remove(threadPoolName)
      } else {
        if (tpThreadNumber != threadNumbers) {
          logWarning(s"use thread pool from cache with threadNumbers ${tpThreadNumber}, " +
            s"expected threadNumbers ${threadNumbers}")
        }
        logDebug(s"reuse thread pool ${threadPoolName} from cache")

        return tpExecutorService
      }
    }

    // remove the least recently used
    if(threadPoolCache.size() >= DEFAULT_THREAD_POOL_CACHE_MAXSIZE) {
      val firstTp = threadPoolCache.keySet().iterator().next()
      val exec = threadPoolCache.get(firstTp)._2
      threadPoolCache.remove(firstTp)
      terminateThreadPoolExecutor.submit(
        new Runnable {
          override def run(): Unit = {
            try {
              logDebug(s"shutting down the thread pool ${firstTp} in threadPoolCache.")
              shutdownAndAwaitTermination(exec)
              logDebug(s"the thread pool ${firstTp} shut down.")
            } catch {
              case NonFatal(e) =>
                logWarning("Error running shutdownAndAwaitTermination", e)
            }
          }
        }
      )
    }

    val tpExecutorService = Executors.newFixedThreadPool(
      threadNumbers,
      (r: Runnable) => {
        val t = new UninterruptibleThread(s"thread-pool-${threadPoolName}") {
          override def run(): Unit = {
            r.run()
          }
        }
        t.setDaemon(true)
        t
      }
    )

    threadPoolCache.put(threadPoolName, (threadNumbers, tpExecutorService))

    tpExecutorService

    }
}

