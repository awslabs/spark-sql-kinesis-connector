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

package org.apache.spark.sql.connector.kinesis;

import java.util.Random;

/**
 * Used to calculate full jitter backoff sleep durations.
 * Reuse from https://github.com/apache/flink-connector-aws.
 *
 * @see <a href="https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/">
 *     Exponential Backoff and Jitter </a>
 */
public class FullJitterBackoffManager {

  /** Random seed used to calculate backoff jitter for Kinesis operations. */
  private final Random seed = new Random();

  // the base backoff time in milliseconds
  private final long baseMillis = 1000L;
  // the maximum backoff time in milliseconds
  private long maxMillis = 3000L;
  // the power constant for exponential backoff
  private final double power = 1.5;

  /**
   * Calculates the sleep time for full jitter based on the given parameters.
   *
   * @param attempt the attempt number
   * @return the time to wait before trying again
   */
  public long calculateFullJitterBackoff(int attempt) {
    long exponentialBackoff = (long) Math.min(maxMillis, baseMillis * Math.pow(power, attempt));
    return (long) (seed.nextDouble() * exponentialBackoff);
  }

  /**
   * Puts the current thread to sleep for the specified number of millis. Simply delegates to
   * {@link Thread#sleep}.
   *
   * @param millisToSleep the number of milliseconds to sleep for
   * @throws InterruptedException
   */
  public void sleep(long millisToSleep) throws InterruptedException {
    Thread.sleep(millisToSleep);
  }
  
  public void setMaxMillis(long t) {
    maxMillis = t;
  }

  public long getMaxMillis() {
    return maxMillis;
  }
  
}

