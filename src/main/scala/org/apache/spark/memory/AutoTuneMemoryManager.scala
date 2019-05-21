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

package org.apache.spark.memory

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

  /**
    * * A [[MemoryManager]] that dynamically partitions the heap space into disjoint regions.
    *
    * The sizes of the execution and storage regions are determined through
    * `spark.auto.memoryFraction` and `spark.auto.memoryFraction` respectively. The two
    * regions are cleanly separated such that neither usage can borrow memory from the other.
    *
    * After each task done, the memory partition adjusts to minimize the GC time.
    */
private[spark] class AutoTuneMemoryManager(
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

    def this(conf: SparkConf, numCores: Int) {
      this(
        conf,
        AutoTuneMemoryManager.getMaxHeap(conf),
        AutoTuneMemoryManager.getInitialStorageMemory(conf),
        numCores)
    }

    private def assertInvariants(): Unit = {
      assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
      assert(
        offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
    }

    assertInvariants()

    private val BUFFER_SIZE = conf.getInt("spark.auto.buffer", 5)
    val STEP = conf.getDouble("spark.auto.step", 0.05)
    var buffer = new util.ArrayList[(Double, Double)](BUFFER_SIZE).asScala
    var preRatio: Double = 0.0
    var preDecision: Boolean = true
    var heapStorageMemory: Long = onHeapStorageMemoryPool.poolSize

    /**
      * Adjust the bar between execution and storage memory after each task done.
      * @param taskTime is the execution time of the current task.
      * @param GCTime is the GC time of the current task.
      */
    def barChange(taskTime: Double, GCTime: Double) : Unit = synchronized {
      val curRatio = GCTime/taskTime
      if (curRatio==preRatio) {
        return
      }
      else if ((curRatio<preRatio & preDecision) |
        (curRatio>preRatio & !preDecision)) {
        setUp(STEP)
        preDecision = true
        preRatio = curRatio
      }
      else {
        setDown(STEP)
        preDecision = false
        preRatio = curRatio
      }
      logDebug(s"After adjustment, " +
        s"storagePoolSize is ${onHeapStorageMemoryPool.poolSize/1024/1024}," +
        s"heapStorageMemory is ${heapStorageMemory/1024/1024}")
      maintainBuffer(taskTime, GCTime)
    }

    /**
      * increase storage fraction by the [[STEP]] percentage of maxHeap.
      * @param step
      */
    def setUp(step: Double): Unit = {
      val storageIncreased = (step * maxHeapMemory).toLong
      if (onHeapStorageMemoryPool.poolSize + storageIncreased < maxHeapMemory) {
        onHeapStorageMemoryPool.incrementPoolSize(storageIncreased)
        onHeapExecutionMemoryPool.decrementPoolSize(storageIncreased)
        if (onHeapStorageMemoryPool.memoryUsed/onHeapStorageMemoryPool.poolSize > 0.8) {
          heapStorageMemory += storageIncreased
        }
      }
    }

    def setDown(step: Double): Unit = {
      val storageDecreased = (step * maxHeapMemory).toLong
      if (onHeapStorageMemoryPool.memoryFree - storageDecreased > 0) {
        val memoryEvict = storageDecreased - onHeapStorageMemoryPool.memoryFree
        if (memoryEvict > 0) {
          onHeapStorageMemoryPool.freeSpaceToShrinkPool(memoryEvict)
        } else {
          heapStorageMemory -= storageDecreased
        }
        onHeapStorageMemoryPool.decrementPoolSize(storageDecreased)
        onHeapExecutionMemoryPool.incrementPoolSize(storageDecreased)
//        if (heapStorageMemory>=onHeapStorageMemoryPool.poolSize | heapStorageMemory < 0) {
//          heapStorageMemory = onHeapStorageMemoryPool.poolSize
//        }
      }
    }

    def maintainBuffer(taskTime: Double, GCTime: Double): Unit = {
      var taskSum: Double = 0
      var GCSum: Double = 0
      if (buffer.size == BUFFER_SIZE) {
        buffer.remove(0)
      }
      buffer.append((taskTime, GCTime))
      for (t <- buffer) {
        taskSum += t._1
        GCSum += t._2
      }
      preRatio = taskSum/GCSum
    }

    override def maxOnHeapStorageMemory: Long = synchronized {
      maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
    }

    override def maxOffHeapStorageMemory: Long = synchronized {
      maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
    }

  override private[memory] def acquireExecutionMemory
  (numBytes: Long,
   taskAttemptId: Long,
   memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        val memoryBorrow = math.min(extraMemoryNeeded,
          storagePool.poolSize - heapStorageMemory)
        if (memoryBorrow > 0) {
          // Only reclaim as much space as is necessary and available:
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(memoryBorrow)
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    def computeMaxExecutionPoolSize(): Long = {
      maxMemory - heapStorageMemory
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, () => computeMaxExecutionPoolSize)
  }


  override def acquireStorageMemory
  (blockId: BlockId,
   numBytes: Long,
   memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapStorageMemory)
    }
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
        numBytes - storagePool.memoryFree)
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    if (onHeapStorageMemoryPool.memoryUsed > heapStorageMemory) {
      heapStorageMemory = onHeapStorageMemoryPool.memoryUsed
    }
    storagePool.acquireMemory(blockId, numBytes)
  }


  override def acquireUnrollMemory
  (blockId: BlockId,
   numBytes: Long,
   memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}




object AutoTuneMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  /**
    * Return the total amount of memory shared between execution and storage, in bytes.
    */
  private def getMaxHeap(conf: SparkConf): Long = {
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.getDouble("spark.auto.fraction", 0.6)
    (usableMemory * memoryFraction).toLong
  }

  private def getInitialStorageMemory(conf: SparkConf): Long = {
    val initialStorageFraction = conf.getDouble("spark.auto.initialFraction", 0.5)
    (getMaxHeap(conf) * initialStorageFraction).toLong
  }

}