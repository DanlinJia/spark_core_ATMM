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
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

  /**
    * * A [[MemoryManager]] that dynamically partitions the heap space into disjoint regions.
    *
    * The sizes of the execution and storage regions are determined through
    * `spark.qlearn.memoryFraction` and `spark.qlearn.memoryFraction` respectively. The two
    * regions are cleanly separated such that neither usage can borrow memory from the other.
    *
    * After each task done, the memory partition adjusts to minimize the GC time.
    */
private[spark] class QLearningMemoryManager(
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
        QLearningMemoryManager.getMaxHeap(conf),
        QLearningMemoryManager.getInitialStorageMemory(conf),
        numCores)
    }

    private def assertInvariants(): Unit = {
      assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
      assert(
        offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
    }

    assertInvariants()

    val randomGenerator = new scala.util.Random
    var heapStorageMemory: Long = onHeapStorageMemoryPool.poolSize

    val stateSpace : ListBuffer[(Double, Double)] =  {
        val stateSpace = new ListBuffer[(Double, Double)]()
        var i = 0
        var j = 0
        for(i <- 1 to 9) {
            for(j <- 1 to 9) {
                if(i>=j) {
                    stateSpace += ((i/10.0, j/10.0))
                }
            }
        }
        stateSpace
    }

    val actionSpace : ListBuffer[Int] = {
        val actionSpace = new ListBuffer[Int]()
        var i = 0
        for(i <- 0 to stateSpace.length-1 ) {
            actionSpace += i
        }
        actionSpace
    }

    var QTable = Array.ofDim[Double](stateSpace.length, actionSpace.length)
    
    var state = (0.5, 0.5)
    var action = 0
    var actionIndex = 0
    var stateIndex = stateSpace.indexOf(state)
    var reward = 0.0
    val discountFactor = conf.getDouble("spark.storage.discountFactor", 0.9)
    val alpha = conf.getDouble("spark.storage.alpha", 0.8)
    val epsilon = conf.getDouble("spark.storage.epsilon", 0.2)

    def QLearnigAgent(taskTime: Double, GCTime: Double): Unit = synchronized {        
        // paras:
        //     sate_space: possible values of heapstorage and storagepoolsize
        //     action_space: possible actions to ajust values of heapstorage and storagepoolsize
        //     discount_factor[0, 1]: discide the impact of previous rewards on the latest q-value
        //     alpha[0，1](learning rate): decide how much we accept the new rewards
        //     epsilon[0，1](exploring rate): decide how offen to explore untouched states
        //     num_episode: the number of applications we need to converge
        //     steps(task id): a stage in an application
        // output:
        //     Q(state,action): Q-table records the expeccation of taking action a at state s
        // training procedure:
        //     1. initialize Q-table 
        //     2. Start iterative training
        //     3. Make decisions to exploit or explore
        //     4. take an action
        //     5. calculate rewards
        //     6. update q-value
        //     7. jump backs to step 3
        //     8. return stable Q-table
        
        reward = taskTime/(GCTime+0.01)
        QTable(stateIndex)(actionIndex) += 
            alpha * (reward + discountFactor*(QTable(stateIndex).max - QTable(stateIndex)(actionIndex)))  
        
        val rnd = randomGenerator.nextFloat()
        if(rnd<epsilon) {
            actionIndex = randomGenerator.nextInt(actionSpace.length)
        } else {
            actionIndex = QTable(stateIndex).indexOf(QTable(stateIndex).max)
        }
        action = actionSpace(actionIndex)
        state = stateSpace(action)
        mapState2Memory(state)
    }

    private def mapState2Memory(state: (Double, Double)): Unit = synchronized {
        val newStoragePoolSize: Long = state._1.toLong * maxHeapMemory
        heapStorageMemory = state._2.toLong * maxHeapMemory
        logDebug(s"new state is ${state}, newstoragepool is ${newStoragePoolSize}, new heapstorage is ${heapStorageMemory}")
        assert(newStoragePoolSize>=heapStorageMemory)
        var memoryBorrowed: Long = newStoragePoolSize - onHeapStorageMemoryPool.poolSize
        if (memoryBorrowed>0) {
            assert(memoryBorrowed <= onHeapExecutionMemoryPool.memoryFree)
            onHeapStorageMemoryPool.incrementPoolSize(memoryBorrowed)
            onHeapExecutionMemoryPool.decrementPoolSize(memoryBorrowed)
        } else if (memoryBorrowed<0) {
            memoryBorrowed = 0 - memoryBorrowed
            memoryBorrowed = Math.min(memoryBorrowed, onHeapStorageMemoryPool.memoryFree)
            onHeapStorageMemoryPool.decrementPoolSize(memoryBorrowed)
            onHeapExecutionMemoryPool.incrementPoolSize(memoryBorrowed)
        }
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
    val exec_used = onHeapExecutionMemoryPool.memoryUsed
    val storage_used = onHeapStorageMemoryPool.memoryUsed
    val exec_pool = onHeapExecutionMemoryPool.poolSize
    val storage_pool = onHeapStorageMemoryPool.poolSize
    val heapstorage = heapStorageMemory
    logInfo("exec_used, storage_used, exec_pool, storage_pool, heapstorage")
    logInfo(s"runtime_trace:$exec_used, $storage_used, $exec_pool, $storage_pool, $heapstorage")
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




object QLearningMemoryManager {

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
    val memoryFraction = conf.getDouble("spark.qlearn.fraction", 0.6)
    (usableMemory * memoryFraction).toLong
  }

  private def getInitialStorageMemory(conf: SparkConf): Long = {
    val initialStorageFraction = conf.getDouble("spark.qlearn.initialFraction", 0.5)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (getMaxHeap(conf) * initialStorageFraction).toLong
  }

}