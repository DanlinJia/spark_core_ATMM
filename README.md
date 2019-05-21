# spark_core_ATMM
This is the source code of Spark (version 2.4.3) with a new memory manager implemented.

To merge this code with other versions:
  1. copy this file src/main/scala/org/apache/spark/memory/AutoTuneMemoryManager.scala to your Spark source code directory    $Your/Spark/Path/src/main/scala/org/apache/spark/memory/.
  2. Files modified are 
        src/main/scala/org/apache/spark/SparkEnv.scala,
        src/main/scala/org/apache/spark/SparkConf.scala, and
        src/main/scala/org/apache/spark/executor/Executor.scala.
  You can find the modified codes by searching "Modified" (commented). Or use command "diff" to compare these files with        original files (Download from https://spark.apache.org/downloads.html).
  Then modify your Spark code.
  3. rebuild Spark Core Module. 
  
