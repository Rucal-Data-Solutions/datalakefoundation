package unit_tests

import org.scalatest.funsuite.AnyFunSuite

import datalake.metadata._
import datalake.processing._

class BenchmarkSpec extends AnyFunSuite with SparkSessionTest {
  test("Benchmark Full, Merge and Historic processing") {
    import spark.implicits._
    import org.apache.commons.io.FileUtils

    // Warm up Spark engine
    info("Warming up Spark engine...")
    val warmupData = (1 to 1000).map(i => (i, i.toLong)).toDF("ID", "SeqNr")
    warmupData.cache().count() // Force execution and cache the data
    warmupData.unpersist() // Clean up the cache
    info("Warm-up complete")

    val bronzeFolder = new java.io.File(s"$testBasePath/bronze")
    if (!bronzeFolder.exists()) bronzeFolder.mkdirs()
    val silverFolder = new java.io.File(s"$testBasePath/silver")
    if (!silverFolder.exists()) silverFolder.mkdirs()

    val env = new Environment(
      "DEBUG (OVERRIDE)",
      testBasePath.replace("\\", "/"),
      "Europe/Amsterdam",
      "/${connection}/${entity}",
      "/${connection}/${entity}",
      "/${connection}/${destination}",
      "-secure"
    )

    val settings = new JsonMetadataSettings()
    val user_dir = System.getProperty("user.dir")
    settings.initialize(s"${user_dir}/src/test/scala/example/metadata.json")

    val metadata = new Metadata(settings, env)
    val mergeEntity = metadata.getEntity(2)
    val historicEntity = metadata.getEntity(1)

    FileUtils.deleteDirectory(new java.io.File(mergeEntity.getPaths.silverpath))
    FileUtils.deleteDirectory(new java.io.File(historicEntity.getPaths.silverpath))

    val fullSlice = "full_slice.parquet"
    val fullData = (1 to 10000).map(i => (i, i.toLong)).toDF("ID", "SeqNr")
    fullData.write.mode("overwrite").parquet(s"${mergeEntity.getPaths.bronzepath}/$fullSlice")

    val fullProc = new Processing(mergeEntity, fullSlice)
    val fullStart = System.nanoTime()
    fullProc.Process(Full)
    val fullDuration = (System.nanoTime() - fullStart) / 1e6d
    info(f"Full processing took $fullDuration%.2f ms")
    assert(fullDuration >= 0)

    val mergeSlice = "merge_slice.parquet"
    val mergeData = (1 to 10000).map(i => (i, i.toLong + 1)).toDF("ID", "SeqNr")
    mergeData.write.mode("overwrite").parquet(s"${mergeEntity.getPaths.bronzepath}/$mergeSlice")

    val mergeProc = new Processing(mergeEntity, mergeSlice)
    val mergeStart = System.nanoTime()
    mergeProc.Process(Merge)
    val mergeDuration = (System.nanoTime() - mergeStart) / 1e6d
    info(f"Merge processing took $mergeDuration%.2f ms")
    assert(mergeDuration >= 0)

    val historicSlice = "historic_slice.parquet"
    val histData = (1 to 10000).map(i => i).toDF("id")
    histData.write.mode("overwrite").parquet(s"${historicEntity.getPaths.bronzepath}/$historicSlice")

    val historicProc = new Processing(historicEntity, historicSlice)
    val historicStart = System.nanoTime()
    historicProc.Process(Historic)
    val historicDuration = (System.nanoTime() - historicStart) / 1e6d
    info(f"Historic processing took $historicDuration%.2f ms")
    assert(historicDuration >= 0)
  }
}

