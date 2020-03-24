package com.spotify.scio.smb

import java.nio.file.Files

import com.spotify.scio.smb._
import com.spotify.scio.ScioContext
import com.spotify.scio.avro.{Account, TestRecord}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SortMergeBucketIOTest extends AnyFlatSpec with Matchers {

  "Sort Merge Bucket IO" should "use SMB read when metadata file is present" in {
    // Write SMB data
    val tempFolder = Files.createTempDirectory("smb")
    tempFolder.toFile.deleteOnExit()

    val sc1 = ScioContext()
    sc1.parallelize(1 to 10)
      .map(i => Account.newBuilder().setId(i).setAmount(1.0).setName("foo").setType("type").build)
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(classOf[Integer], "id", classOf[Account])
          .to(tempFolder.toString)
      )

    sc1.run().waitUntilDone()

    val sc2 = ScioContext()
    val counter = sc2.initCounter("SmbResults")
    sc2
      .smbAvroFile[Account](tempFolder.toString)
      .groupByKey
      .on(classOf[Integer])
      .getOrFallback match {
        case Left(smbResult) => counter.inc()
        case Right(_) => throw new NotImplementedError("Fallback should not be evaluated")
      }

    sc2.run().waitUntilDone().counter(counter).committed.get shouldEqual(10)
  }
}
