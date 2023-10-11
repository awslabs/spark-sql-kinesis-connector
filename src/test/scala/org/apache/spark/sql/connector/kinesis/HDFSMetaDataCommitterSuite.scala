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

package org.apache.spark.sql.connector.kinesis

import java.io.File

import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.connector.kinesis.metadata.HDFSMetadataCommitter
import org.apache.spark.util.SerializableConfiguration


class HDFSMetaDataCommitterSuite extends KinesisTestBase {

  val testConf: Configuration = new Configuration()
  val serializedConf = new SerializableConfiguration(testConf)

  test("Add and Get operation") {
    withTempDir { temp =>
      val dir = new File(temp, "commit")
      val metadataCommitter = new HDFSMetadataCommitter[String](dir.getAbsolutePath,
        serializedConf, DEFAULT_KINESIS_OPTIONS)
      assert(metadataCommitter.add(0, "Shard-000001", "foo"))
      assert(metadataCommitter.get(0) === Seq("foo"))

      assert(metadataCommitter.add(1, "Shard-000001", "one"))
      assert(metadataCommitter.add(1, "Shard-000002", "two"))
      assert(metadataCommitter.get(1).toSet === Set("one", "two"))

      // Adding the same batch over-writes the previous entry
      // This is required since re-attempt of a failed task will
      // update in same location
      assert(metadataCommitter.add(1, "Shard-000001", "updated-one"))
      assert(metadataCommitter.get(1).toSet === Set("updated-one", "two"))
    }
  }

  test("Purge operation") {
    withTempDir { temp =>
      val metadataCommitter = new HDFSMetadataCommitter[String](
        temp.getAbsolutePath,
        serializedConf, DEFAULT_KINESIS_OPTIONS)

      assert(metadataCommitter.add(0, "Shard-000001", "one"))
      assert(metadataCommitter.add(1, "Shard-000001", "two"))
      assert(metadataCommitter.add(2, "Shard-000001", "three"))

      assert(metadataCommitter.get(0).nonEmpty)
      assert(metadataCommitter.get(1).nonEmpty)
      assert(metadataCommitter.get(2).nonEmpty)

      metadataCommitter.purge(1)
      assertThrows[IllegalStateException](metadataCommitter.get(0))
      assertThrows[IllegalStateException](metadataCommitter.get(1))
      assert(metadataCommitter.get(2).nonEmpty)

      // There should be exactly one file, called "2", in the metadata directory.
      val allFiles = new File(metadataCommitter.metadataPath.toString).listFiles().toSeq
      assert(allFiles.size == 1)
      assert(allFiles.head.getName == "2")
    }
  }
}
