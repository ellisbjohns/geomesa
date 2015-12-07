package org.locationtech.geomesa.blob.core

import java.io.File

import com.google.common.io.{ByteStreams, Files}
import com.vividsolutions.jts.geom.Coordinate
import nitf.{SubWindow, ImageSegment, Reader, IOHandle}
import org.geotools.data.DataStoreFinder
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.util.Z3UuidGenerator
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.blob.core.handlers._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class NTIF_BlobStoreTest extends Specification {
  val dsParams = Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "tableName"         -> "geomesa",
    "useMock"           -> "true",
    "featureEncoding"   -> "avro")
  val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

  val bstore = new AccumuloBlobStore(ds)

  sequential

  val testfile1 = "U_1001A.NTF"
 // val testfile2 = "testFile2.txt"
  var testFile1Id = ""

  "AccumuloBlobStore" should {
    "be able able store a sample NTIF file based on it's metadata and then retrieve this file" in {

      val (storeId, file) = ingestFile(testfile1, "ntif")

      testFile1Id = storeId.get

      val (returnedBytes, filename) = bstore.get(storeId.get)

      val inputStream = ByteStreams.toByteArray(Files.newInputStreamSupplier(file))

      filename mustEqual testfile1
      inputStream mustEqual returnedBytes
    }
  }


    def ingestFile(fileName: String, filetype: String): (Option[String], File) = {
      val file = new File(getClass.getClassLoader.getResource(fileName).getFile)
      val params = Map("filetype" -> filetype)
      val storeId = bstore.put(file, params)
      (storeId, file)
    }




}
