package org.locationtech.geomesa.blob.core

import java.io.File
import com.google.common.io.{Files, ByteStreams}
import com.vividsolutions.jts.geom.Coordinate
import nitf.{SubWindow, ImageSegment, Reader, IOHandle}
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.util.Z3UuidGenerator
import org.locationtech.geomesa.blob.core.handlers._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import java.util

/**
  * Created by ejohns on 11/23/15.
  */
@RunWith(classOf[JUnitRunner])
class NTIF_FileHandlerTest extends Specification {


  "BlobStoreFileHandler" should {
    "be able able to retrieve a date from a NTIF file" in {
      val fileName = "U_1001A.NTF"
      val param = new java.util.HashMap[String, String]()


      val file = new File(getClass.getClassLoader.getResource(fileName).getFile)



      val nfh= new NTIF_FileHandler
      val sf= nfh.buildSF(file,param)

      //process
      sf.getAttribute(dateFieldName) must not be null
    }


  }

  "BlobStoreFileHandler" should {
    "be able able to retrieve a location from a NTIF file" in {
      val fileName = "U_1001A.NTF"
      val param = new java.util.HashMap[String, String]()


      val file = new File(getClass.getClassLoader.getResource(fileName).getFile)



      val nfh= new NTIF_FileHandler
      val sf= nfh.buildSF(file,param)

      //process
      sf.getAttribute(geomeFieldName) must not be null
    }



  }




  }
