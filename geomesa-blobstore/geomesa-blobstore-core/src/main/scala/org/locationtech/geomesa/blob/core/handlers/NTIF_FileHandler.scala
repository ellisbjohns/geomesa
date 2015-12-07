package org.locationtech.geomesa.blob.core.handlers


import java.io.File
import java.util


import nitf._
import com.vividsolutions.jts.geom.{Coordinate}
import org.geotools.geometry.{GeometryFactoryFinder, GeometryBuilder}
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.measure._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.locationtech.geomesa.accumulo.util.Z3UuidGenerator
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}

import scala.collection.JavaConversions._

/**
  * Created by ejohns on 11/23/15.
  */
class NTIF_FileHandler extends AbstractFileHandler {


  override def buildSF(file: File, params: util.Map[String, String]): SimpleFeature = {


    val handle = new IOHandle(file.getAbsolutePath)
    val reader = new Reader
    val record = reader.read(handle)
    val segments: Array[ImageSegment] = record.getImages
    val imageRequest: SubWindow = new SubWindow
    var metadata = Map[String, String]()
    if (segments.length > 0) {
      for (segment <- segments) {
        // get a new ImageReader
        //val deserializer = reader.getNewImageReader(0)

        //metadata regarding the image
        val subheader = segment.getSubheader
        val dtg = subheader.getImageDateAndTime.toString

        val fmt = DateTimeFormat.forPattern("ddkkmmss'Z'MMMyy")
        val dt = fmt.parseDateTime(dtg)
        //val  dt = fmt.parseDateTime(dtg.substring(0,8)+ dtg.substring(9,14))


        //TODO change this to calculate the centroid
        val coords = getCoordinatesInDegreesFromString(subheader.getCornerCoordinates.toString)

        val coordinate1 = new Coordinate(coords.head._1.degrees(), coords.head._2.degrees())

        //make geometery from point.  This will enventually be a centroid of the four corners
        val point = WKTUtils.read(s"POINT(${coords.head._1.degrees()} ${coords.head._2.degrees()})")



        builder.set(geomeFieldName, point)

        builder.set(dateFieldName, dt)

        builder.set(idFieldName, Z3UuidGenerator.createUuid(point, dt.getMillis))

        builder.set(filenameFieldName, file.getName)

        //val sftSpec = s"$filenameFieldName:String,$idFieldName:String,$geomeFieldName:Geometry,$dateFieldName:Date,thumbnail:String"

        //val sft: SimpleFeatureType = SimpleFeatureTypes.createType(blobFeatureTypeName, sftSpec)

      }


    }
    builder.buildFeature("")

  }

  def getCoordinatesInDegreesFromString(DMS: String): List[(Latitude, Longitude)] = {
    val angleFormatLat = new AngleFormat("DDMMSS")
    val angleFormatLon = new AngleFormat("DDDMMSS")
    val lat1 = new Latitude(angleFormatLat.parse(DMS.substring(0, 6)).degrees() * (if (DMS.charAt(6) == 'N') 1 else -1)) //TODO make sure to add filter logic for non 'N' 'S' 'E' 'W' characters (see DMS.scala for regex))
    val lon1 = new Longitude(angleFormatLon.parse(DMS.substring(8, 14)).degrees() * (if (DMS.charAt(14) == 'E') 1 else -1))
    //acount for the fact the angles have N,S,E,W associated

    val lat2 = new Latitude(angleFormatLat.parse(DMS.substring(15, 21)).degrees() * (if (DMS.charAt(21) == 'N') 1 else -1))
    val lon2 = new Longitude(angleFormatLon.parse(DMS.substring(23, 29)).degrees() * (if (DMS.charAt(29) == 'E') 1 else -1))
    //acount for the fact the angles have N,S,E,W associated

    val returnCoords = List((lat1, lon1), (lat2, lon2))

    returnCoords
  }

  def canProcess(file: File, params: util.Map[String, String])={

    accept(file) || params.get("filetype").equals( "ntif")//TODO fix this to reflect true ntif file spectifics
  }

  def accept(file: File) = {
    file.getName.endsWith("ntif")

  }



}
