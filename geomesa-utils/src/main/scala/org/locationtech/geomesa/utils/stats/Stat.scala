/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.opengis.feature.simple.SimpleFeature

import scala.util.parsing.combinator.RegexParsers

trait Stat[T] {
  def observe(sf: SimpleFeature)
  def add(other: T): T

  def toJson(): String
}

object Stat {

  class StatParser extends RegexParsers {

    val attributeName = """\w+""".r

    def minMaxParser: Parser[MinMax[_]] = {
      "MinMax(" ~> attributeName <~ ")" ^^ {
        case attribute: String => new MinMax[java.lang.Long](attribute)
      }
    }

    def iteratorStackParser: Parser[IteratorStackCounter] = {
      "IteratorCount" ^^ { case _ => new IteratorStackCounter() }
    }

    def enumeratedHistogramParser: Parser[EnumeratedHistogram[String]] = {
      "Histogram(" ~> attributeName <~ ")" ^^ {
        case attribute: String  => new EnumeratedHistogram[String](attribute)
      }
    }

    def statParser: Parser[Stat[_]] = {
      minMaxParser | iteratorStackParser | enumeratedHistogramParser
    }

    def statsParser: Parser[Stat[_]] = {
      rep1sep(statParser, ",") ^^ {
        case statParsers: Seq[Stat[_]] => new SeqStat(statParsers)
      }
    }

    def parse(s: String): Stat[_] = {
      parseAll(statsParser, s) match {
        case Success(result, _) => result
        case failure: NoSuccess =>
          throw new Exception(s"Could not parse $s.")
      }
    }
  }

  // Stat's apply method shoul take a SFT and do light validation.
  def apply(s: String) = new StatParser().parse(s)
}
