package com.pancakedb.spark

import scala.collection.mutable.ArrayBuffer

class Arguments(argMap: Map[String, Arguments.Argument]) {
  private val BooleanTrue = Set("t", "true", "")
  private val BooleanFalse = Set("f", "false")

  def int(key: String): Option[Int] = {
    get(key, value => value.toInt)
  }

  def double(key: String): Option[Double] = {
    get(key, value => value.toDouble)
  }

  def long(key: String): Option[Long] = {
    get(key, value => value.toLong)
  }

  def string(key: String): Option[String] = {
    get(key, value => value)
  }

  def boolean(key: String): Option[Boolean] = {
    get(key, value => {
      val lowercase = value.toLowerCase()
      if (BooleanTrue(lowercase)) {
        true
      } else if (BooleanFalse(lowercase)) {
        false
      } else {
        throw new Exception(s"Unable to interpret $value as boolean!")
      }
    })
  }

  def get[T](key: String, getter: String => T): Option[T] = {
    argMap.get(key).map(argument => getter(argument.value))
  }

  def fullMap(): Map[String, String] = {
    argMap.mapValues(_.value)
  }
}

object Arguments {
  private case class Argument(
    value: String
  )

  def parse(args: Array[String]): Arguments = {
    var currentKey = Option.empty[String]
    var currentValue = Option.empty[String]
    val parsed = ArrayBuffer.empty[(String, Argument)]

    def endKey(): Unit = {
      if (currentKey.isDefined) {
        val pair = (currentKey.get, Argument(currentValue.getOrElse("")))
        parsed += pair
        currentKey = None
        currentValue = None
      }
    }

    args.foreach(arg => {
      if (arg.startsWith("--")) {
        //it's a key
        if (arg.length <= 2) {
          throw new Exception("Cannot interpret '--' found while parsing raw args")
        }

        endKey()
        currentKey = Some(arg.slice(2, arg.length))
      } else {
        //it's a value
        if (currentKey.isEmpty) {
          throw new Exception(s"Cannot parse argument value $arg with no associated key")
        }

        if (currentValue.isDefined) {
          throw new Exception(s"Cannot parse consecutive argument values ${currentKey.get} $arg")
        }

        currentValue = Some(arg)
      }
    })

    endKey()

    new Arguments(parsed.toMap)
  }
}
