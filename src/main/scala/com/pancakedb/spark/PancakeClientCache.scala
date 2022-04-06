package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import org.slf4j.LoggerFactory

import scala.collection.mutable

object PancakeClientCache {
  private val logger = LoggerFactory.getLogger(getClass)

  val map: mutable.HashMap[PancakeCoords, PancakeClient] = mutable.HashMap.empty[PancakeCoords, PancakeClient]

  def getFromParams(params: Parameters): PancakeClient = {
    get(PancakeCoords(params.host, params.port))
  }

  def get(coords: PancakeCoords): PancakeClient = {
    map.synchronized {
      if (!map.contains(coords)) {
        logger.info(s"Initializing Pancake client with host ${coords.host} and port ${coords.port}")
        map(coords) = PancakeClient(coords.host, coords.port)
      }
    }

    map(coords)
  }

  case class PancakeCoords(host: String, port: Int)
}
