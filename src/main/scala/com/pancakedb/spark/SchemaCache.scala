package com.pancakedb.spark

import com.pancakedb.client.Exceptions.HttpException
import com.pancakedb.client.PancakeClient
import com.pancakedb.idl.{GetSchemaRequest, Schema}
import com.pancakedb.spark.Exceptions.TableDoesNotExist
import org.slf4j.LoggerFactory

case class SchemaCache(tableName: String, client: PancakeClient) {
  private val logger = LoggerFactory.getLogger(getClass)
  private var schema: Option[Option[Schema]] = None

  def getOption: Option[Schema] = {
    this.synchronized {
      if (schema.isEmpty) {
        logger.info(s"Querying schema for table $tableName")
        val getSchemaReq = GetSchemaRequest.newBuilder()
          .setTableName(tableName)
          .build()

        schema = try {
          Some(Some(client.grpc.getSchema(getSchemaReq).get().getSchema))
        } catch {
          case HttpException(404, _) => Some(None)
        }
      }
    }

    schema.get
  }

  def get: Schema = {
    val maybeResult = getOption
    if (maybeResult.isEmpty) {
      throw TableDoesNotExist(tableName)
    }
    maybeResult.get
  }

  def set(newSchema: Schema): Unit = {
    this.synchronized {
      this.schema = Some(Some(newSchema))
    }
  }
}
