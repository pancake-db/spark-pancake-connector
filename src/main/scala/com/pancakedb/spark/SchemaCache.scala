package com.pancakedb.spark

import com.pancakedb.idl.{GetSchemaRequest, Schema}
import com.pancakedb.spark.Exceptions.TableDoesNotExist
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.slf4j.LoggerFactory

import java.util.concurrent.ExecutionException

case class SchemaCache(params: Parameters) {
  private val tableName = params.tableName
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
          val client = PancakeClientCache.getFromParams(params)
          Some(Some(client.grpc.getSchema(getSchemaReq).get().getSchema))
        } catch {
          case e: ExecutionException =>
            val cause = e.getCause.asInstanceOf[StatusRuntimeException]
            if (cause.getStatus.getCode == Code.NOT_FOUND) {
              Some(None)
            } else {
              throw e
            }
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
