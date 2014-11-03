package org.apache.spark.sql.squandl

import com.jimmoores.quandl.{QuandlSession, DataSetRequest, MetaDataRequest}
import com.jimmoores.quandl.{Row => QuandlRow}

class QuandlDataset(quandlCode : String) {
  import java.sql.{Date, Timestamp}
  import scala.collection.mutable.Buffer
  import scala.collection.Iterator
  import scala.util.control.Exception.allCatch
  import scala.util.matching.Regex
  import org.apache.spark.sql.{
    SQLContext, SchemaRDD, Row, StructField, StructType,
    DateType, TimestampType, DoubleType, StringType}
  import scala.collection.JavaConversions._

  private val session = QuandlSession.create
  private val request = DataSetRequest.Builder.of(quandlCode).build()
  private val metarequest = MetaDataRequest.of(quandlCode)
  private val dataset = session.getDataSet(request)
  private val rowIterator: Iterator[QuandlRow] = dataset.iterator
  private val header = dataset.getHeaderDefinition
  private val json = session.getMetaData(metarequest).getRawJSON
  private val dateRe = """^\d{4}-\d{2}-\d{2}$""".r
  private val timeRe = """^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{0,9}){0,1}$""".r
  private def getStructField( columnName: String,
                      field: String): StructField = {
    dateRe.findFirstIn(field) match {
      case Some(_) => {return StructField(columnName, DateType, false)}
      case None => null
    }
    timeRe.findFirstIn(field) match {
      case Some(_) => {return StructField(columnName, TimestampType, false)}
      case None => null
    }
    (allCatch opt field.toDouble) match {
      case Some(_) => {return StructField(columnName, DoubleType, true)} 
      case None    => {return StructField(columnName, StringType, true)}
     }
  }
  private def quandlRowToRow(r: QuandlRow): Row = {
    val strings = for {
      n <- List.range(0, r.size)
    } yield r.getString(n)
    Row.fromStringsBySchema(strings, schema)
  }

  val rows = rowIterator.toSeq
  val firstRow = rows(0)
  val columnNames = (header.getColumnNames : Buffer[String]).toIndexedSeq 
  val metadata = json.keys.toList.map(
    {key:Any => ( key.toString, json.get(key.toString) )} )
      .toMap

  def structFields = for {
    n <- List.range(0, firstRow.size)
    } yield getStructField(columnNames(n), firstRow.getString(n))

  val schema = StructType(structFields)

  def rdd(context: SQLContext): SchemaRDD = {
    val sparkRows = rows.map( quandlRowToRow )
    val rdd = context.sparkContext.parallelize(sparkRows)
    context.applySchema(rdd, schema)
  }
}

