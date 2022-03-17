import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{EncoderFactory, JsonEncoder}

import java.io._
import java.nio.charset.StandardCharsets

object MinApp extends App {

  def avroToJson(record: GenericRecord, pretty: Boolean) = {
    val writer = new GenericDatumWriter[GenericRecord](record.getSchema)
    val out = new ByteArrayOutputStream
    val jsonEncoder: JsonEncoder = EncoderFactory.get.jsonEncoder(record.getSchema, out, pretty)
    writer.write(record, jsonEncoder)
    jsonEncoder.flush()

    new String(out.toByteArray, StandardCharsets.UTF_8)
  }

  def prepareRecord(id: String, code: String) = {
    val record = new GenericData.Record(schema)

    record.put("id", id)

    val fixedField = schema.getFields.get(1)
    val fixedSchema = fixedField.schema()
    //  println(fixedSchema)
    val fixedBytes = code.getBytes(StandardCharsets.UTF_8)
    val fixedRecord = new GenericData.Fixed(fixedSchema)
    fixedRecord.bytes(fixedBytes)
    record.put("code", fixedRecord)

    val arrayField = schema.getFields.get(2)
    val arraySchema = arrayField.schema()
    val list = List(s"Anil($id)", "Singh", s"Arihant($id)")
    import scala.jdk.CollectionConverters._
    val arrayRecord = new GenericData.Array[String](arraySchema, list.asJavaCollection)
    record.put("names", arrayRecord)

    val enumField = schema.getFields.get(3)
    val enumSchema = enumField.schema()
    val enumData = "Male"
    val enumRecord = new GenericData.EnumSymbol(enumSchema, enumData)
    record.put("gender", enumRecord)

    val mapData = Map("first" -> s"Anil($id)", "last" -> "Singh", "son" -> s"Arihant($id)").asJava
    record.put("meta", mapData)

    record
  }

  println("Good Morning....")
  val out = new File("data-file1.avro")
  val file = getClass.getClassLoader.getResourceAsStream("schema-with-fixed.avsc")
//  val file = getClass.getClassLoader.getResourceAsStream("schema-simple.avsc")

  val parser = new Schema.Parser()
  val schema = parser.parse(file)
//  println(schema)

  val records = (0 to 3).map(x => prepareRecord(id = s"100$x", code = s"SMB0$x"))
  val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  val writer = new DataFileWriter[GenericRecord](datumWriter)

  writer.create(schema, out)
  records.foreach(record => writer.append(record))
  writer.flush()

  val datumReader = new GenericDatumReader[GenericRecord](schema)
  val reader = new DataFileReader[GenericRecord](out, datumReader)

  reader.forEach(r => println(s"${avroToJson(r, true)}"))

}
