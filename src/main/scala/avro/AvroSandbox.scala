package avro

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, FlowShape }
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge, Source }
import org.apache.avro.Schema
import org.apache.avro.file.{ DataFileReader, DataFileWriter }
import org.apache.avro.generic.{ GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.{ DecoderFactory, EncoderFactory, JsonEncoder }
import org.apache.avro.util.{ ByteBufferInputStream, ByteBufferOutputStream }

object AvroSandbox extends App {

//  implicit val system = ActorSystem("avro-sandbox")
//  implicit val mat = ActorMaterializer()

  val file1 = new File("users1.txt")
  val file2 = new File("users2.txt")
  val file3 = new File("users3.txt")

  val avroSchemaVersion1: Schema = new Schema.Parser().parse {
    s"""
      |{"namespace": "example.avro",
      | "type": "record",
      | "name": "User",
      | "fields": [
      |     {"name": "name", "type": "string"},
      |     {"name": "favorite_number",  "type": ["int", "null"]},
      |     {"name": "favorite_color", "type": ["string", "null"]},
      |     {"name": "version", "type": "int"},
      |     {"name": "car", "type": "string", "default": "NONE"}
      | ]
      |}
    """.stripMargin
  }

  val avroSchemaVersion2 = new Schema.Parser().parse {
    s"""
       |{"namespace": "example.avro",
       | "type": "record",
       | "name": "User",
       | "fields": [
       |     {"name": "name", "type": "string"},
       |     {"name": "favorite_number",  "type": ["int", "null"]},
       |     {"name": "favorite_color", "type": ["string", "null"]},
       |     {"name": "version", "type": "int"}
       | ]
       |}
    """.stripMargin
  }

  val avroSchemaVersion3 = new Schema.Parser().parse {
    s"""
       |{"namespace": "example.avro",
       | "type": "record",
       | "name": "User",
       | "fields": [
       |     {"name": "name", "type": "string"},
       |     {"name": "favorite_number",  "type": ["int", "null"]},
       |     {"name": "favorite_color", "type": ["string", "null"]},
       |     {"name": "version", "type": "int"},
       |     {"name": "van", "type": "string", "default": "NONE"}
       | ]
       |}
    """.stripMargin
  }

  def produce(schema: Schema, file: File) = {
//    println("Producing with schema " + schema)
    val idx = 0

    val record = new GenericData.Record(schema)
    record.put("name", s"Name:$idx")
    record.put("favorite_number", idx)
    record.put("version", idx)

    schema match {
      case `avroSchemaVersion1` => println(s"Producing v1 file $file"); record.put("car", "British Leyland Mini")
      case `avroSchemaVersion2` => println(s"Producing v2 file $file"); ()
      case `avroSchemaVersion3` => println(s"Producing v3 file $file"); record.put("van", "DKV")
    }

    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema, file)
    dataFileWriter.append(record)
    dataFileWriter.close()
  }


  def consume(schema: Schema, file: File, version: String) = {
    println(s"Consuming $version from file $file")
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](file, datumReader)

    while (dataFileReader.hasNext) {
      val user = dataFileReader.next()
      System.out.println(user)
    }
    dataFileReader.close()
  }

  produce(avroSchemaVersion1, file1)
  produce(avroSchemaVersion2, file2)
  produce(avroSchemaVersion3, file3)

  consume(avroSchemaVersion1, file1, "v1")
  consume(avroSchemaVersion1, file2, "v1")
  consume(avroSchemaVersion1, file3, "v1")

  consume(avroSchemaVersion2, file1, "v2")
  consume(avroSchemaVersion2, file2, "v2")
  consume(avroSchemaVersion2, file3, "v2")

  consume(avroSchemaVersion3, file1, "v3")
  consume(avroSchemaVersion3, file2, "v3")
  consume(avroSchemaVersion3, file3, "v3")

}
