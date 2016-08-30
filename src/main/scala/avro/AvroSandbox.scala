package avro

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, FlowShape }
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Merge, Source }
import org.apache.avro.Schema
import org.apache.avro.file.{ DataFileReader, DataFileWriter }
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{ GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord }
import org.apache.avro.io.{ DecoderFactory, EncoderFactory, JsonEncoder }
import org.apache.avro.util.{ ByteBufferInputStream, ByteBufferOutputStream }

object AvroSandbox extends App {

  implicit val system = ActorSystem("avro-sandbox")
  implicit val mat = ActorMaterializer()

  val file = new File("users.txt")

  val avroSchemaVersion1 = new Schema.Parser().parse {
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

  def produce() = {
    val idx = 0

    val record = new GenericData.Record(avroSchemaVersion2)
    record.put("name", s"Name:$idx")
    record.put("favorite_number", idx)
    record.put("version", 1)
//    record.put("car", "British Leyland Mini")

    val datumWriter = new GenericDatumWriter[GenericRecord](avroSchemaVersion2)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(avroSchemaVersion2, file)
    dataFileWriter.append(record)
    dataFileWriter.close()
  }


  def consume() = {
    val datumReader = new GenericDatumReader[GenericRecord](avroSchemaVersion1)
    val dataFileReader = new DataFileReader[GenericRecord](file, datumReader)

    while (dataFileReader.hasNext) {
      val user = dataFileReader.next()
      System.out.println(user)
    }
    dataFileReader.close()
  }


  produce()
  consume()


//  val serialise = Flow.fromGraph(GraphDSL.create() { implicit builder =>
//    import GraphDSL.Implicits._
//
//    val toBothSerializers = builder.add(Broadcast[Int](2))
//
//    def version(version: Int) = builder.add(Flow[Int].map { idx =>
//      s"version$version message index: $idx"
//
//      val outputStream = new ByteBufferOutputStream
//      val encoder = EncoderFactory.get().jsonEncoder(avroSchemaVersion, outputStream)
//      datumWriter.write(record, encoder)
//      encoder
//      new ByteBufferInputStream(outputStream.getBufferList)
//    } map { stream =>
//      val inputStream =
//
//      val datumReader = new GenericDatumReader[GenericRecord](avroSchemaVersion(1))
//
//      var record: GenericRecord = null
//
//      datumReader.read(record, DecoderFactory.get().jsonDecoder(avroSchemaVersion(1), inputStream))
//      println(record)
//      record
//    })
//
//    val toSingleOut = builder.add(Merge[GenericRecord](2))
//
//    toBothSerializers ~> version(1) ~> toSingleOut
//    toBothSerializers ~> version(2) ~> toSingleOut
//
//    FlowShape(toBothSerializers.in, toSingleOut.out)
//  })
//
//  Source(1 to 10)
//    .via { serialise }
//    .runForeach(println)

}
