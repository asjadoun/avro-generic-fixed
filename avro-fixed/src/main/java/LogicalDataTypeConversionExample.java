import org.apache.avro.*;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;

public class LogicalDataTypeConversionExample {
    public static void main(String [] args ) throws IOException {
        Timestamp tsp = new Timestamp(1530228588182L);
        String strSchema = "{\"type\":\"record\",\"name\":\"tsp_name\",\"fields\":[{\"name\":\"tsp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}\n";
        Schema schema = new Schema.Parser().parse(strSchema);

        System.out.println("Current local time: " + new DateTime(tsp.getTime(), DateTimeZone.UTC));

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("tsp",tsp.getTime()); //Assume I cannot change this

        System.out.println("GenericRecord: " + deserialize(schema, toByteArray(schema , genericRecord)));
    }

    public static byte [] toByteArray(Schema schema, GenericRecord genericRecord) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.getData().addLogicalTypeConversion(new TimeConversions.DateConversion());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(genericRecord, encoder);
        encoder.flush();
        return baos.toByteArray();
    }


    public static GenericRecord deserialize(Schema schema, byte[] data) throws IOException {
        final GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new MyTimestampConversion());

        InputStream is = new ByteArrayInputStream(data);
        Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, genericData);
        return reader.read(null, decoder);
    }

    public static class MyTimestampConversion extends Conversion<String> {
        public MyTimestampConversion() {
        }

        public Class<String> getConvertedType() {
            return String.class;
        }

        public String getLogicalTypeName() {
            return "timestamp-millis";
        }

        public String fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
            return (new DateTime(millisFromEpoch, DateTimeZone.UTC)).toString();
        }

        public Long toLong(String timestamp, Schema schema, LogicalType type) {
            return Long.valueOf(timestamp);
        }
    }
}