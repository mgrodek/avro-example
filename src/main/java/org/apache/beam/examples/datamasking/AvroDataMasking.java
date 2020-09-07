package org.apache.beam.examples.datamasking;

import lombok.Value;
import lombok.extern.java.Log;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;
import static org.apache.commons.lang3.StringUtils.stripEnd;

@Log
public class AvroDataMasking {

    private static final String SCHEMA_PATH = "src/test/resources/avsc/";
    private static final String AVRO_PATH = "src/test/resources/avro/*%s*.avro";
    private static final String OUTPUT_PATH = "src/test/resources/avro/output/";

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        Stream.of(requireNonNull(new File(SCHEMA_PATH).listFiles()))
                .map(schema -> new SchemaContainer(schema.getName(), getSchema(schema)))
                .filter(SchemaContainer::nonNullSchema)
                .forEach(container -> p.apply("Read avro files for container " + container.getFileName(),
                        AvroIO.readGenericRecords(container.getSchema()).from(format(AVRO_PATH, container.getFileName())))
                        .apply("Data masking", ParDo.of(new DataMaskingFn()))
                        .apply("Write masked data to avro files", AvroIO.writeGenericRecords(container.getSchema())
                                .to(OUTPUT_PATH + container.getFileName())
                                .withSuffix(".avro")));

        p.run();
    }

    @Value
    private static class SchemaContainer {
        String fileName;
        Schema schema;

        boolean nonNullSchema() {
            return nonNull(schema);
        }

        String getFileName() {
            return stripEnd(fileName, ".avsc");
        }
    }

    private static Schema getSchema(File schema) {
        try {
            log.info("Found schema: " + schema.getName());
            return new Parser().parse(schema);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static class DataMaskingFn extends DoFn<GenericRecord, GenericRecord> {
        private static final String TO_MASK = "email";

        @ProcessElement
        public void processElement(@Element GenericRecord record, OutputReceiver<GenericRecord> out) {
            GenericRecord recordToMask = new Record((Record) record, true);

            if (nonNull(recordToMask.get(TO_MASK))) {
                String hash = sha256Hex(recordToMask.get(TO_MASK).toString());
                log.info("data masking email in " + record.getSchema().getName() + ": " + hash);
                recordToMask.put(TO_MASK, hash);
            }

            out.output(recordToMask);
        }
    }
}