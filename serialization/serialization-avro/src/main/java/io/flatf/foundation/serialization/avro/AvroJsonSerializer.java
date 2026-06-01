package io.flatf.foundation.serialization.avro;

import io.flatf.foundation.common.character.Charsets;
import io.flatf.foundation.common.log4j2.Log4j2LoggerFactory;
import io.flatf.foundation.common.serialization.specific.JsonSerializer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

@NotThreadSafe
public class AvroJsonSerializer<T extends SpecificRecord> implements JsonSerializer<T> {

    private static final Logger log = Log4j2LoggerFactory.getLogger(AvroJsonSerializer.class);

    private JsonEncoder encoder;

    private final ByteArrayOutputStream outputStream;

    private final DatumWriter<T> writer;

    /**
     * Use default ByteArrayOutputStream size
     */
    public AvroJsonSerializer(Class<T> recordClass) {
        this(recordClass, 8192);
    }

    public AvroJsonSerializer(Class<T> recordClass, int size) {
        this.writer = new SpecificDatumWriter<>(recordClass);
        this.outputStream = new ByteArrayOutputStream(size);
    }

    @Override
    public String serialize(@Nonnull T obj) {
        try {
            // TODO 对象可重用
            encoder = EncoderFactory.get().jsonEncoder(obj.getSchema(), outputStream);

            writer.write(obj, encoder);
            encoder.flush();
            String str = outputStream.toString(Charsets.UTF8);
            outputStream.reset();
            return str;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            IOUtils.closeQuietly(outputStream);
            throw new RuntimeException("AvroTextSerializer serialization exception -> " + e.getMessage());
        }

    }

}
