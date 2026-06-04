package io.flatf.infra.serialization.avro;

import io.flatf.common.lang.Validator;
import io.flatf.common.log4j2.Log4j2LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

@NotThreadSafe
public final class AvroFileWriter<T extends SpecificRecord> implements Closeable {

    private static final Logger log = Log4j2LoggerFactory.getLogger(AvroFileWriter.class);

    // SpecificRecord schema
    private final Schema schema;

    // DataFileWriter
    private final DataFileWriter<T> fileWriter;

    /**
     * @param record T
     */
    public AvroFileWriter(T record) {
        this.schema = record.getSchema();
        // DatumWriter instance use SpecificDatumWriter
        DatumWriter<T> datumWriter = new SpecificDatumWriter<>(schema);
        this.fileWriter = new DataFileWriter<>(datumWriter);
    }

    /**
     * @param saveFile File
     * @param records  Collection<T>
     * @throws IOException ioe
     */
    public void append(final File saveFile, Collection<T> records) throws IOException {
        append(null, saveFile, records);
    }

    /**
     * @param codec    CodecFactory
     * @param saveFile File
     * @param records  Collection<T>
     * @throws IOException ioe
     */
    public void append(@Nullable CodecFactory codec, final File saveFile, final Collection<T> records)
            throws IOException {
        Validator.nonNull(saveFile, "saveFile");
        File dir = saveFile.getParentFile();
        if (!dir.exists()) {
            boolean mkdirs = dir.mkdirs();
            log.debug("{} mkdirs -> {}", dir, mkdirs);
        }
        // 如果文件存在则追加, 否则创建新文件
        if (saveFile.exists()) {
            fileWriter.appendTo(saveFile);
        } else {
            fileWriter.create(schema, saveFile);
        }
        // 设置Codec
        if (codec != null) {
            fileWriter.setCodec(codec);
        }
        // Serializing
        if (CollectionUtils.isNotEmpty(records)) {
            for (T record : records) {
                // write record
                fileWriter.append(record);
            }
            fileWriter.fSync();
        }
    }

    @Override
    public void close() throws IOException {
        fileWriter.close();
    }

}
