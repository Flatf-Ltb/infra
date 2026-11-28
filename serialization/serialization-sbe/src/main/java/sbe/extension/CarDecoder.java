/* Generated SBE (Simple Binary Encoding) message codec. */
package sbe.extension;

import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;


/**
 * Description of a basic Car
 */
@SuppressWarnings("all")
public final class CarDecoder
{
    public static final int BLOCK_LENGTH = 62;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 1;
    public static final String SEMANTIC_VERSION = "5.2";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final CarDecoder parentMessage = this;
    private DirectBuffer buffer;
    private int offset;
    private int limit;
    int actingBlockLength;
    int actingVersion;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public CarDecoder wrap(
        final DirectBuffer buffer,
        final int offset,
        final int actingBlockLength,
        final int actingVersion)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public CarDecoder wrapAndApplyHeader(
        final DirectBuffer buffer,
        final int offset,
        final MessageHeaderDecoder headerDecoder)
    {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        if (TEMPLATE_ID != templateId)
        {
            throw new IllegalStateException("Invalid TEMPLATE_ID: " + templateId);
        }

        return wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version());
    }

    public CarDecoder sbeRewind()
    {
        return wrap(buffer, offset, actingBlockLength, actingVersion);
    }

    public int sbeDecodedLength()
    {
        final int currentLimit = limit();
        sbeSkip();
        final int decodedLength = encodedLength();
        limit(currentLimit);

        return decodedLength;
    }

    public int actingVersion()
    {
        return actingVersion;
    }

    public int encodedLength()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        this.limit = limit;
    }

    public static int serialNumberId()
    {
        return 1;
    }

    public static int serialNumberSinceVersion()
    {
        return 0;
    }

    public static int serialNumberEncodingOffset()
    {
        return 0;
    }

    public static int serialNumberEncodingLength()
    {
        return 8;
    }

    public static String serialNumberMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long serialNumberNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long serialNumberMinValue()
    {
        return 0x0L;
    }

    public static long serialNumberMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long serialNumber()
    {
        return buffer.getLong(offset + 0, BYTE_ORDER);
    }


    public static int modelYearId()
    {
        return 2;
    }

    public static int modelYearSinceVersion()
    {
        return 0;
    }

    public static int modelYearEncodingOffset()
    {
        return 8;
    }

    public static int modelYearEncodingLength()
    {
        return 2;
    }

    public static String modelYearMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int modelYearNullValue()
    {
        return 65535;
    }

    public static int modelYearMinValue()
    {
        return 0;
    }

    public static int modelYearMaxValue()
    {
        return 65534;
    }

    public int modelYear()
    {
        return (buffer.getShort(offset + 8, BYTE_ORDER) & 0xFFFF);
    }


    public static int availableId()
    {
        return 3;
    }

    public static int availableSinceVersion()
    {
        return 0;
    }

    public static int availableEncodingOffset()
    {
        return 10;
    }

    public static int availableEncodingLength()
    {
        return 1;
    }

    public static String availableMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public short availableRaw()
    {
        return ((short)(buffer.getByte(offset + 10) & 0xFF));
    }

    public BooleanType available()
    {
        return BooleanType.get(((short)(buffer.getByte(offset + 10) & 0xFF)));
    }


    public static int codeId()
    {
        return 4;
    }

    public static int codeSinceVersion()
    {
        return 0;
    }

    public static int codeEncodingOffset()
    {
        return 11;
    }

    public static int codeEncodingLength()
    {
        return 1;
    }

    public static String codeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public byte codeRaw()
    {
        return buffer.getByte(offset + 11);
    }

    public Model code()
    {
        return Model.get(buffer.getByte(offset + 11));
    }


    public static int someNumbersId()
    {
        return 5;
    }

    public static int someNumbersSinceVersion()
    {
        return 0;
    }

    public static int someNumbersEncodingOffset()
    {
        return 12;
    }

    public static int someNumbersEncodingLength()
    {
        return 16;
    }

    public static String someNumbersMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long someNumbersNullValue()
    {
        return 4294967295L;
    }

    public static long someNumbersMinValue()
    {
        return 0L;
    }

    public static long someNumbersMaxValue()
    {
        return 4294967294L;
    }

    public static int someNumbersLength()
    {
        return 4;
    }


    public long someNumbers(final int index)
    {
        if (index < 0 || index >= 4)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        final int pos = offset + 12 + (index * 4);

        return (buffer.getInt(pos, BYTE_ORDER) & 0xFFFF_FFFFL);
    }


    public static int vehicleCodeId()
    {
        return 6;
    }

    public static int vehicleCodeSinceVersion()
    {
        return 0;
    }

    public static int vehicleCodeEncodingOffset()
    {
        return 28;
    }

    public static int vehicleCodeEncodingLength()
    {
        return 6;
    }

    public static String vehicleCodeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static byte vehicleCodeNullValue()
    {
        return (byte)0;
    }

    public static byte vehicleCodeMinValue()
    {
        return (byte)32;
    }

    public static byte vehicleCodeMaxValue()
    {
        return (byte)126;
    }

    public static int vehicleCodeLength()
    {
        return 6;
    }


    public byte vehicleCode(final int index)
    {
        if (index < 0 || index >= 6)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        final int pos = offset + 28 + (index * 1);

        return buffer.getByte(pos);
    }


    public static String vehicleCodeCharacterEncoding()
    {
        return java.nio.charset.StandardCharsets.US_ASCII.name();
    }

    public int getVehicleCode(final byte[] dst, final int dstOffset)
    {
        final int length = 6;
        if (dstOffset < 0 || dstOffset > (dst.length - length))
        {
            throw new IndexOutOfBoundsException("Copy will go out of range: offset=" + dstOffset);
        }

        buffer.getBytes(offset + 28, dst, dstOffset, length);

        return length;
    }

    public String vehicleCode()
    {
        final byte[] dst = new byte[6];
        buffer.getBytes(offset + 28, dst, 0, 6);

        int end = 0;
        for (; end < 6 && dst[end] != 0; ++end);

        return new String(dst, 0, end, java.nio.charset.StandardCharsets.US_ASCII);
    }


    public int getVehicleCode(final Appendable value)
    {
        for (int i = 0; i < 6; ++i)
        {
            final int c = buffer.getByte(offset + 28 + i) & 0xFF;
            if (c == 0)
            {
                return i;
            }

            try
            {
                value.append(c > 127 ? '?' : (char)c);
            }
            catch (final java.io.IOException ex)
            {
                throw new java.io.UncheckedIOException(ex);
            }
        }

        return 6;
    }


    public static int extrasId()
    {
        return 7;
    }

    public static int extrasSinceVersion()
    {
        return 0;
    }

    public static int extrasEncodingOffset()
    {
        return 34;
    }

    public static int extrasEncodingLength()
    {
        return 1;
    }

    public static String extrasMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    private final OptionalExtrasDecoder extras = new OptionalExtrasDecoder();

    public OptionalExtrasDecoder extras()
    {
        extras.wrap(buffer, offset + 34, parentMessage.actingVersion);
        return extras;
    }

    public static int discountedModelId()
    {
        return 8;
    }

    public static int discountedModelSinceVersion()
    {
        return 0;
    }

    public static int discountedModelEncodingOffset()
    {
        return 35;
    }

    public static int discountedModelEncodingLength()
    {
        return 1;
    }

    public static String discountedModelMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "constant";
        }

        return "";
    }

    public byte discountedModelRaw()
    {
        return Model.C.value();
    }


    public Model discountedModel()
    {
        return Model.C;
    }


    public static int engineId()
    {
        return 9;
    }

    public static int engineSinceVersion()
    {
        return 0;
    }

    public static int engineEncodingOffset()
    {
        return 35;
    }

    public static int engineEncodingLength()
    {
        return 10;
    }

    public static String engineMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    private final EngineDecoder engine = new EngineDecoder();

    public EngineDecoder engine()
    {
        engine.wrap(buffer, offset + 35);
        return engine;
    }

    public static int uuidId()
    {
        return 100;
    }

    public static int uuidSinceVersion()
    {
        return 1;
    }

    public static int uuidEncodingOffset()
    {
        return 45;
    }

    public static int uuidEncodingLength()
    {
        return 16;
    }

    public static String uuidMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "optional";
        }

        return "";
    }

    public static long uuidNullValue()
    {
        return -9223372036854775808L;
    }

    public static long uuidMinValue()
    {
        return -9223372036854775807L;
    }

    public static long uuidMaxValue()
    {
        return 9223372036854775807L;
    }

    public static int uuidLength()
    {
        return 2;
    }


    public long uuid(final int index)
    {
        if (index < 0 || index >= 2)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        if (parentMessage.actingVersion < 1)
        {
            return -9223372036854775808L;
        }

        final int pos = offset + 45 + (index * 8);

        return buffer.getLong(pos, BYTE_ORDER);
    }


    public static int cupHolderCountId()
    {
        return 101;
    }

    public static int cupHolderCountSinceVersion()
    {
        return 1;
    }

    public static int cupHolderCountEncodingOffset()
    {
        return 61;
    }

    public static int cupHolderCountEncodingLength()
    {
        return 1;
    }

    public static String cupHolderCountMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "optional";
        }

        return "";
    }

    public static short cupHolderCountNullValue()
    {
        return (short)255;
    }

    public static short cupHolderCountMinValue()
    {
        return (short)0;
    }

    public static short cupHolderCountMaxValue()
    {
        return (short)254;
    }

    public short cupHolderCount()
    {
        if (parentMessage.actingVersion < 1)
        {
            return (short)255;
        }

        return ((short)(buffer.getByte(offset + 61) & 0xFF));
    }


    private final FuelFiguresDecoder fuelFigures = new FuelFiguresDecoder(this);

    public static long fuelFiguresDecoderId()
    {
        return 10;
    }

    public static int fuelFiguresDecoderSinceVersion()
    {
        return 0;
    }

    public FuelFiguresDecoder fuelFigures()
    {
        fuelFigures.wrap(buffer);
        return fuelFigures;
    }

    public static final class FuelFiguresDecoder
        implements Iterable<FuelFiguresDecoder>, java.util.Iterator<FuelFiguresDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final CarDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;

        FuelFiguresDecoder(final CarDecoder parentMessage)
        {
            this.parentMessage = parentMessage;
        }

        public void wrap(final DirectBuffer buffer)
        {
            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            final int limit = parentMessage.limit();
            parentMessage.limit(limit + HEADER_SIZE);
            blockLength = (buffer.getShort(limit + 0, BYTE_ORDER) & 0xFFFF);
            count = (buffer.getShort(limit + 2, BYTE_ORDER) & 0xFFFF);
        }

        public FuelFiguresDecoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
        }

        public static int countMinValue()
        {
            return 0;
        }

        public static int countMaxValue()
        {
            return 65534;
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 6;
        }

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int actingVersion()
        {
            return parentMessage.actingVersion;
        }

        public int count()
        {
            return count;
        }

        public java.util.Iterator<FuelFiguresDecoder> iterator()
        {
            return this;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return index < count;
        }

        public static int speedId()
        {
            return 11;
        }

        public static int speedSinceVersion()
        {
            return 0;
        }

        public static int speedEncodingOffset()
        {
            return 0;
        }

        public static int speedEncodingLength()
        {
            return 2;
        }

        public static String speedMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int speedNullValue()
        {
            return 65535;
        }

        public static int speedMinValue()
        {
            return 0;
        }

        public static int speedMaxValue()
        {
            return 65534;
        }

        public int speed()
        {
            return (buffer.getShort(offset + 0, BYTE_ORDER) & 0xFFFF);
        }


        public static int mpgId()
        {
            return 12;
        }

        public static int mpgSinceVersion()
        {
            return 0;
        }

        public static int mpgEncodingOffset()
        {
            return 2;
        }

        public static int mpgEncodingLength()
        {
            return 4;
        }

        public static String mpgMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static float mpgNullValue()
        {
            return Float.NaN;
        }

        public static float mpgMinValue()
        {
            return -3.4028234663852886E38f;
        }

        public static float mpgMaxValue()
        {
            return 3.4028234663852886E38f;
        }

        public float mpg()
        {
            return buffer.getFloat(offset + 2, BYTE_ORDER);
        }


        public static int usageDescriptionId()
        {
            return 200;
        }

        public static int usageDescriptionSinceVersion()
        {
            return 0;
        }

        public static String usageDescriptionCharacterEncoding()
        {
            return java.nio.charset.StandardCharsets.US_ASCII.name();
        }

        public static String usageDescriptionMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static int usageDescriptionHeaderLength()
        {
            return 4;
        }

        public int usageDescriptionLength()
        {
            final int limit = parentMessage.limit();
            return (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        }

        public int skipUsageDescription()
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
            final int dataOffset = limit + headerLength;
            parentMessage.limit(dataOffset + dataLength);

            return dataLength;
        }

        public int getUsageDescription(final MutableDirectBuffer dst, final int dstOffset, final int length)
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
            final int bytesCopied = Math.min(length, dataLength);
            parentMessage.limit(limit + headerLength + dataLength);
            buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public int getUsageDescription(final byte[] dst, final int dstOffset, final int length)
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
            final int bytesCopied = Math.min(length, dataLength);
            parentMessage.limit(limit + headerLength + dataLength);
            buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

            return bytesCopied;
        }

        public void wrapUsageDescription(final DirectBuffer wrapBuffer)
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
            parentMessage.limit(limit + headerLength + dataLength);
            wrapBuffer.wrap(buffer, limit + headerLength, dataLength);
        }

        public String usageDescription()
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
            parentMessage.limit(limit + headerLength + dataLength);

            if (0 == dataLength)
            {
                return "";
            }

            final byte[] tmp = new byte[dataLength];
            buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

            return new String(tmp, java.nio.charset.StandardCharsets.US_ASCII);
        }

        public int getUsageDescription(final Appendable appendable)
        {
            final int headerLength = 4;
            final int limit = parentMessage.limit();
            final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
            final int dataOffset = limit + headerLength;

            parentMessage.limit(dataOffset + dataLength);
            buffer.getStringWithoutLengthAscii(dataOffset, dataLength, appendable);

            return dataLength;
        }

        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("speed=");
            builder.append(this.speed());
            builder.append('|');
            builder.append("mpg=");
            builder.append(this.mpg());
            builder.append('|');
            builder.append("usageDescription=");
            builder.append('\'');
            getUsageDescription(builder);
            builder.append('\'');
            builder.append(')');

            return builder;
        }
        
        public FuelFiguresDecoder sbeSkip()
        {
            skipUsageDescription();

            return this;
        }
    }

    private final PerformanceFiguresDecoder performanceFigures = new PerformanceFiguresDecoder(this);

    public static long performanceFiguresDecoderId()
    {
        return 13;
    }

    public static int performanceFiguresDecoderSinceVersion()
    {
        return 0;
    }

    public PerformanceFiguresDecoder performanceFigures()
    {
        performanceFigures.wrap(buffer);
        return performanceFigures;
    }

    public static final class PerformanceFiguresDecoder
        implements Iterable<PerformanceFiguresDecoder>, java.util.Iterator<PerformanceFiguresDecoder>
    {
        public static final int HEADER_SIZE = 4;
        private final CarDecoder parentMessage;
        private DirectBuffer buffer;
        private int count;
        private int index;
        private int offset;
        private int blockLength;
        private final AccelerationDecoder acceleration;

        PerformanceFiguresDecoder(final CarDecoder parentMessage)
        {
            this.parentMessage = parentMessage;
            acceleration = new AccelerationDecoder(parentMessage);
        }

        public void wrap(final DirectBuffer buffer)
        {
            if (buffer != this.buffer)
            {
                this.buffer = buffer;
            }

            index = 0;
            final int limit = parentMessage.limit();
            parentMessage.limit(limit + HEADER_SIZE);
            blockLength = (buffer.getShort(limit + 0, BYTE_ORDER) & 0xFFFF);
            count = (buffer.getShort(limit + 2, BYTE_ORDER) & 0xFFFF);
        }

        public PerformanceFiguresDecoder next()
        {
            if (index >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
        }

        public static int countMinValue()
        {
            return 0;
        }

        public static int countMaxValue()
        {
            return 65534;
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 1;
        }

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int actingVersion()
        {
            return parentMessage.actingVersion;
        }

        public int count()
        {
            return count;
        }

        public java.util.Iterator<PerformanceFiguresDecoder> iterator()
        {
            return this;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return index < count;
        }

        public static int octaneRatingId()
        {
            return 14;
        }

        public static int octaneRatingSinceVersion()
        {
            return 0;
        }

        public static int octaneRatingEncodingOffset()
        {
            return 0;
        }

        public static int octaneRatingEncodingLength()
        {
            return 1;
        }

        public static String octaneRatingMetaAttribute(final MetaAttribute metaAttribute)
        {
            if (MetaAttribute.PRESENCE == metaAttribute)
            {
                return "required";
            }

            return "";
        }

        public static short octaneRatingNullValue()
        {
            return (short)255;
        }

        public static short octaneRatingMinValue()
        {
            return (short)90;
        }

        public static short octaneRatingMaxValue()
        {
            return (short)110;
        }

        public short octaneRating()
        {
            return ((short)(buffer.getByte(offset + 0) & 0xFF));
        }


        public static long accelerationDecoderId()
        {
            return 15;
        }

        public static int accelerationDecoderSinceVersion()
        {
            return 0;
        }

        public AccelerationDecoder acceleration()
        {
            acceleration.wrap(buffer);
            return acceleration;
        }

        public static final class AccelerationDecoder
            implements Iterable<AccelerationDecoder>, java.util.Iterator<AccelerationDecoder>
        {
            public static final int HEADER_SIZE = 4;
            private final CarDecoder parentMessage;
            private DirectBuffer buffer;
            private int count;
            private int index;
            private int offset;
            private int blockLength;

            AccelerationDecoder(final CarDecoder parentMessage)
            {
                this.parentMessage = parentMessage;
            }

            public void wrap(final DirectBuffer buffer)
            {
                if (buffer != this.buffer)
                {
                    this.buffer = buffer;
                }

                index = 0;
                final int limit = parentMessage.limit();
                parentMessage.limit(limit + HEADER_SIZE);
                blockLength = (buffer.getShort(limit + 0, BYTE_ORDER) & 0xFFFF);
                count = (buffer.getShort(limit + 2, BYTE_ORDER) & 0xFFFF);
            }

            public AccelerationDecoder next()
            {
                if (index >= count)
                {
                    throw new java.util.NoSuchElementException();
                }

                offset = parentMessage.limit();
                parentMessage.limit(offset + blockLength);
                ++index;

                return this;
            }

            public static int countMinValue()
            {
                return 0;
            }

            public static int countMaxValue()
            {
                return 65534;
            }

            public static int sbeHeaderSize()
            {
                return HEADER_SIZE;
            }

            public static int sbeBlockLength()
            {
                return 6;
            }

            public int actingBlockLength()
            {
                return blockLength;
            }

            public int actingVersion()
            {
                return parentMessage.actingVersion;
            }

            public int count()
            {
                return count;
            }

            public java.util.Iterator<AccelerationDecoder> iterator()
            {
                return this;
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public boolean hasNext()
            {
                return index < count;
            }

            public static int mphId()
            {
                return 16;
            }

            public static int mphSinceVersion()
            {
                return 0;
            }

            public static int mphEncodingOffset()
            {
                return 0;
            }

            public static int mphEncodingLength()
            {
                return 2;
            }

            public static String mphMetaAttribute(final MetaAttribute metaAttribute)
            {
                if (MetaAttribute.PRESENCE == metaAttribute)
                {
                    return "required";
                }

                return "";
            }

            public static int mphNullValue()
            {
                return 65535;
            }

            public static int mphMinValue()
            {
                return 0;
            }

            public static int mphMaxValue()
            {
                return 65534;
            }

            public int mph()
            {
                return (buffer.getShort(offset + 0, BYTE_ORDER) & 0xFFFF);
            }


            public static int secondsId()
            {
                return 17;
            }

            public static int secondsSinceVersion()
            {
                return 0;
            }

            public static int secondsEncodingOffset()
            {
                return 2;
            }

            public static int secondsEncodingLength()
            {
                return 4;
            }

            public static String secondsMetaAttribute(final MetaAttribute metaAttribute)
            {
                if (MetaAttribute.PRESENCE == metaAttribute)
                {
                    return "required";
                }

                return "";
            }

            public static float secondsNullValue()
            {
                return Float.NaN;
            }

            public static float secondsMinValue()
            {
                return -3.4028234663852886E38f;
            }

            public static float secondsMaxValue()
            {
                return 3.4028234663852886E38f;
            }

            public float seconds()
            {
                return buffer.getFloat(offset + 2, BYTE_ORDER);
            }


            public StringBuilder appendTo(final StringBuilder builder)
            {
                if (null == buffer)
                {
                    return builder;
                }

                builder.append('(');
                builder.append("mph=");
                builder.append(this.mph());
                builder.append('|');
                builder.append("seconds=");
                builder.append(this.seconds());
                builder.append(')');

                return builder;
            }
            
            public AccelerationDecoder sbeSkip()
            {

                return this;
            }
        }

        public StringBuilder appendTo(final StringBuilder builder)
        {
            if (null == buffer)
            {
                return builder;
            }

            builder.append('(');
            builder.append("octaneRating=");
            builder.append(this.octaneRating());
            builder.append('|');
            builder.append("acceleration=[");
            final int accelerationOriginalOffset = acceleration.offset;
            final int accelerationOriginalIndex = acceleration.index;
            final AccelerationDecoder acceleration = this.acceleration();
            if (acceleration.count() > 0)
            {
                while (acceleration.hasNext())
                {
                    acceleration.next().appendTo(builder);
                    builder.append(',');
                }
                builder.setLength(builder.length() - 1);
            }
            acceleration.offset = accelerationOriginalOffset;
            acceleration.index = accelerationOriginalIndex;
            builder.append(']');
            builder.append(')');

            return builder;
        }
        
        public PerformanceFiguresDecoder sbeSkip()
        {
            AccelerationDecoder acceleration = this.acceleration();
            if (acceleration.count() > 0)
            {
                while (acceleration.hasNext())
                {
                    acceleration.next();
                    acceleration.sbeSkip();
                }
            }

            return this;
        }
    }

    public static int manufacturerId()
    {
        return 18;
    }

    public static int manufacturerSinceVersion()
    {
        return 0;
    }

    public static String manufacturerCharacterEncoding()
    {
        return java.nio.charset.StandardCharsets.UTF_8.name();
    }

    public static String manufacturerMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int manufacturerHeaderLength()
    {
        return 4;
    }

    public int manufacturerLength()
    {
        final int limit = parentMessage.limit();
        return (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
    }

    public int skipManufacturer()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int dataOffset = limit + headerLength;
        parentMessage.limit(dataOffset + dataLength);

        return dataLength;
    }

    public int getManufacturer(final MutableDirectBuffer dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int getManufacturer(final byte[] dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public void wrapManufacturer(final DirectBuffer wrapBuffer)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);
        wrapBuffer.wrap(buffer, limit + headerLength, dataLength);
    }

    public String manufacturer()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);

        if (0 == dataLength)
        {
            return "";
        }

        final byte[] tmp = new byte[dataLength];
        buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

        return new String(tmp, java.nio.charset.StandardCharsets.UTF_8);
    }

    public static int modelId()
    {
        return 19;
    }

    public static int modelSinceVersion()
    {
        return 0;
    }

    public static String modelCharacterEncoding()
    {
        return java.nio.charset.StandardCharsets.UTF_8.name();
    }

    public static String modelMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int modelHeaderLength()
    {
        return 4;
    }

    public int modelLength()
    {
        final int limit = parentMessage.limit();
        return (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
    }

    public int skipModel()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int dataOffset = limit + headerLength;
        parentMessage.limit(dataOffset + dataLength);

        return dataLength;
    }

    public int getModel(final MutableDirectBuffer dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int getModel(final byte[] dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public void wrapModel(final DirectBuffer wrapBuffer)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);
        wrapBuffer.wrap(buffer, limit + headerLength, dataLength);
    }

    public String model()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);

        if (0 == dataLength)
        {
            return "";
        }

        final byte[] tmp = new byte[dataLength];
        buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

        return new String(tmp, java.nio.charset.StandardCharsets.UTF_8);
    }

    public static int activationCodeId()
    {
        return 20;
    }

    public static int activationCodeSinceVersion()
    {
        return 0;
    }

    public static String activationCodeCharacterEncoding()
    {
        return java.nio.charset.StandardCharsets.US_ASCII.name();
    }

    public static String activationCodeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int activationCodeHeaderLength()
    {
        return 4;
    }

    public int activationCodeLength()
    {
        final int limit = parentMessage.limit();
        return (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
    }

    public int skipActivationCode()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int dataOffset = limit + headerLength;
        parentMessage.limit(dataOffset + dataLength);

        return dataLength;
    }

    public int getActivationCode(final MutableDirectBuffer dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int getActivationCode(final byte[] dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public void wrapActivationCode(final DirectBuffer wrapBuffer)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);
        wrapBuffer.wrap(buffer, limit + headerLength, dataLength);
    }

    public String activationCode()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);

        if (0 == dataLength)
        {
            return "";
        }

        final byte[] tmp = new byte[dataLength];
        buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

        return new String(tmp, java.nio.charset.StandardCharsets.US_ASCII);
    }

    public int getActivationCode(final Appendable appendable)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, BYTE_ORDER) & 0xFFFF_FFFFL);
        final int dataOffset = limit + headerLength;

        parentMessage.limit(dataOffset + dataLength);
        buffer.getStringWithoutLengthAscii(dataOffset, dataLength, appendable);

        return dataLength;
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final CarDecoder decoder = new CarDecoder();
        decoder.wrap(buffer, offset, actingBlockLength, actingVersion);

        return decoder.appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final int originalLimit = limit();
        limit(offset + actingBlockLength);
        builder.append("[Car](sbeTemplateId=");
        builder.append(TEMPLATE_ID);
        builder.append("|sbeSchemaId=");
        builder.append(SCHEMA_ID);
        builder.append("|sbeSchemaVersion=");
        if (parentMessage.actingVersion != SCHEMA_VERSION)
        {
            builder.append(parentMessage.actingVersion);
            builder.append('/');
        }
        builder.append(SCHEMA_VERSION);
        builder.append("|sbeBlockLength=");
        if (actingBlockLength != BLOCK_LENGTH)
        {
            builder.append(actingBlockLength);
            builder.append('/');
        }
        builder.append(BLOCK_LENGTH);
        builder.append("):");
        builder.append("serialNumber=");
        builder.append(this.serialNumber());
        builder.append('|');
        builder.append("modelYear=");
        builder.append(this.modelYear());
        builder.append('|');
        builder.append("available=");
        builder.append(this.available());
        builder.append('|');
        builder.append("code=");
        builder.append(this.code());
        builder.append('|');
        builder.append("someNumbers=");
        builder.append('[');
        if (someNumbersLength() > 0)
        {
            for (int i = 0; i < someNumbersLength(); i++)
            {
                builder.append(this.someNumbers(i));
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        builder.append(']');
        builder.append('|');
        builder.append("vehicleCode=");
        for (int i = 0; i < vehicleCodeLength() && this.vehicleCode(i) > 0; i++)
        {
            builder.append((char)this.vehicleCode(i));
        }
        builder.append('|');
        builder.append("extras=");
        final OptionalExtrasDecoder extras = this.extras();
        if (null != extras)
        {
            extras.appendTo(builder);
        }
        else
        {
            builder.append("null");
        }
        builder.append('|');
        builder.append("discountedModel=");
        builder.append(this.discountedModel());
        builder.append('|');
        builder.append("engine=");
        final EngineDecoder engine = this.engine();
        if (null != engine)
        {
            engine.appendTo(builder);
        }
        else
        {
            builder.append("null");
        }
        builder.append('|');
        builder.append("uuid=");
        builder.append('[');
        if (uuidLength() > 0)
        {
            for (int i = 0; i < uuidLength(); i++)
            {
                builder.append(this.uuid(i));
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        builder.append(']');
        builder.append('|');
        builder.append("cupHolderCount=");
        builder.append(this.cupHolderCount());
        builder.append('|');
        builder.append("fuelFigures=[");
        final int fuelFiguresOriginalOffset = fuelFigures.offset;
        final int fuelFiguresOriginalIndex = fuelFigures.index;
        final FuelFiguresDecoder fuelFigures = this.fuelFigures();
        if (fuelFigures.count() > 0)
        {
            while (fuelFigures.hasNext())
            {
                fuelFigures.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        fuelFigures.offset = fuelFiguresOriginalOffset;
        fuelFigures.index = fuelFiguresOriginalIndex;
        builder.append(']');
        builder.append('|');
        builder.append("performanceFigures=[");
        final int performanceFiguresOriginalOffset = performanceFigures.offset;
        final int performanceFiguresOriginalIndex = performanceFigures.index;
        final PerformanceFiguresDecoder performanceFigures = this.performanceFigures();
        if (performanceFigures.count() > 0)
        {
            while (performanceFigures.hasNext())
            {
                performanceFigures.next().appendTo(builder);
                builder.append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        performanceFigures.offset = performanceFiguresOriginalOffset;
        performanceFigures.index = performanceFiguresOriginalIndex;
        builder.append(']');
        builder.append('|');
        builder.append("manufacturer=");
        builder.append('\'').append(manufacturer()).append('\'');
        builder.append('|');
        builder.append("model=");
        builder.append('\'').append(model()).append('\'');
        builder.append('|');
        builder.append("activationCode=");
        builder.append('\'');
        getActivationCode(builder);
        builder.append('\'');

        limit(originalLimit);

        return builder;
    }
    
    public CarDecoder sbeSkip()
    {
        sbeRewind();
        FuelFiguresDecoder fuelFigures = this.fuelFigures();
        if (fuelFigures.count() > 0)
        {
            while (fuelFigures.hasNext())
            {
                fuelFigures.next();
                fuelFigures.sbeSkip();
            }
        }
        PerformanceFiguresDecoder performanceFigures = this.performanceFigures();
        if (performanceFigures.count() > 0)
        {
            while (performanceFigures.hasNext())
            {
                performanceFigures.next();
                performanceFigures.sbeSkip();
            }
        }
        skipManufacturer();
        skipModel();
        skipActivationCode();

        return this;
    }
}
