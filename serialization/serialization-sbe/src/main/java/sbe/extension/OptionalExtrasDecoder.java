/* Generated SBE (Simple Binary Encoding) message codec. */
package sbe.extension;

import org.agrona.DirectBuffer;

@SuppressWarnings("all")
public final class OptionalExtrasDecoder
{
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 1;
    public static final String SEMANTIC_VERSION = "5.2";
    public static final int ENCODED_LENGTH = 1;
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private int offset;
    private DirectBuffer buffer;

    public OptionalExtrasDecoder wrap(final DirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.offset = offset;

        return this;
    }

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public int encodedLength()
    {
        return ENCODED_LENGTH;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    private int actingVersion = SCHEMA_VERSION;

    public OptionalExtrasDecoder wrap(final DirectBuffer buffer, final int offset, final int actingVersion)
    {
        wrap(buffer, offset);
        this.actingVersion = actingVersion;

        return this;
    }

    public boolean isEmpty()
    {
        return 0 == buffer.getByte(offset);
    }

    public short getRaw()
    {
        return ((short)(buffer.getByte(offset) & 0xFF));
    }

    public boolean sunRoof()
    {
        return 0 != (buffer.getByte(offset) & (1 << 0));
    }

    public static boolean sunRoof(final byte value)
    {
        return 0 != (value & (1 << 0));
    }

    public boolean sportsPack()
    {
        return 0 != (buffer.getByte(offset) & (1 << 1));
    }

    public static boolean sportsPack(final byte value)
    {
        return 0 != (value & (1 << 1));
    }

    public boolean cruiseControl()
    {
        return 0 != (buffer.getByte(offset) & (1 << 2));
    }

    public static boolean cruiseControl(final byte value)
    {
        return 0 != (value & (1 << 2));
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        return appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        builder.append('{');
        boolean atLeastOne = false;
        if (sunRoof())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("sunRoof");
            atLeastOne = true;
        }
        if (sportsPack())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("sportsPack");
            atLeastOne = true;
        }
        if (cruiseControl())
        {
            if (atLeastOne)
            {
                builder.append(',');
            }
            builder.append("cruiseControl");
            atLeastOne = true;
        }
        builder.append('}');

        return builder;
    }
}
