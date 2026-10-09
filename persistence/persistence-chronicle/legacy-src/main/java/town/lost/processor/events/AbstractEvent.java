package town.lost.processor.events;

import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.MicroTimestampLongConverter;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public abstract class AbstractEvent<E extends AbstractEvent<E>>
        extends SelfDescribingMarshallable {

    private String eventSource;

    @LongConversion(MicroTimestampLongConverter.class)
    private long eventTimeStamp;

    public String eventSource() {
        return eventSource;
    }

    public E eventSource(String eventSource) {
        this.eventSource = eventSource;
        return self();
    }

    public long eventTimeStamp() {
        return eventTimeStamp;
    }

    public E eventTimeStamp(long eventTimeStamp) {
        this.eventTimeStamp = eventTimeStamp;
        return self();
    }

    protected abstract E self();

}
