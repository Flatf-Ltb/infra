package io.flatf.infra.serialization.json;

import io.flatf.common.epoch.EpochUnit;
import io.flatf.infra.serialization.ContentType;
import io.flatf.infra.serialization.specific.JsonSerializable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Getter
@Setter
@Accessors(chain = true)
public final class JsonMsg implements JsonSerializable {

    private long sequence;
    private long epoch;
    private EpochUnit epochUnit = EpochUnit.MILLIS;
    private int envelope;
    private int version = 1;
    private ContentType contentType;
    private String content;

    @Override
    public String toString() {
        return toJson();
    }

    @Nonnull
    @Override
    public String toJson() {
        return JsonWriter.toJson(this);
    }

    /**
     * @param json String
     * @return JsonMsg
     */
    @Nullable
    public static JsonMsg fromJson(String json) {
        return JsonReader.toObject(json, JsonMsg.class);
    }

}
