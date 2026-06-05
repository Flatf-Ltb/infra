package io.flatf.infra.serialization.json;


import io.flatf.infra.serialization.specific.JsonSerializable;

import javax.annotation.Nonnull;

public abstract class JsonBean implements JsonSerializable {

    @Override
    public String toString() {
        return toJson();
    }

    @Nonnull
    @Override
    public String toJson() {
        return JsonWriter.toJson(this);
    }

}
