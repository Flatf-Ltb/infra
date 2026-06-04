package io.flatf.infra.serialization.json;

import com.alibaba.fastjson2.JSONObject;
import io.flatf.common.epoch.EpochUnit;
import io.flatf.common.serialization.specific.JsonSerializable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;

@Getter
@Accessors(chain = true)
public class JsonObjectExt implements JsonSerializable {

    @Setter
    private String title;
    @Setter
    private long epochTime;
    @Setter
    private EpochUnit epochUnit;


    private JSONObject object;

    public <T> T getWith(Class<T> clazz) {
        return object.to(clazz);
    }

    public JsonObjectExt setObject(Object object) {
        return setObject(JSONObject.from(object));
    }

    public JsonObjectExt setObject(JSONObject object) {
        this.object = object;
        return this;
    }

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
