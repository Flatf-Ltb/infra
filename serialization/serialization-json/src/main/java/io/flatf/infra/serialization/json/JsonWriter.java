package io.flatf.infra.serialization.json;

import javax.annotation.Nullable;

import static com.alibaba.fastjson2.JSON.toJSONString;
import static com.alibaba.fastjson2.JSONWriter.Feature.PrettyFormat;
import static com.alibaba.fastjson2.JSONWriter.Feature.WriteNulls;
import static io.flatf.common.constant.StringConst.EMPTY_STRING;

/**
 * @author yellow013
 * <p>
 */
public final class JsonWriter {

    private JsonWriter() {
    }

    /**
     * 返回JSON
     *
     * @param obj T
     * @return String
     */
    public static String toJson(@Nullable Object obj) {
        return obj == null ? EMPTY_STRING : toJSONString(obj);
    }

    /**
     * 以较高可视化的格式返回JSON
     *
     * @param obj T
     * @return String
     */
    public static String toPrettyJson(Object obj) {
        return obj == null ? EMPTY_STRING : toJSONString(obj, PrettyFormat);
    }

    /**
     * 返回包含Null值的JSON
     *
     * @param obj T
     * @return String
     */
    public static String toJsonHasNulls(Object obj) {
        return obj == null ? EMPTY_STRING
                : toJSONString(obj, WriteNulls);
    }

    /**
     * 以较高可视化的格式返回JSON, 并且包含Null值
     *
     * @param obj T
     * @return String
     */
    public static String toPrettyJsonHasNulls(Object obj) {
        return obj == null ? EMPTY_STRING
                : toJSONString(obj, PrettyFormat, WriteNulls);
    }

}
