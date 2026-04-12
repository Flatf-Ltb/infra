package io.flatf.common.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException.BadValue;
import com.typesafe.config.ConfigException.Missing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;

import static io.flatf.common.functional.Functions.getOrDefault;
import static io.flatf.common.functional.Functions.getOrThrows;
import static io.flatf.common.lang.Validator.nonNull;
import static io.flatf.common.util.StringSupport.nonEmpty;

public final class ConfigWrapper<O extends ConfigOption> {

    private final String module;

    private final Config config;

    public ConfigWrapper(@Nonnull Config config) {
        this("", config);
    }

    public ConfigWrapper(@Nullable String module, @Nonnull Config config) {
        nonNull(config, "config");
        this.config = config;
        this.module = nonEmpty(module)
                ? module.endsWith(".")
                ? module
                : module + "."
                : "";
    }

    /**
     * 是否存在此项配置, 并且配置不为空
     *
     * @param option OP
     * @return boolean
     */
    public boolean hasOption(@Nonnull O option) {
        return config.hasPath(option.getConfigName(module));
    }

    /**
     * 是否存在此项配置, 或此项配置为空
     *
     * @param option OP
     * @return boolean
     */
    public boolean hasOptionOrNull(@Nonnull O option) {
        return config.hasPathOrNull(option.getConfigName(module));
    }

    /**
     * 获取[boolean]配置值, 如果未配置, 默认值[false]
     *
     * @param option OP
     * @return boolean
     */
    public boolean getBoolean(@Nonnull O option) {
        return getBoolean(option, false);
    }

    /**
     * 获取[boolean]配置值, 如果未配置, 使用指定默认值
     *
     * @param option     OP
     * @param defaultVal boolean
     * @return boolean
     */
    public boolean getBoolean(@Nonnull O option, boolean defaultVal) {
        return getOrDefault(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getBoolean(option.getConfigName(module)),
                defaultVal);
    }

    /**
     * 获取[int]配置值, 如果未配置, 默认值[0]
     *
     * @param option OP
     * @return int
     */
    public int getInt(@Nonnull O option) {
        return getInt(option, 0);
    }

    /**
     * 获取[int]配置值, 如果未配置, 使用指定默认值
     *
     * @param option     OP
     * @param defaultVal int
     * @return int
     */
    public int getInt(@Nonnull O option, int defaultVal) {
        return getOrDefault(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getInt(option.getConfigName(module)),
                defaultVal);
    }

    /**
     * 获取[long]配置值, 如果未配置, 默认值[0L]
     *
     * @param option OP
     * @return long
     */
    public long getLong(@Nonnull O option) {
        return getLong(option, 0L);
    }

    /**
     * 获取[long]配置值, 如果未配置, 使用指定默认值
     *
     * @param option     OP
     * @param defaultVal long
     * @return long
     */
    public long getLong(@Nonnull O option, long defaultVal) {
        return getOrDefault(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getLong(option.getConfigName(module)),
                defaultVal);
    }

    /**
     * 获取[double]配置值, 如果未配置, 默认值[0.0D]
     *
     * @param option OP
     * @return double
     */
    public double getDouble(@Nonnull O option) {
        return getDouble(option, 0.0D);
    }

    /**
     * 获取[double]配置值, 如果未配置, 使用指定默认值
     *
     * @param option     OP
     * @param defaultVal double
     * @return double
     */
    public double getDouble(@Nonnull O option, double defaultVal) {
        return getOrDefault(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getDouble(option.getConfigName(module)),
                defaultVal);
    }

    /**
     * 获取[String]配置值, 如果未配置, 默认值[""]
     *
     * @param option OP
     * @return String
     */
    public String getString(@Nonnull O option) {
        return getString(option, "");
    }

    /**
     * 获取[String]配置值, 如果未配置, 使用指定默认值
     *
     * @param option     OP
     * @param defaultVal String
     * @return String
     */
    public String getString(@Nonnull O option, @Nonnull String defaultVal) {
        return getOrDefault(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getString(option.getConfigName(module)),
                defaultVal);
    }

    /**
     * @param option OP
     * @return boolean
     * @throws Missing exception
     */
    public boolean getBooleanOrThrows(@Nonnull O option) throws Missing {
        return getOrThrows(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getBoolean(option.getConfigName(module)),
                new Missing(option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @return int
     * @throws Missing exception
     */
    public int getIntOrThrows(@Nonnull O option) throws Missing {
        return getOrThrows(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getInt(option.getConfigName(module)),
                new Missing(option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify IntPredicate
     * @return int
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public int getIntOrThrows(@Nonnull O option, IntPredicate verify)
            throws Missing, BadValue {
        return getIntOrThrows(option, verify,
                new IllegalArgumentException("Illegal argument -> " + option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify IntPredicate
     * @param ex     Exception
     * @return int
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public int getIntOrThrows(@Nonnull O option, IntPredicate verify, Exception ex)
            throws Missing, BadValue {
        int value = getIntOrThrows(option);
        if (verify.test(value))
            return value;
        else
            throw new BadValue(option.getConfigName(module), "value == " + value, ex);
    }

    /**
     * @param option OP
     * @return long
     * @throws Missing exception
     */
    public long getLongOrThrows(@Nonnull O option) throws Missing {
        return getOrThrows(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getLong(option.getConfigName(module)),
                new Missing(option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify LongPredicate
     * @return long
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public long getLongOrThrows(@Nonnull O option, LongPredicate verify)
            throws Missing, BadValue {
        return getLongOrThrows(option, verify,
                new IllegalArgumentException("Illegal argument -> " + option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify LongPredicate
     * @param ex     Exception
     * @return long
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public long getLongOrThrows(@Nonnull O option, LongPredicate verify, Exception ex)
            throws Missing, BadValue {
        long value = getLongOrThrows(option);
        if (verify.test(value))
            return value;
        else
            throw new BadValue(option.getConfigName(module), "value == " + value, ex);
    }

    /**
     * @param option OP
     * @return double
     * @throws Missing exception
     */
    public double getDoubleOrThrows(@Nonnull O option) throws Missing {
        return getOrThrows(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getDouble(option.getConfigName(module)),
                new Missing(option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify DoublePredicate
     * @return double
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public double getDoubleOrThrows(@Nonnull O option, DoublePredicate verify)
            throws Missing, BadValue {
        return getDoubleOrThrows(option, verify,
                new IllegalArgumentException("Illegal argument -> " + option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify DoublePredicate
     * @param ex     Exception
     * @return double
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public double getDoubleOrThrows(@Nonnull O option, DoublePredicate verify, Exception ex)
            throws Missing, BadValue {
        double value = getDoubleOrThrows(option);
        if (verify.test(value))
            return value;
        else
            throw new BadValue(option.getConfigName(module), "value == " + value, ex);
    }

    /**
     * @param option OP
     * @return String
     * @throws Missing exception
     */
    public String getStringOrThrows(@Nonnull O option) throws Missing {
        return getOrThrows(
                () -> config.hasPath(option.getConfigName(module)),
                () -> config.getString(option.getConfigName(module)),
                new Missing(option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify Predicate<String>
     * @return String
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public String getStringOrThrows(@Nonnull O option, Predicate<String> verify)
            throws Missing, BadValue {
        return getStringOrThrows(option, verify,
                new IllegalArgumentException("Illegal argument -> " + option.getConfigName(module)));
    }

    /**
     * @param option OP
     * @param verify Predicate<String>
     * @param ex     Exception
     * @return String
     * @throws Missing  exception
     * @throws BadValue exception
     */
    public String getStringOrThrows(@Nonnull O option, Predicate<String> verify, Exception ex)
            throws Missing, BadValue {
        String value = getStringOrThrows(option);
        if (verify.test(value))
            return value;
        else
            throw new BadValue(
                    option.getConfigName(module), "value == " + value, ex);
    }

}
