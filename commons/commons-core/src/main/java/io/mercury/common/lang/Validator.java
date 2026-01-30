package io.mercury.common.lang;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static io.mercury.common.lang.Predicates.isAtWithinRange;
import static io.mercury.common.lang.Predicates.isGreaterOrEqualThan;
import static io.mercury.common.lang.Predicates.isGreaterThan;
import static io.mercury.common.lang.Predicates.isLessOrEqualThan;
import static io.mercury.common.lang.Predicates.isLessThan;
import static io.mercury.common.util.StringSupport.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class Validator {

    private Validator() {
    }

    /**
     * @param value   int
     * @param min     int
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void greaterThan(int value, int min, String objName)
            throws IllegalArgumentException {
        greaterThan(value, min, objName, null);
    }

    /**
     * @param value   int
     * @param min     int
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void greaterThan(int value, int min, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isGreaterThan(value, min)) {
            if (logger != null)
                logger.error("illegal int param [{}] == {}, min limit: {}", objName, value, min);
            throw new IllegalArgumentException("IntParam: [" + objName + " ] must greater than " + min);
        }
    }

    /**
     * @param value   long
     * @param min     long
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void greaterThan(long value, long min, String objName)
            throws IllegalArgumentException {
        greaterThan(value, min, objName, null);
    }

    /**
     * @param value   long
     * @param min     long
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void greaterThan(long value, long min, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isGreaterThan(value, min)) {
            if (logger != null)
                logger.error("illegal long param [{}] == {}, min limit: {}", objName, value, min);
            throw new IllegalArgumentException("LongParam: [" + objName + "] must greater than " + min);
        }
    }

    /**
     * @param value   int
     * @param min     int
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void greaterOrEqualThan(int value, int min, String objName)
            throws IllegalArgumentException {
        greaterOrEqualThan(value, min, objName, null);
    }

    /**
     * @param value   int
     * @param min     int
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void greaterOrEqualThan(int value, int min, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isGreaterOrEqualThan(value, min)) {
            if (logger != null)
                logger.error("illegal int param [{}] == {}, min limit: {}", objName, value, min);
            throw new IllegalArgumentException("IntParam: [" + objName + "] must greater or equal than " + min);
        }
    }

    /**
     * @param value   long
     * @param min     long
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void greaterOrEqualThan(long value, long min, String objName)
            throws IllegalArgumentException {
        greaterOrEqualThan(value, min, objName, null);
    }

    /**
     * @param value   long
     * @param min     long
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void greaterOrEqualThan(long value, long min, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isGreaterOrEqualThan(value, min)) {
            if (logger != null)
                logger.error("illegal long param [{}] == {}, min limit: {}", objName, value, min);
            throw new IllegalArgumentException("LongParam: [" + objName + "] must greater or equal than " + min);
        }
    }

    /**
     * @param value   int
     * @param max     int
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void lessThan(int value, int max, String objName)
            throws IllegalArgumentException {
        lessThan(value, max, objName, null);
    }

    /**
     * @param value   int
     * @param max     int
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void lessThan(int value, int max, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isLessThan(value, max)) {
            if (logger != null)
                logger.error("illegal int param [{}] == {}, max limit: {}", objName, value, max);
            throw new IllegalArgumentException("IntParam: [" + objName + "] must less than " + max);
        }
    }

    /**
     * @param value   long
     * @param max     long
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void lessThan(long value, long max, String objName)
            throws IllegalArgumentException {
        lessThan(value, max, objName, null);
    }

    /**
     * @param value   long
     * @param max     long
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void lessThan(long value, long max, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isLessThan(value, max)) {
            if (logger != null)
                logger.error("illegal long param [{}] == {}, max limit: {}", objName, value, max);
            throw new IllegalArgumentException("LongParam: [" + objName + "] must less than " + max);
        }
    }

    /**
     * @param value   int
     * @param max     int
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void lessOrEqualThan(int value, int max, String objName)
            throws IllegalArgumentException {
        lessOrEqualThan(value, max, objName, null);
    }

    /**
     * @param value   int
     * @param max     int
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void lessOrEqualThan(int value, int max, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isLessOrEqualThan(value, max)) {
            if (logger != null)
                logger.error("illegal int param [{}] == {}, max limit: {}", objName, value, max);
            throw new IllegalArgumentException("Param: [" + objName + "] must less or equal than " + max);
        }
    }

    /**
     * @param value   long
     * @param max     long
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void lessOrEqualThan(long value, long max, String objName)
            throws IllegalArgumentException {
        lessOrEqualThan(value, max, objName, null);
    }

    /**
     * @param value   long
     * @param max     long
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void lessOrEqualThan(long value, long max, String objName, Logger logger)
            throws IllegalArgumentException {
        if (!isLessOrEqualThan(value, max)) {
            if (logger != null)
                logger.error("illegal long param [{}] == {}, max limit: {}", objName, value, max);
            throw new IllegalArgumentException("Param: [" + objName + "] must less or equal than " + max);
        }
    }

    /**
     * @param value   int
     * @param min     int
     * @param max     int
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void atWithinRange(int value, int min, int max, String objName)
            throws IllegalArgumentException {
        atWithinRange(value, min, max, objName, null);
    }

    /**
     * @param value   int
     * @param min     int
     * @param max     int
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void atWithinRange(int value, int min, int max, String objName, Logger logger) {
        if (!isAtWithinRange(value, min, max)) {
            if (logger != null)
                logger.error("illegal int param [{}] == {}, min limit: {}, max limit: {}", objName, value, min, max);
            throw new IllegalArgumentException(
                    "Param: [" + objName + "] must in the range of [" + min + "] to [" + max + "]");
        }
    }

    /**
     * @param value   long
     * @param min     long
     * @param max     long
     * @param objName String
     * @throws IllegalArgumentException exception
     */
    public static void atWithinRange(long value, long min, long max, String objName)
            throws IllegalArgumentException {
        atWithinRange(value, min, max, objName, null);
    }

    /**
     * @param value   long
     * @param min     long
     * @param max     long
     * @param objName String
     * @param logger  Logger
     * @throws IllegalArgumentException exception
     */
    public static void atWithinRange(long value, long min, long max, String objName, Logger logger) {
        if (!isAtWithinRange(value, min, max)) {
            if (logger != null)
                logger.error("illegal long param [{}]: {}, min limit: {}, max limit: {}", objName, value, min, max);
            throw new IllegalArgumentException(
                    "Param: [" + objName + "] must in the range of [" + min + "] to [" + max + "]");
        }
    }

    /**
     * @param t T
     * @return T
     * @throws NullPointerException exception
     */
    public static <T> T nonNull(T t) throws NullPointerException {
        return nonNull(t, "");
    }

    /**
     * @param t       T
     * @param objName String
     * @return T
     * @throws NullPointerException exception
     */
    public static <T> T nonNull(T t, @Nonnull String objName) throws NullPointerException {
        return requireNonNull(t, isNullOrEmpty(objName)
                ? "param cannot be null" : "param [" + objName + "] cannot be null");
    }

    /**
     * @param t T
     * @param e E
     * @return T
     * @throws E exception
     */
    public static <T, E extends Throwable> T nonNull(T t, E e) throws E {
        if (t == null)
            throw e;
        return t;
    }

    /**
     * @param str     String
     * @param objName String
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static void nonEmpty(String str, String objName)
            throws NullPointerException, IllegalArgumentException {
        nonEmpty(str, objName, null);
    }

    /**
     * @param str     String
     * @param objName String
     * @param logger  Logger
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     *
     */
    public static void nonEmpty(String str, String objName, Logger logger)
            throws NullPointerException, IllegalArgumentException {
        if (str == null) {
            if (logger != null)
                logger.error("StringParam: [{}] can not be null", objName);
            throw new NullPointerException("StringParam: [" + objName + "] can not be null");
        }
        if (str.isEmpty()) {
            if (logger != null)
                logger.error("StringParam: [{}] can not be empty", objName);
            throw new IllegalArgumentException("StringParam: [" + objName + "] can not be empty");
        }
    }


    /**
     * @param collection C
     * @param objName    String
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static <C extends Collection<E>, E> void nonEmptyCollection(C collection, String objName)
            throws NullPointerException, IllegalArgumentException {
        if (collection == null)
            throw new NullPointerException("Param: [" + objName + "] can not be null");
        if (collection.isEmpty())
            throw new IllegalArgumentException("Param: [" + objName + "] can not be empty");
    }

    /**
     * @param map     M
     * @param objName String
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static <M extends Map<?, ?>> void nonEmptyMap(M map, String objName)
            throws NullPointerException, IllegalArgumentException {
        if (map == null)
            throw new NullPointerException("Param: [" + objName + "] can not be null");
        if (map.isEmpty())
            throw new IllegalArgumentException("Param: [" + objName + "] can not be empty");
    }

    /**
     * @param array          T[]
     * @param requiredLength int
     * @param arrayName      String
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static <T> void requiredLength(T[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
    }

    /**
     * @param collection     Collection<E>
     * @param requiredLength int
     * @param arrayName      String
     * @return Collection<E>
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static <E> Collection<E> requiredLength(Collection<E> collection, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (collection == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (collection.size() < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
        return collection;
    }

    /**
     * @param list           List<T>
     * @param requiredLength int
     * @param listName       String
     * @return List<T>
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static <T> List<T> requiredLength(List<T> list, int requiredLength, String listName)
            throws NullPointerException, IllegalArgumentException {
        if (list == null)
            throw new NullPointerException("Param: [" + listName + "] can not be null");
        if (list.size() < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + listName + "] length must be greater than " + requiredLength);
        return list;
    }

    /**
     * @param array          boolean[]
     * @param requiredLength int
     * @param arrayName      String
     * @return boolean[]
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static boolean[] requiredLength(boolean[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
        return array;
    }

    /**
     * @param array          byte[]
     * @param requiredLength int
     * @param arrayName      String
     * @return byte[]
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static byte[] requiredLength(byte[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
        return array;
    }

    /**
     * @param array          char[]
     * @param requiredLength int
     * @param arrayName      String
     * @return char[]
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static char[] requiredLength(char[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
        return array;
    }

    /**
     * @param array          int[]
     * @param requiredLength int
     * @param arrayName      String
     * @return int[]
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static int[] requiredLength(int[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
        return array;
    }

    /**
     * @param array          long[]
     * @param requiredLength int
     * @param arrayName      String
     * @return long[]
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static long[] requiredLength(long[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
        return array;
    }

    /**
     * @param array          float[]
     * @param requiredLength int
     * @param arrayName      String
     * @return float[]
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static float[] requiredLength(float[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + "] can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + "] length must be greater than " + requiredLength);
        return array;
    }

    /**
     * @param array          double[]
     * @param requiredLength int
     * @param arrayName      String
     * @return double[]
     * @throws NullPointerException     exception
     * @throws IllegalArgumentException exception
     */
    public static double[] requiredLength(double[] array, int requiredLength, String arrayName)
            throws NullPointerException, IllegalArgumentException {
        if (array == null)
            throw new NullPointerException("Param: [" + arrayName + " can not be null");
        if (array.length < requiredLength)
            throw new IllegalArgumentException(
                    "Param: [" + arrayName + " length must be greater than " + requiredLength);
        return array;
    }

    /**
     * @param param     T
     * @param predicate Predicate<T>
     * @param paramName String
     * @return T
     * @throws IllegalArgumentException exception
     */
    public static <T> T isValid(T param, Predicate<T> predicate, String paramName) throws IllegalArgumentException {
        return isValid(param, predicate, new IllegalArgumentException("Param: [" + paramName + "] is illegal"));
    }

    /**
     * @param param     T
     * @param predicate Predicate<T>
     * @param exception E
     * @return T
     * @throws E exception
     */
    public static <T, E extends Exception> T isValid(T param, Predicate<T> predicate, E exception) throws E {
        if (predicate.test(param))
            return param;
        throw exception;
    }

}
