package org.vortex.basic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vortex.basic.primitive.Lists;
import org.vortex.basic.primitive.Pair;
import sun.reflect.Reflection;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StructuredLog {

    public static final String KEY_SEPARATOR = ":";
    public static final String ATTRIBUTE_DELIMITER = " ";
    private final Logger logger;
    public static final List<Pair<String, Object>> CONSTANT_SUCCESS_LOG_ATTRIBUTES = Lists.list(Pair.of("status", "done"));

    public StructuredLog() {
        logger = logger(getCallerClass(2));
    }

    /**
     * Recommended to use this interface if the usage is more online and dynamic
     *
     * @param clazz
     */
    public StructuredLog(Class clazz) {
        logger = logger(clazz);
    }

    public Logger logger(Class callerClass) {
        return LoggerFactory.getLogger(callerClass);
    }

    Class getCallerClass(int callStackDepth) {
        return Reflection.getCallerClass(callStackDepth);
    }

    @SafeVarargs
    public final void error(Exception exception, Pair<String, Object>... errorAttributes) {
        List<Pair<String, Object>> ConstantErrorLogAttributes = Lists.list(Pair.of("status", "failed"), Pair.of("type", exception.getClass().getSimpleName()));
        Pair<String, Object[]> loggingFormatAndValues = logFormatAndValues(ConstantErrorLogAttributes, errorAttributes);
        logger.error(loggingFormatAndValues.first(), loggingFormatAndValues.second(), exception);
    }

    @SafeVarargs
    public final void error(Pair<String, Object>... errorAttributes) {
        error(new RuntimeException(), errorAttributes);
    }

    @SafeVarargs
    public final void error(Throwable throwable, Pair<String, Object>... errorAttributes) {
        List<Pair<String, Object>> ConstantErrorLogAttributes = Lists.list(Pair.of("status", "failed"), Pair.of("type", throwable.getClass().getSimpleName()));
        Pair<String, Object[]> loggingFormatAndValues = logFormatAndValues(ConstantErrorLogAttributes, errorAttributes);
        logger.error(loggingFormatAndValues.first(), loggingFormatAndValues.second(), throwable);
    }

    @SafeVarargs
    public final void info(Pair<String, Object>... attributes) {
        Pair<String, Object[]> loggingFormatAndValues = logFormatAndValues(CONSTANT_SUCCESS_LOG_ATTRIBUTES, attributes);
        logger.info(loggingFormatAndValues.first(), loggingFormatAndValues.second());
    }

    @SafeVarargs
    public final void debug(Pair<String, Object>... attributes) {
        Pair<String, Object[]> loggingFormatAndValues = logFormatAndValues(CONSTANT_SUCCESS_LOG_ATTRIBUTES, attributes);
        logger.debug(loggingFormatAndValues.first(), loggingFormatAndValues.second());
    }

    @SafeVarargs
    final Pair<String, Object[]> logFormatAndValues(List<Pair<String, Object>> constantAttributes, Pair<String, Object>... dynamicAttributes) {
        String messageFormat = messageFormat(Stream.concat(constantAttributes.stream(), Stream.of(dynamicAttributes)));
        Object[] loggingValues = loggingValues(Stream.concat(constantAttributes.stream(), Stream.of(dynamicAttributes)));
        return Pair.of(messageFormat, loggingValues);
    }

    /**
     * Provides the primitive format to express structured logging
     * <p>
     * TODO: Make separators of all kinds configurable parameters
     *
     * @param attributes
     * @return
     */
    String messageFormat(Stream<Pair<String, Object>> attributes) {
        return attributes
                .map(aPair -> aPair.first() + KEY_SEPARATOR + "{}")
                .collect(Collectors.joining(ATTRIBUTE_DELIMITER));
    }

    Object[] loggingValues(Stream<Pair<String, Object>> attributes) {
        return attributes
                .map(Pair::second)
                .toArray();
    }
}
