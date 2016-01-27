package org.vortex.basic;

import org.junit.Test;
import org.vortex.basic.primitive.Pair;

import java.util.stream.Stream;

import static name.mlnkrishnan.shouldJ.ShouldJ.it;

public class StructuredLogTest {
    @Test
    public void shouldCreateALogMessageFormatGivenOneAttributePair() {
        String format = new StructuredLog().messageFormat(Stream.of(Pair.of("key1", "value1")));
        it(format).shouldBe("key1:{}");
    }

    @Test
    public void shouldCreateALogMessageFormatGivenManyAttributePairs() {
        String format = new StructuredLog().messageFormat(Stream.of(Pair.of("key1", "value1"), Pair.of("key2", "value2")));
        it(format).shouldBe("key1:{} key2:{}");
    }

    @Test
    public void shouldCreateALoggingValuesGivenOneAttributePair() {
        Object[] loggingValues = new StructuredLog().loggingValues(Stream.of(Pair.of("key1", "value1")));
        it(loggingValues).shouldBeOfLength(1);
        it(loggingValues).shouldHave("value1");
    }

    @Test
    public void shouldCreateALoggingValuesGivenManyAttributePairs() {
        Object[] loggingValues = new StructuredLog().loggingValues(Stream.of(Pair.of("key1", "value1"), Pair.of("key2", "value2")));
        it(loggingValues).shouldBeOfLength(2);
        it(loggingValues).shouldHave("value1");
        it(loggingValues).shouldHave("value2");
    }

}