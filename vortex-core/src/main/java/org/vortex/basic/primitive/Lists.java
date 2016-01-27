package org.vortex.basic.primitive;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Lists {
    @SafeVarargs
    public static <T> List<T> list(T... values) {
        return Stream.of(values).collect(Collectors.toList());
    }
}
