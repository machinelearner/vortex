package org.vortex.help;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Lists {
    public static <T> List<T> list(T... values) {
        return Stream.of(values).collect(Collectors.toList());
    }
}
