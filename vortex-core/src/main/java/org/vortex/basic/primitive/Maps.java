package org.vortex.basic.primitive;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class Maps {
    public static <T, Key> Map<Key, T> map(Iterator<? extends T> iterator, Function<? super T, ? extends Key> callable) {
        return map(new LinkedHashMap(), (Iterator) iterator, (Function) callable);
    }

    public static <K, V> Map<K, V> map(final Map<K, V> seed, final Stream<? extends Pair<? extends K, ? extends V>> entries) {
        entries.forEach(entry -> seed.put(entry.first(), entry.second()));
        return seed;
    }

    //Bloody mutates! Change immediately
    public static <T, Key> Map<Key, T> map(Map<Key, T> seed, Iterator<? extends T> iterator, Function<? super T, ? extends Key> callable) {
        iterator.forEachRemaining(entry -> seed.put(callable.apply(entry), entry));
        return seed;
    }

    public static <K, V> Map<K, V> map(final Stream<? extends Pair<? extends K, ? extends V>> entries) {
        return map(new LinkedHashMap<>(), entries);
    }

    public static <K, V> Map<K, V> map(K key, V value) {
        return map(Pair.pair(key, value));
    }

    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2) {
        return map(Pair.pair(key1, value1), Pair.pair(key2, value2));
    }

    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3) {
        return map(Pair.pair(key1, value1), Pair.pair(key2, value2), Pair.pair(key3, value3));
    }

    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        return map(Pair.pair(key1, value1), Pair.pair(key2, value2), Pair.pair(key3, value3), Pair.pair(key4, value4));
    }

    public static <K, V> Map<K, V> map(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4, K key5, V value5) {
        return map(Pair.pair(key1, value1), Pair.pair(key2, value2), Pair.pair(key3, value3), Pair.pair(key4, value4), Pair.pair(key5, value5));
    }

    public static <K, V> Map<K, V> map(final Pair<? extends K, ? extends V> first) {
        return map(Stream.<Pair<? extends K, ? extends V>>of(first));
    }

    public static <K, V> Map<K, V> map(final Pair<? extends K, ? extends V> first, final Pair<? extends K, ? extends V> second) {
        return map(Stream.<Pair<? extends K, ? extends V>>of(first, second));
    }

    public static <K, V> Map<K, V> map(final Pair<? extends K, ? extends V> first, final Pair<? extends K, ? extends V> second, final Pair<? extends K, ? extends V> third) {
        return map(Stream.<Pair<? extends K, ? extends V>>of(first, second, third));
    }

    public static <K, V> Map<K, V> map(final Pair<? extends K, ? extends V> first, final Pair<? extends K, ? extends V> second, final Pair<? extends K, ? extends V> third, final Pair<? extends K, ? extends V> fourth) {
        return map(Stream.<Pair<? extends K, ? extends V>>of(first, second, third, fourth));
    }

    public static <K, V> Map<K, V> map(final Pair<? extends K, ? extends V> first, final Pair<? extends K, ? extends V> second, final Pair<? extends K, ? extends V> third, final Pair<? extends K, ? extends V> fourth, final Pair<? extends K, ? extends V> fifth) {
        return map(Stream.<Pair<? extends K, ? extends V>>of(first, second, third, fourth, fifth));
    }

    public static <K, V> Map<K, V> map(final Pair<? extends K, ? extends V>... entries) {
        return map(Stream.of(entries));
    }

    public static <K, V> Map<K, V> map(final Map<K, V> seed, final Pair<? extends K, ? extends V>... entries) {
        Stream.of(entries).forEach(entry -> seed.put(entry.first(), entry.second()));
        return seed;
    }

    public static Object get(Map<String, Object> map, String key, Object defaultValue) {
        return map.containsKey(key) ? map.get(key) : defaultValue;
    }

    public static Pairs pairs(Map<String, Object> map) {
        return map.entrySet().stream().map(entry -> Pair.pair(entry.getKey(), entry.getValue())).collect(Pairs::new, Pairs::add, Pairs::addAll);
    }
}
