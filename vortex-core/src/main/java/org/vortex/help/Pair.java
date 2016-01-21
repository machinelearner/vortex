package org.vortex.help;

import java.util.Objects;

public class Pair<FIRST, SECOND> {
    public final FIRST first;
    public final SECOND second;

    public Pair(FIRST first, SECOND second) {
        this.first = first;
        this.second = second;
    }

    public String toString() {
        return "Pair[" + first + "," + second + "]";
    }

    public boolean equals(Object var1) {
        return var1 instanceof Pair && Objects.equals(first, ((Pair) var1).first) && Objects.equals(second, ((Pair) var1).second);
    }

    public int hashCode() {
        return first == null ? (second == null ? 0 : second.hashCode() + 1) : (second == null ? first.hashCode() + 2 : first.hashCode() * 17 + second.hashCode());
    }

    public static <A, B> Pair<A, B> of(A aValue, B bValue) {
        return new Pair(aValue, bValue);
    }

    public static <A, B> Pair<A, B> pair(A aValue, B bValue) {
        return new Pair(aValue, bValue);
    }

    public FIRST first() {
        return first;
    }

    public SECOND second() {
        return second;
    }

    public String toTupleString() {
        return String.format("%s,%s", first, second);
    }
}
