package org.vortex.basic.primitive;

public interface Callable1<Input, Output> {
    Output call(Input input) throws Exception;
}
