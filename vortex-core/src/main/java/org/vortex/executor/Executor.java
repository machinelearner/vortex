package org.vortex.executor;

import org.vortex.domain.Result;

public interface Executor<T extends Executor, A> {

    public Result submit();

    public T withStep(A a);
}
