package org.vortex.executor.state;

import org.jdeferred.DoneCallback;
import org.jdeferred.FailCallback;
import org.vortex.domain.Result;

public interface StateCapture extends DoneCallback<Result>, FailCallback<Throwable>{
    void onFail(Result result);

    void onDone(Result result);

    String info();
}
