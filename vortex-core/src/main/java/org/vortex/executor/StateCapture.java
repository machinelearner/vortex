package org.vortex.executor;

import org.jdeferred.DoneCallback;
import org.jdeferred.FailCallback;
import org.vortex.domain.Result;

public interface StateCapture extends DoneCallback<Result>, FailCallback<Throwable>{
    public void onFail(Result result);

    public void onDone(Result result);

    public String info();
}
