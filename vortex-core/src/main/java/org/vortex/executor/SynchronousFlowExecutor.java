package org.vortex.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vortex.domain.Flow;
import org.vortex.domain.Result;
import org.vortex.help.Callable1;
import org.vortex.help.Lists;
import org.vortex.help.Maps;

import java.util.List;


public class SynchronousFlowExecutor implements Executor<SynchronousFlowExecutor, Flow> {
    protected List<Flow> flows;
    protected StateCapture stateCapture;
    Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    public SynchronousFlowExecutor(StateCapture stateCapture) {
        this.stateCapture = stateCapture;
        this.flows = Lists.list();
    }

    @Override
    public SynchronousFlowExecutor withStep(Flow flow) {
        this.flows.add(flow);
        return this;
    }

    protected Callable1<Flow, Result> executeTask() {
        return flow -> {
            Result result = flow.execute().call();
            if(result.isSuccess())
                stateCapture.onDone(result);
            else
                stateCapture.onFail(result);
            return result;
        };
    }

    @Override
    public Result submit() {
        LOGGER.info("Flow Executor submitted with {} way of capturing state", stateCapture.info());
        final List<Result> successResults = Lists.list();
        final List<Result> failedResults = Lists.list();
        boolean success = flows.stream()
                .allMatch(other -> {
                    try {
                        LOGGER.info("Executing flow with metadata : {} and Info : {} ", other.metadata(), other.info());
                        Result result = executeTask().call(other);
                        if (result.isFailure()) {
                            LOGGER.info("Failed while executing flow with result {} ", result.result());
                            failedResults.add(result);
                            return false;
                        }
                        LOGGER.info("Done executing flow with result {} ", result.result());
                        successResults.add(result);
                    } catch (Exception e) {
                        LOGGER.error("Failed while executing flow with an exception", e);
                        failedResults.add(Result.failure("Failed while executing flow with an exception", Maps.map()));
                        return false;
                    }
                    return true;
                });

        return success? Result.success("Done running migrations", Maps.<String, Object>map("result", successResults, "success", success))
                : Result.success("Done running migrations with failure", Maps.<String, Object>map("result", successResults, "success", success, "failed", failedResults.get(0)));
    }
}
