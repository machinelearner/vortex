package org.vortex.executor;

import org.vortex.basic.StructuredLog;
import org.vortex.basic.primitive.Callable1;
import org.vortex.basic.primitive.Lists;
import org.vortex.basic.primitive.Maps;
import org.vortex.basic.primitive.Pair;
import org.vortex.domain.Flow;
import org.vortex.domain.Result;
import org.vortex.executor.state.StateCapture;

import java.util.List;
import java.util.Map;


public class SynchronousFlowExecutor implements Executor<SynchronousFlowExecutor, Flow> {
    protected List<Flow> flows;
    protected StateCapture stateCapture;
    StructuredLog LOGGER = new StructuredLog(this.getClass());

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
            if (result.isSuccess())
                stateCapture.onDone(result);
            else
                stateCapture.onFail(result);
            return result;
        };
    }

    @Override
    public Result submit() {
        LOGGER.info(Pair.pair("stage", "SyncFlowSubmit"), Pair.pair("stage", "PreSubmit"), Pair.pair("stateCapture", stateCapture.info()));
        final List<Result> successResults = Lists.list();
        final List<Result> failedResults = Lists.list();
        boolean success = flows.stream()
                .allMatch(other -> {
                    try {
                        LOGGER.info(Pair.pair("stage", "SyncFlowSubmit"), Pair.pair("metadata", other.metadata()), Pair.pair("info", other.info()));
                        Result result = executeTask().call(other);
                        if (result.isFailure()) {
                            LOGGER.error(Pair.pair("stage", "SyncFlowSubmit"), Pair.pair("metadata", other.metadata()), Pair.pair("result", result));
                            failedResults.add(result);
                            return false;
                        }
                        LOGGER.info(Pair.pair("stage", "SyncFlowSubmit"), Pair.pair("stage", "PostSubmit"), Pair.pair("result", result.result()));
                        successResults.add(result);
                    } catch (Exception e) {
                        LOGGER.error(e, Pair.pair("stage", "SyncFlowSubmit"), Pair.pair("stage", "PostSubmit"));
                        failedResults.add(Result.failure("Failed while executing flow with an exception", Maps.map()));
                        return false;
                    }
                    return true;
                });

        return getFlowResult(successResults, failedResults, success);
    }

    private Result getFlowResult(List<Result> successResults, List<Result> failedResults, Boolean success) {
        String COMPLETION_MESSAGE = "Flow execution complete";
        String FAILED_COMPLETION_ADDENDUM = " with failures";
        Map<String, Object> resultValue = Maps.map("result", successResults, "success", success);
        if (!success) {
            COMPLETION_MESSAGE += FAILED_COMPLETION_ADDENDUM;
            resultValue.put("failed", failedResults);
        }
        return Result.success(COMPLETION_MESSAGE, resultValue);
    }
}
