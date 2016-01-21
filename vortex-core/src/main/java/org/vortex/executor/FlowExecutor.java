package org.vortex.executor;

import org.jdeferred.impl.DefaultDeferredManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vortex.Settings;
import org.vortex.domain.Flow;
import org.vortex.domain.Result;
import org.vortex.help.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class FlowExecutor implements Executor<FlowExecutor, Flow> {

    private List<Flow> tasks;
    private Settings settings;
    protected Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private StateCapture stateCapture;


    public FlowExecutor(StateCapture stateCapture) {
        this.stateCapture = stateCapture;
        this.settings = settings;
        tasks = new ArrayList<>();
    }

    @Override
    public FlowExecutor withStep(Flow flow) {
        tasks.add(flow);
        return this;
    }

    protected Function<Flow, Map<String, String>> executeTask() {
        final DefaultDeferredManager defaultDeferredManager = new DefaultDeferredManager();
        return flow -> {
            defaultDeferredManager
                    .when(flow.execute())
                    .done(stateCapture)
                    .fail(stateCapture);
            return flow.info();
        };
    }

    @Override
    public Result submit() {
        LOGGER.info("Flow Executor submitted with {} way of capturing state", stateCapture.info());
        List<Map<String, String>> submitted = tasks.stream().map(executeTask()).collect(Collectors.toList());
        return Result.success("Submitted", Maps.<String, Object>map("flows", submitted));
    }

    public static FlowExecutor withDefaultCallbacks(Settings settings) {
        return new FlowExecutor(new HttpStateCapture(settings));
    }

}
