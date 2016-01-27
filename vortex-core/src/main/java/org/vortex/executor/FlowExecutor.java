package org.vortex.executor;

import org.jdeferred.impl.DefaultDeferredManager;
import org.vortex.Settings;
import org.vortex.basic.StructuredLog;
import org.vortex.basic.primitive.Maps;
import org.vortex.basic.primitive.Pair;
import org.vortex.domain.Flow;
import org.vortex.domain.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class FlowExecutor implements Executor<FlowExecutor, Flow> {

    private List<Flow> tasks;
    private Settings settings;
    // WARNING: If executor is going to frequently going to be created
    // Figure out a different way of doing this, as this is relatively costly(takes few ms to
    // perform instantiating a structured logging construct
    protected StructuredLog LOGGER = new StructuredLog();
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
        LOGGER.info(Pair.pair("stage", "flowSubmit"), Pair.pair("stateCapture", stateCapture.info()));
        List<Map<String, String>> submitted = tasks.stream().map(executeTask()).collect(Collectors.toList());
        return Result.success("Submitted", Maps.<String, Object>map("flows", submitted));
    }

    public static FlowExecutor withDefaultCallbacks(Settings settings) {
        return new FlowExecutor(new HttpStateCapture(settings));
    }

}
