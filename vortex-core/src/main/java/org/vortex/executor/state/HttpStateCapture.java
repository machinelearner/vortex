package org.vortex.executor.state;

import com.squareup.okhttp.*;
import org.vortex.Settings;
import org.vortex.basic.StructuredLog;
import org.vortex.basic.primitive.Pair;
import org.vortex.domain.Result;
import org.vortex.executor.ExecutorException;

import java.io.IOException;

public class HttpStateCapture implements StateCapture {
    public static final MediaType APPLICATION_JSON = MediaType.parse("application/json; charset=utf-8");
    private Settings settings;
    protected StructuredLog LOGGER = new StructuredLog();
    OkHttpClient client;

    public HttpStateCapture(Settings settings) {
        this.settings = settings;
        client = new OkHttpClient();
    }

    @Override
    public void onDone(Result success) {
        sendResult(success);
    }

    private void sendResult(Result result) {
        LOGGER.info(Pair.pair("stage", "stateCapture"), Pair.pair("captureType", "http"), Pair.pair("result", result));
        Request request = new Request.Builder().url(settings.notificationUrl("http")).post(RequestBody.create(APPLICATION_JSON, result.toJson().toJSONString())).build();
        Response response;
        try {
            response = client.newCall(request).execute();
        } catch (IOException e) {
            LOGGER.error(e, Pair.pair("target", settings.notificationUrl("http")), Pair.pair("result", e.getMessage()));
            throw new ExecutorException("Sending notification failed", e);
        }

        if (!response.isSuccessful()) {
            LOGGER.info(Pair.pair("captureType", "http"), Pair.pair("target", settings.notificationUrl("http")),
                    Pair.pair("result", response.code()));
            String message = response.body().toString();
            LOGGER.debug(Pair.pair("captureType", "http"), Pair.pair("target", settings.notificationUrl("http")),
                    Pair.pair("result", response.code()), Pair.pair("message", message));
            throw new ExecutorException(message);
        }
    }

    @Override
    public void onFail(Result failure) {
        sendResult(failure);
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void onFail(Throwable result) {

    }
}
