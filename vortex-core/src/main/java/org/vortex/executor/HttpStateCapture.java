package org.vortex.executor;

import com.squareup.okhttp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vortex.Settings;
import org.vortex.domain.Result;

import java.io.IOException;

public class HttpStateCapture implements StateCapture {
    public static final MediaType APPLICATION_JSON = MediaType.parse("application/json; charset=utf-8");
    private Settings settings;
    protected Logger LOGGER = LoggerFactory.getLogger(this.getClass());
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
        Request request = new Request.Builder().url(settings.notificationUrl("http")).post(RequestBody.create(APPLICATION_JSON, result.toJson().toJSONString())).build();
        Response response = null;
        try {
            response = client.newCall(request).execute();
        } catch (IOException e) {
            LOGGER.error("Sending notification to {} failed with error: {}", settings.notificationUrl("http"), e.getMessage());
            throw new ExecutorException("Sending notification failed", e);        }

        if (!response.isSuccessful()) {
            LOGGER.info("Sending notification to {} failed with status: {}", settings.notificationUrl("http"), response.code());
            String message = response.body().toString();
            LOGGER.debug("Sending notification to {} failed with response: {}", settings.notificationUrl("http"), message);
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
