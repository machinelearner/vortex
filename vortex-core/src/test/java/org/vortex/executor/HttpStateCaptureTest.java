package org.vortex.executor;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.vortex.Settings;
import org.vortex.domain.Result;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class HttpStateCaptureTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(9009);
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private Settings settings;
    private HttpStateCapture httpStateCapture;

    @Before
    public void setUp() throws Exception {
        settings = Settings.overrideDefaults(new Settings() {{
            put("http.notification.url", "http://localhost:9009/notify/tenant");
        }});
        httpStateCapture = new HttpStateCapture(settings);
    }

    @Test
    public void shouldSendNotificationOnSuccess() {
        stubFor(post(urlEqualTo("/notify/tenant")).willReturn(aResponse().withStatus(201)));
        Result done = Result.success("done");

        httpStateCapture.onDone(done);

        verify(postRequestedFor(urlMatching("/notify/tenant"))
                .withRequestBody(equalToJson(done.toJson().toJSONString())));
    }

    @Test
    public void shouldErrorOutIfNotificationOnSuccessFails() {
        stubFor(post(urlEqualTo("/notify/tenant")).willReturn(aResponse().withStatus(400)));
        Result done = Result.success("done");

        expectedException.expect(ExecutorException.class);
        httpStateCapture.onDone(done);

        verify(postRequestedFor(urlMatching("/notify/tenant"))
                .withRequestBody(equalToJson(done.toJson().toJSONString())));
    }

}