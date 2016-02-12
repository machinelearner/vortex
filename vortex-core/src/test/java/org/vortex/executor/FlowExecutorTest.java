package org.vortex.executor;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.vortex.Settings;
import org.vortex.domain.Flow;
import org.vortex.domain.Result;
import org.vortex.basic.primitive.Maps;
import org.vortex.executor.state.StateCapture;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static name.mlnkrishnan.shouldJ.ShouldJ.it;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class FlowExecutorTest {
    private Settings settings;

    CountDownLatch countDown;
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8090);

    @Before
    public void setUp() throws Exception {
        settings = Settings.overrideDefaults(new Settings() {{
            put("dbUrl", "mongodb://localhost:27017");
            put("http.notification.url", "http://localhost:8090/notify");
        }});
        countDown = new CountDownLatch(1);
        stubFor(post(urlEqualTo("/notify"))
                .willReturn(aResponse().withStatus(200)));
    }

    @After
    public void tearDown() throws InterruptedException {
        //Wait before shutting down wiremock
        Thread.sleep(500);
    }

    @Test
    public void shouldCreateAsyncExecutor() throws InterruptedException {
        Flow aFlow = mock(Flow.class);
        when(aFlow.execute()).thenReturn(() -> Result.success("Done"));
        Flow anotherFlow = mock(Flow.class);
        when(anotherFlow.execute()).thenReturn(() -> Result.failure("Failed", Maps.<String, Object>map()));
        Result result = FlowExecutor.withDefaultCallbacks(settings)
                .withStep(aFlow)
                .withStep(anotherFlow)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
    }

    @Test
    public void shouldGetAllStepsInExecutorAsSubmittedStatusAsPartOfResult() {
        Flow aFlow = mock(Flow.class);
        when(aFlow.execute()).thenReturn(() -> Result.success("Done"));
        when(aFlow.info()).thenReturn(Maps.map("Source", "aSource"));
        Flow anotherFlow = mock(Flow.class);
        when(anotherFlow.execute()).thenReturn(() -> Result.failure("Failed", Maps.<String, Object>map()));
        when(anotherFlow.info()).thenReturn(Maps.map("Source", "anotherSource"));


        Result result = FlowExecutor.withDefaultCallbacks(settings)
                .withStep(aFlow)
                .withStep(anotherFlow)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        it(result.result().get("flows")).shouldBe(Arrays.asList(aFlow.info(), anotherFlow.info()));
    }

    @Test
    public void shouldSubmitAFlowAndCaptureSuccessState() throws InterruptedException {
        Flow aFlow = mock(Flow.class);
        when(aFlow.execute()).thenReturn(() -> Result.success("Done"));
        StateCapture mockStateCapture = mock(StateCapture.class);

        Result result = new FlowExecutor(mockStateCapture)
                .withStep(aFlow)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        Thread.sleep(2000);
        verify(mockStateCapture).onDone(Result.success("Done"));
    }

}