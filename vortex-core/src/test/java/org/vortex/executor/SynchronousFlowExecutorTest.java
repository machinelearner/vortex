package org.vortex.executor;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.vortex.Settings;
import org.vortex.basic.primitive.Maps;
import org.vortex.domain.Flow;
import org.vortex.domain.Result;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static name.mlnkrishnan.shouldJ.ShouldJ.it;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class SynchronousFlowExecutorTest {
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

    @Test
    public void shouldExecuteAllStepsInExecutorSuccessfully() {
        Flow aFlow = mock(Flow.class);
        final Result aFlowResult = Result.success("Done");
        when(aFlow.execute()).thenReturn(() -> aFlowResult);
        when(aFlow.info()).thenReturn(Maps.map("Source", "aSource"));
        Flow anotherFlow = mock(Flow.class);
        final Result anotherFlowResult = Result.success("Done", Maps.<String, Object>map());
        when(anotherFlow.execute()).thenReturn(() -> anotherFlowResult);
        when(anotherFlow.info()).thenReturn(Maps.map("Source", "anotherSource"));


        Result result = new SynchronousFlowExecutor(new HttpStateCapture(settings))
                .withStep(aFlow)
                .withStep(anotherFlow)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        it(result.result().get("result")).shouldBe(Arrays.asList(aFlowResult, anotherFlowResult));
        it(result.result().get("success")).shouldBe(true);
    }

    @Test
    public void shouldNotExecutePendingFlowsWhenOneFails() {
        Flow aFlow = mock(Flow.class);
        final Result aFlowResult = Result.success("Done", Maps.<String, Object>map("id", "aFlow", "Source", "aSource"));
        when(aFlow.execute()).thenReturn(() -> aFlowResult);

        Flow anotherFlow1 = mock(Flow.class);
        final Result anotherFlow1Result = Result.success("Done", Maps.<String, Object>map("id", "anotherFlow1", "Source", "anotherSource1"));
        when(anotherFlow1.execute()).thenReturn(() -> anotherFlow1Result);

        Flow anotherFlow2 = mock(Flow.class);
        final Result anotherFlow2Result = Result.failure("Failed", Maps.<String, Object>map("id", "anotherFlow2", "Source", "anotherSource2"));
        when(anotherFlow2.execute()).thenReturn(() -> anotherFlow2Result);

        Flow anotherFlow3 = mock(Flow.class);
        final Result anotherFlow3Result = Result.success("Done", Maps.<String, Object>map("id", "anotherFlow3", "Source", "anotherSource3"));
        when(anotherFlow3.execute()).thenReturn(() -> anotherFlow3Result);


        Result result = new SynchronousFlowExecutor(new HttpStateCapture(settings))
                .withStep(aFlow)
                .withStep(anotherFlow1)
                .withStep(anotherFlow2)
                .withStep(anotherFlow3)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        it(result.result().get("result")).shouldBe(Arrays.asList(aFlowResult, anotherFlow1Result));
        it(result.result().get("failed")).shouldBe(anotherFlow2Result);
        it(result.result().get("success")).shouldBe(false);
    }

    @Test
    public void shouldSubmitFlowsAndCaptureStates() {
        Flow aFlow = mock(Flow.class);
        final Result aFlowResult = Result.success("Done", Maps.<String, Object>map("id", "aFlow", "Source", "aSource"));
        when(aFlow.execute()).thenReturn(() -> aFlowResult);

        Flow anotherFlow1 = mock(Flow.class);
        final Result anotherFlow1Result = Result.success("Done", Maps.<String, Object>map("id", "anotherFlow1", "Source", "anotherSource1"));
        when(anotherFlow1.execute()).thenReturn(() -> anotherFlow1Result);

        Flow anotherFlow2 = mock(Flow.class);
        final Result anotherFlow2Result = Result.success("Done", Maps.<String, Object>map("id", "anotherFlow2", "Source", "anotherSource2"));
        when(anotherFlow2.execute()).thenReturn(() -> anotherFlow2Result);

        Flow anotherFlow3 = mock(Flow.class);
        final Result anotherFlow3Result = Result.failure("Failed", Maps.<String, Object>map("id", "anotherFlow3", "Source", "anotherSource3"));
        when(anotherFlow3.execute()).thenReturn(() -> anotherFlow3Result);

        Flow anotherFlow4 = mock(Flow.class);
        final Result anotherFlow4Result = Result.failure("Failed", Maps.<String, Object>map("id", "anotherFlow4", "Source", "anotherSource4"));
        when(anotherFlow4.execute()).thenReturn(() -> anotherFlow4Result);
        StateCapture stateCapture = mock(StateCapture.class);

        Result result = new SynchronousFlowExecutor(stateCapture)
                .withStep(aFlow)
                .withStep(anotherFlow1)
                .withStep(anotherFlow2)
                .withStep(anotherFlow3)
                .withStep(anotherFlow4)
                .submit();

        verifyNoMoreInteractions(anotherFlow4);
        it(result.isSuccess()).shouldBeTrue();
        it(result.result().get("result")).shouldBe(Arrays.asList(aFlowResult, anotherFlow1Result, anotherFlow2Result));
        it(result.result().get("failed")).shouldBe(anotherFlow3Result);
        it(result.result().get("success")).shouldBe(false);

        verify(stateCapture, times(1)).onFail(anotherFlow3Result);
        ArgumentCaptor<Result> captor = ArgumentCaptor.forClass(Result.class);
        verify(stateCapture, times(3)).onDone(captor.capture());

        it(captor.getAllValues()).shouldHaveAll(aFlowResult, anotherFlow1Result, anotherFlow2Result).shouldNotHave(anotherFlow3Result).shouldNotHave(anotherFlow4Result);
        it(captor.getAllValues()).shouldHaveAll(aFlowResult, anotherFlow1Result, anotherFlow2Result).shouldNotHave(anotherFlow3Result).shouldNotHave(anotherFlow4Result);

    }
}