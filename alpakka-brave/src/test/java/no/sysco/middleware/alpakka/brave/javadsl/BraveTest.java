package no.sysco.middleware.alpakka.brave.javadsl;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import brave.Tracing;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import zipkin2.Span;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


public class BraveTest {

  private static ActorSystem system;
  private static ActorMaterializer materializer;

  private BlockingQueue<Span> spans = new LinkedBlockingQueue<>();
  private Tracing tracing = Tracing.newBuilder()
      .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
          .addScopeDecorator(StrictScopeDecorator.create())
          .build())
      .spanReporter(spans::add)
      .build();

  @BeforeClass
  public static void setUp() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void tearDown() {
    if (system != null) {
      TestKit.shutdownActorSystem(system);
    }
  }

  @Test
  public void shouldCreateTracePair() throws InterruptedException {
    new TestKit(system) {{
      Source.single("hello")
          .via(Brave.startSpanFlow(tracing, "my-span"))
          .map(Pair::second)
          .map(param -> {
            tracing.tracer().toSpan(param).finish();
            return param;
          })
          .to(Sink.foreach(TestCase::assertNotNull))
          .run(materializer);

      Span span = takeSpan();
      assertThat(span).isNotNull();
    }};
  }

  /**
   * Call this to block until a span was reported
   */
  private Span takeSpan() throws InterruptedException {
    Span result = spans.poll(3, TimeUnit.SECONDS);
    assertThat(result)
        .withFailMessage("Producer span was not reported")
        .isNotNull();
    // ensure the span finished
    assertThat(result.durationAsLong()).isPositive();
    return result;
  }
}
