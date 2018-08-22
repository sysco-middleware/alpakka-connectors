package no.sysco.middleware.alpakka.brave.javadsl.examples;

public class HelloWorld {
  //public static void main(String[] args) {
  //  public static void main(String[] args) {
  //  final ActorSystem system = ActorSystem.create();
  //  final ActorMaterializer materializer = ActorMaterializer.create(system);
  //
  //  final Sender sender = URLConnectionSender.create("http://zipkin-zipkin.paas-test.statnett.no/api/v2/spans");
  //  final Reporter<zipkin2.Span> reporter = AsyncReporter.create(sender);
  //  final Tracing tracing =
  //      Tracing.newBuilder()
  //          .localServiceName("test-akka-streams")
  //          .sampler(Sampler.ALWAYS_SAMPLE)
  //          .spanReporter(reporter)
  //          .build();
  //
  //  final Flow<String, String, NotUsed> concatFlow =
  //      Flow.<String>create().map(a -> a + "b");
  //
  //  Source.repeat("a")
  //      .take(10)
  //      .via(Brave.startSpanFlow(tracing, "main"))
  //      .async()
  //      .via(Brave.spanFlow(tracing, "concat0", concatFlow))
  //      .async()
  //      .via(Brave.spanFlow(tracing, "concat1", concatFlow))
  //      .async()
  //      .via(Brave.spanFlow(tracing, "concat2", concatFlow))
  //      .async()
  //      .via(Brave.finishSpanFlow(tracing))
  //      .runWith(Sink.foreach(out::println), materializer);
  //}
  //}
}
