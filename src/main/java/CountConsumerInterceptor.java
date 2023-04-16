import io.micrometer.core.instrument.Gauge;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class CountConsumerInterceptor implements ConsumerInterceptor<String, Customer> {

   public static String inputtopic;
    CountMeasure measure;
    public static Gauge gauge1;

    static {
        inputtopic = System.getenv("TOPIC");
    }

    public CountConsumerInterceptor() {
        measure = new CountMeasure(0.0);
        gauge1 = Gauge.builder(inputtopic + "Total", measure, CountMeasure::getCount)
               // .tag("topicTo", "NA")
                .register(PrometheusUtils.prometheusRegistry);
    }

    @Override
    public ConsumerRecords<String, Customer> onConsume(ConsumerRecords<String, Customer> consumerRecords) {
        measure.setCount(measure.getCount() + consumerRecords.count());
        return consumerRecords;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
    }


    @Override
    public void close() {

    }


    @Override
    public void configure(Map<String, ?> map) {

    }
}