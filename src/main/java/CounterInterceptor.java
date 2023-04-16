import io.micrometer.core.instrument.Gauge;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String,Customer> {

    Map<String, Gauge> topicToGauge = new HashMap<>();
    Map<String, CountMeasure> topicToMeasure = new HashMap<>();


    @Override
    public ProducerRecord<String, Customer> onSend(ProducerRecord<String, Customer> producerRecord) {
        String topicto = producerRecord.topic();
        Gauge gauge=  topicToGauge.get(topicto);
        if(gauge == null) {
            CountMeasure measure = new CountMeasure(0.0);
            //Gauge gauge1 = Gauge.builder(topicto,  measure, CountMeasure::getCount).tag("topic to", topicto).register(PrometheusUtils.prometheusRegistry);
            Gauge gauge1 = Gauge.builder(CountConsumerInterceptor.inputtopic+topicto,  measure, CountMeasure::getCount)
                    //.tag("topicFrom", CountConsumerInterceptor.inputtopic + "i")
                    .register(PrometheusUtils.prometheusRegistry);

            topicToGauge.put(topicto, gauge1);
            topicToMeasure.put(topicto, measure);

        }else {
            topicToMeasure.get(topicto).setCount( topicToMeasure.get(topicto).getCount() + 1.0);
        }
        return producerRecord;

    }
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}