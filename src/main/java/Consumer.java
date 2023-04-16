import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    private static final Logger log = LogManager.getLogger(Consumer.class);
    static double eventsViolating = 0;
    static double eventsNonViolating = 0;
    static double totalEvents = 0;
    public static KafkaConsumer<String, Customer> consumer = null;
    static float maxConsumptionRatePerConsumer = 0.0f;
    static float ConsumptionRatePerConsumerInThisPoll = 0.0f;
    static float averageRatePerConsumerForGrpc = 0.0f;
    static long pollsSoFar = 0;

   static  ArrayList<TopicPartition> tps;
    static KafkaProducer<String, Customer> producer;

    public Consumer() throws IOException, URISyntaxException, InterruptedException {
    }





    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ModelNotFoundException, MalformedModelException {
        PrometheusUtils.initPrometheus();
        producer = Producer.producerFactory();
        KafkaConsumerConfig config = KafkaConsumerConfig.fromEnv();
        log.info(KafkaConsumerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaConsumerConfig.createProperties(config);
        // props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, BinPackPartitionAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, LagBasedPartitionAssignor.class.getName());
      /*  props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                org.apache.kafka.clients.consumer.RangeAssignor.class.getName());*/
        boolean commit = !Boolean.parseBoolean(config.getEnableAutoCommit());
        consumer = new KafkaConsumer<String, Customer>(props);
        consumer.subscribe(Collections.singletonList(config.getTopic())/*, new RebalanceListener()*/);
        log.info("Subscribed to topic {}", config.getTopic());



        addShutDownHook();


        tps = new ArrayList<>();
        tps.add(new TopicPartition("testtopic1", 0));
        tps.add(new TopicPartition("testtopic1", 1));
        tps.add(new TopicPartition("testtopic1", 2));
        tps.add(new TopicPartition("testtopic1", 3));
        tps.add(new TopicPartition("testtopic1", 4));

        try {
            while (true) {
                Long timeBeforePolling = System.currentTimeMillis();
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                if (records.count() != 0) {

                    for (TopicPartition tp : tps) {
                    /*  double percenttopic2 = records.records(tp).size()* 0.5;// *0.7;
                       double currentEventIndex = 0;*/
                        for (ConsumerRecord<String, Customer> record : records.records(tp)) {
                            totalEvents++;
                            if (System.currentTimeMillis() - record.timestamp() <= 5000) {
                                eventsNonViolating++;
                            } else {
                                eventsViolating++;
                            }
                            //TODO sleep per record or per batch

                                //Thread.sleep(Long.parseLong(config.getSleep()));
                                mldetect();
                                PrometheusUtils.latencygaugemeasure.setDuration(System.currentTimeMillis() - record.timestamp());
                                PrometheusUtils.timer.record(Duration.ofMillis(System.currentTimeMillis() - record.timestamp()));


                           /*  if (currentEventIndex < percenttopic2) {

                                  producer.send(new ProducerRecord<String, Customer>("testtopic2",
                                          tp.partition(), record.timestamp(), record.key(), record.value()));
                             }else {

                                 producer.send(new ProducerRecord<String, Customer>("testtopic3",
                                         tp.partition(), record.timestamp(), record.key(), record.value()));

                             }




                               currentEventIndex++;*/

                             /*   producer.send(new ProducerRecord<String, Customer>("testtopic4",
                                        tp.partition(), record.timestamp(), record.key(), record.value()));*/


                        }
                   }

/*                    if (commit) {*/
                        consumer.commitSync();
/*                    }*/
                    log.info("In this poll, received {} events", records.count());
                    Long timeAfterPollingProcessingAndCommit = System.currentTimeMillis();
                    ConsumptionRatePerConsumerInThisPoll = ((float) records.count() /
                            (float) (timeAfterPollingProcessingAndCommit - timeBeforePolling)) * 1000.0f;
                    pollsSoFar += 1;
                    averageRatePerConsumerForGrpc = averageRatePerConsumerForGrpc +
                            (ConsumptionRatePerConsumerInThisPoll - averageRatePerConsumerForGrpc) / (float) (pollsSoFar);

                    if (maxConsumptionRatePerConsumer < ConsumptionRatePerConsumerInThisPoll) {
                        maxConsumptionRatePerConsumer = ConsumptionRatePerConsumerInThisPoll;
                    }
                    log.info("ConsumptionRatePerConsumerInThisPoll in this poll {}", ConsumptionRatePerConsumerInThisPoll);
                    log.info("maxConsumptionRatePerConsumer {}", maxConsumptionRatePerConsumer);
                    double percentViolating = (double) eventsViolating / (double) totalEvents;
                    double percentNonViolating = (double) eventsNonViolating / (double) totalEvents;
                    log.info("Percent violating so far {}", percentViolating);
                    log.info("Percent non violating so far {}", percentNonViolating);
                    log.info("total events {}", totalEvents);
                }
            }

        } catch (WakeupException e) {
            // e.printStackTrace();
        } finally {
            consumer.close();
            log.info("Closed consumer and we are done");
        }
    }



    private static void mldetect() throws IOException, ModelNotFoundException, MalformedModelException {
        Criteria<URL, Classifications> criteria =
                Criteria.builder()
                        .setTypes(URL.class, Classifications.class)
                        .optProgress(new ProgressBar())
                        .optEngine("PyTorch")
                        .optModelUrls("djl://ai.djl.pytorch/resnet/0.0.1/traced_resnet18")
                        .build();


        URL url = new URL("https://resources.djl.ai/images/kitten.jpg");
        URL url2 = new URL("https://www.hartz.com/wp-content/uploads/2022/04/small-dog-owners-1.jpg");

        try (ZooModel<URL, Classifications> model = criteria.loadModel();
             Predictor<URL, Classifications> predictor = model.newPredictor()) {
            Classifications classifications = predictor.predict(url);
            Classifications classifications2 = predictor.predict(url2);

            System.out.println(classifications);
            System.out.println(classifications2);

        } catch (TranslateException e) {
            e.printStackTrace();
        }
    }



    private static void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Starting exit...");
                consumer.wakeup();
                try {
                    this.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}