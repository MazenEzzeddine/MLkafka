import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {
    private static final Logger log = LogManager.getLogger(RebalanceListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        try {
            log.info("Sleeping on rebalancing");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
