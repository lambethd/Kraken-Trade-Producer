package lambethd.kraken.producer.interfaces;

import org.apache.kafka.clients.producer.Producer;

public interface IKafkaProducerCreator {
    Producer<Long, String> createProducer();
}
