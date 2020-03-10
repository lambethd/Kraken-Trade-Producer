package lambethd.kraken.producer.service;

import lambethd.kraken.producer.interfaces.IKafkaProducerCreator;
import lambethd.kraken.producer.kafka.IKafkaConstants;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@Service
public class TradeProducer {
    @Autowired
    private IKafkaProducerCreator kafkaProducerCreator;

    @PostConstruct
    public void begin() {
        System.out.println("Beginning");
        int id = 0;
        Producer<Long, String> producer = kafkaProducerCreator.createProducer();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            try {
                ProducerRecord<Long, String> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, getStringValue(id));
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
                id++;
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e.getMessage());
            }
        }
    }

    private String getStringValue(int id) {
        return id + "," + "Stock" + "," + getStockId() + "," + getBuyPrice();
    }

    private double getBuyPrice() {
        Random rnd = new Random();
        return rnd.nextDouble();
    }

    private String getStockId() {
        Random rnd = new Random();
        int id = rnd.nextInt(4);
        switch (id) {
            case 0:
                return "MSFT";
            case 1:
                return "AAPL";
            case 2:
                return "GOOGL";
            case 3:
                return "WOW";
            default:
                return "AXP";
        }
    }

    private String getTradeType() {
        return "Bond";
    }
}
