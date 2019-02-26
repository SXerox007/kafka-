package kafka_java_intro.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

// Create thread with runnable
public class ConsumerThread implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;
    private Logger logger;

    // init
    public ConsumerThread(final CountDownLatch latch, final KafkaConsumer<String,String> consumer){
         this.logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        this.latch = latch;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("\nKey: " + record.key() + " Value: " + record.value());
                    logger.info("\nPartition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        }catch (WakeupException e){
            logger.info("Kafka Consumer Exited");
        } finally {
            consumer.close();
            // tell main method consumer is over
            latch.countDown();
        }
    }

    //shutdown the consumer
    public void shutDown(){
        // it will interrupt and make the exception
        consumer.wakeup();
    }
}
