package io.tpd.kafkaexample;

import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloKafkaController {

    private static final Logger logger =
        LoggerFactory.getLogger(HelloKafkaController.class);

    private final KafkaProducer template;
    private final String topicName;
    private final int messagesPerRequest;
    private CountDownLatch latch;

    public HelloKafkaController(
        final KafkaProducer template,
        @Value("${test.topic}") final String topicName,
        @Value("${tpd.messages-per-request}") final int messagesPerRequest) {
        this.template = template;
        this.topicName = topicName;
        this.messagesPerRequest = messagesPerRequest;
    }

    @GetMapping("/hello")
    public String hello() throws Exception {
        latch = new CountDownLatch(messagesPerRequest);
        template.send(topicName, "misha butr");
        logger.info("All messages received");
        return "Hello Kafka!";
    }
}
