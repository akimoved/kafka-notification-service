package org.akimoved.notification.kafka;

import org.akimoved.notification.dto.VerificationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @KafkaListener(
            topics = "verification-codes",
            groupId = "notification-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeVerificationCode(
            @Payload VerificationMessage message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        String timestamp = LocalDateTime.now().format(formatter);

        log.info("\n" + "=".repeat(60));
        log.info("НОВЫЙ КОД ПОДТВЕРЖДЕНИЯ");
        log.info("=".repeat(60));
        log.info("Время:  {}", timestamp);
        log.info("Email:  {}", message.email());
        log.info("Код:    {}", message.code());
        log.info("Topic:  {}", topic);
        log.info("=".repeat(60) + "\n");
    }
}
