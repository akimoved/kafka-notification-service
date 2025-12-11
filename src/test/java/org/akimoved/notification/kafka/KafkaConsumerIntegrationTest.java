package org.akimoved.notification.kafka;

import org.akimoved.notification.NotificationServiceApplication;
import org.akimoved.notification.dto.VerificationMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

/**
 * Интеграционный тест Kafka-консюмера с использованием Testcontainers.
 * Проверяет, что сообщение VerificationMessage корректно потребляется из топика.
 */
@SpringBootTest(classes = NotificationServiceApplication.class)
@Testcontainers
class KafkaConsumerIntegrationTest {

    private static final String TOPIC = "verification-codes";

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.1")
    );

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @SpyBean
    private KafkaConsumer kafkaConsumer;

    @Autowired
    private ApplicationContext context;

    /**
     * Создаёт KafkaTemplate для отправки сообщений VerificationMessage.
     */
    private KafkaTemplate<String, VerificationMessage> kafkaTemplate(String bootstrapServers) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Отключаем заголовки с типами, чтобы полезная нагрузка была чистым JSON
        configs.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        ProducerFactory<String, VerificationMessage> pf = new DefaultKafkaProducerFactory<>(configs);
        return new KafkaTemplate<>(pf);
    }

    /**
     * Проверяет, что консюмер получает сообщение из топика и метод обработчика вызывается.
     */
    @Test
    @DisplayName("KafkaConsumer: сообщение потребляется из топика verification-codes")
    void consumeMessage_fromVerificationCodesTopic() throws Exception {
        String bootstrap = kafka.getBootstrapServers();

        KafkaTemplate<String, VerificationMessage> template = kafkaTemplate(bootstrap);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<VerificationMessage> capturedMessage = new AtomicReference<>();
        AtomicReference<String> capturedTopic = new AtomicReference<>();

        // Перехватываем вызов обработчика, фиксируем факт и аргументы, затем вызываем реальную логику
        doAnswer(invocation -> {
            VerificationMessage msg = invocation.getArgument(0);
            String t = invocation.getArgument(1);
            capturedMessage.set(msg);
            capturedTopic.set(t);
            latch.countDown();
            return invocation.callRealMethod();
        }).when(kafkaConsumer).consumeVerificationCode(any(VerificationMessage.class), anyString());

        VerificationMessage message = new VerificationMessage("ivan@example.ru", "123456");
        template.send(TOPIC, message);
        template.flush();

        boolean consumed = latch.await(15, TimeUnit.SECONDS);
        assertThat(consumed).as("Сообщение должно быть потреблено консюмером").isTrue();
        assertThat(capturedTopic.get()).isEqualTo(TOPIC);
        assertThat(capturedMessage.get()).isNotNull();
        assertThat(capturedMessage.get().email()).isEqualTo("ivan@example.ru");
        assertThat(capturedMessage.get().code()).isEqualTo("123456");
    }
}
