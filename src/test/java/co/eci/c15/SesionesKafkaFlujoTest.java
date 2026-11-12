package co.eci.c15;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/** Lo que el broker deja en Kafka (topic sesion) cuando un jugador abre y cierra el lobby: lo leen salas y gameplay. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class SesionesKafkaFlujoTest {

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry propiedades) {
        KafkaDePrueba.propiedades(propiedades, "sesionesKafka");
    }

    @LocalServerPort
    private int puerto;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafka;

    @MockitoBean
    private JwtDecoder tokens;

    @Test
    void abrirYCerrarElLobbyDejaLosDosAvisosConLaSalaComoLlave() throws Exception {
        when(tokens.decode(anyString())).thenReturn(Jwt.withTokenValue("t").header("alg", "RS256").subject("auth0|ana")
                .issuedAt(Instant.EPOCH).expiresAt(Instant.parse("2999-01-01T00:00:00Z")).build());
        WebSocketStompClient cliente = new WebSocketStompClient(new StandardWebSocketClient());
        cliente.setMessageConverter(new StringMessageConverter());
        StompHeaders connect = new StompHeaders();
        connect.add("Authorization", "Bearer t");
        StompSession ana = cliente.connectAsync("ws://localhost:" + puerto + "/ws", new WebSocketHttpHeaders(), connect,
                new StompSessionHandlerAdapter() {
                }).get(10, TimeUnit.SECONDS);

        ana.send("/app/salas/k-s1/sesion", "");
        // Pausa para que el broker registre la sesion antes de cortar (no hay nada que observar desde aqui)
        await().pollDelay(Duration.ofMillis(500)).until(() -> true);
        ana.disconnect();

        List<ConsumerRecord<String, String>> avisos = new ArrayList<>();
        try (KafkaConsumer<String, String> lector = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka,
                ConsumerConfig.GROUP_ID_CONFIG, "prueba-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"), new StringDeserializer(), new StringDeserializer())) {
            lector.subscribe(List.of("c15.sesionesKafka.sesion"));
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                lector.poll(Duration.ofMillis(500)).forEach(avisos::add);
                return avisos.size() >= 2;
            });
        }

        assertTrue(avisos.stream().allMatch(r -> r.key().equals("k-s1")), "la llave es la sala");
        String abierta = avisos.get(0).value();
        String cerrada = avisos.get(avisos.size() - 1).value();
        assertTrue(abierta.startsWith("{\"estado\":\"ABIERTA\",\"ambito\":\"SALA\",\"id\":\"k-s1\",\"userId\":\"auth0|ana\""), abierta);
        assertTrue(cerrada.startsWith("{\"estado\":\"CERRADA\",\"ambito\":\"SALA\",\"id\":\"k-s1\",\"userId\":\"auth0|ana\""), cerrada);
        assertEquals(abierta.substring(abierta.indexOf("\"sessionId\"")), cerrada.substring(cerrada.indexOf("\"sessionId\"")),
                "es la misma conexion");
    }
}
