package co.eci.c15;

import co.eci.c15.common.events.MatchFinishedEvent;
import co.eci.c15.common.events.PartidaIniciadaEvent;
import co.eci.c15.common.events.SesionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Un cliente STOMP de verdad contra /ws: el token del CONNECT dice quien es, el lobby y el mapa se avisan como
 * sesiones (topic c15.sesion) y los topics de un equipo solo le llegan a sus miembros.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebSocketFlujoTest {

    @LocalServerPort
    private int puerto;

    /** "token-<usuario>" vale por el jugador auth0|<usuario>. */
    @MockitoBean
    private JwtDecoder tokens;

    @Autowired
    private SesionesPublicadas sesiones;

    @Autowired
    private ApplicationEventPublisher eventos;

    @Autowired
    private SimpMessagingTemplate topics;

    private final WebSocketStompClient cliente = new WebSocketStompClient(new StandardWebSocketClient());

    @BeforeEach
    void setUp() {
        cliente.setMessageConverter(new StringMessageConverter());
        when(tokens.decode(anyString())).thenAnswer(i -> {
            String token = i.getArgument(0);
            return Jwt.withTokenValue(token).header("alg", "RS256").subject("auth0|" + token.substring("token-".length()))
                    .issuedAt(Instant.EPOCH).expiresAt(Instant.parse("2999-01-01T00:00:00Z")).build();
        });
    }

    private StompSession conectar(String usuario) throws Exception {
        StompHeaders connect = new StompHeaders();
        connect.add("Authorization", "Bearer token-" + usuario);
        return cliente.connectAsync("ws://localhost:" + puerto + "/ws", new WebSocketHttpHeaders(), connect,
                new StompSessionHandlerAdapter() {
                }).get(10, TimeUnit.SECONDS);
    }

    private static BlockingQueue<String> escuchar(StompSession sesion, String topic) {
        BlockingQueue<String> llegados = new LinkedBlockingQueue<>();
        sesion.subscribe(topic, new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return String.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                llegados.add((String) payload);
            }
        });
        return llegados;
    }

    @Test
    void elLobbyYElMapaSeAvisanComoSesionesYAlDesconectarseSeCierranLasDos() throws Exception {
        StompSession ana = conectar("ana");

        ana.send("/app/salas/s1/sesion", "");
        await().atMost(Duration.ofSeconds(10)).until(() -> sesiones.hay(SesionEvent.ABIERTA, SesionEvent.SALA, "s1", "auth0|ana"));
        ana.send("/app/partidas/s1/sesion", "");
        await().atMost(Duration.ofSeconds(10)).until(() -> sesiones.hay(SesionEvent.ABIERTA, SesionEvent.PARTIDA, "s1", "auth0|ana"));

        ana.disconnect();

        await().atMost(Duration.ofSeconds(10)).until(() -> sesiones.hay(SesionEvent.CERRADA, SesionEvent.SALA, "s1", "auth0|ana")
                && sesiones.hay(SesionEvent.CERRADA, SesionEvent.PARTIDA, "s1", "auth0|ana"));
    }

    @Test
    void losTopicsDeUnEquipoSoloLesLleganASusMiembrosMientrasDureLaPartida() throws Exception {
        eventos.publishEvent(new PartidaIniciadaEvent("m-ws", Map.of("e1", List.of("auth0|beto"), "e2", List.of("auth0|caro"))));
        StompSession beto = conectar("beto");
        StompSession caro = conectar("caro");
        BlockingQueue<String> deBeto = escuchar(beto, "/topic/partidas/m-ws/equipos/e1/roles");
        BlockingQueue<String> deCaro = escuchar(caro, "/topic/partidas/m-ws/equipos/e1/roles");
        BlockingQueue<String> generalCaro = escuchar(caro, "/topic/partidas/m-ws/cronometro");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            topics.convertAndSend("/topic/partidas/m-ws/equipos/e1/roles", "secreto");
            assertEquals("secreto", deBeto.poll(200, TimeUnit.MILLISECONDS));
        });
        topics.convertAndSend("/topic/partidas/m-ws/cronometro", "59");
        assertEquals("59", generalCaro.poll(5, TimeUnit.SECONDS), "los topics generales le llegan a cualquiera");
        assertNull(deCaro.poll(500, TimeUnit.MILLISECONDS), "caro no es del equipo e1");

        eventos.publishEvent(new MatchFinishedEvent("m-ws", "TIME_UP", null, Map.of(), Map.of()));
        BlockingQueue<String> tarde = escuchar(beto, "/topic/partidas/m-ws/equipos/e1/inventario");
        // Pausa para que el broker procese (y rechace) la suscripcion antes de publicar
        await().pollDelay(Duration.ofMillis(300)).until(() -> true);
        topics.convertAndSend("/topic/partidas/m-ws/equipos/e1/inventario", "ya no");
        assertNull(tarde.poll(500, TimeUnit.MILLISECONDS), "terminada la partida, nadie se suscribe a su equipo");

        beto.disconnect();
        caro.disconnect();
    }
}
