package co.eci.c15.broker;

import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConexionesWebSocketTest {

    private final ConexionesWebSocket conexiones = new ConexionesWebSocket(new ThreadPoolTaskScheduler(), Duration.ofSeconds(30));

    private static Message<byte[]> mensaje(String sessionId) {
        SimpMessageHeaderAccessor h = SimpMessageHeaderAccessor.create(SimpMessageType.CONNECT_ACK);
        h.setSessionId(sessionId);
        return MessageBuilder.createMessage(new byte[0], h.getMessageHeaders());
    }

    private static SessionDisconnectEvent desconexion(String sessionId) {
        return new SessionDisconnectEvent(new Object(), mensaje(sessionId), sessionId, CloseStatus.NORMAL);
    }

    @Test
    void cuentaLasConexionesAbiertasDeEstaReplica() {
        conexiones.onConexion(new SessionConnectedEvent(new Object(), mensaje("s1")));
        conexiones.onConexion(new SessionConnectedEvent(new Object(), mensaje("s2")));
        assertEquals(2, conexiones.abiertas());

        conexiones.onDesconexion(desconexion("s1"));
        assertEquals(1, conexiones.abiertas());
    }

    @Test
    void unaDesconexionAvisadaDosVecesNoDescuentaDoble() {
        conexiones.onConexion(new SessionConnectedEvent(new Object(), mensaje("s1")));
        conexiones.onConexion(new SessionConnectedEvent(new Object(), mensaje("s2")));

        conexiones.onDesconexion(desconexion("s1"));
        conexiones.onDesconexion(desconexion("s1"));
        conexiones.onDesconexion(desconexion("nunca-conectada"));

        assertEquals(1, conexiones.abiertas());
        assertDoesNotThrow(conexiones::reportar);
    }

    @Test
    void reportaCadaPeriodoAlArrancar() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.initialize();
        try {
            ConexionesWebSocket c = new ConexionesWebSocket(scheduler, Duration.ofMillis(50));
            assertDoesNotThrow(c::programarReporte);
        } finally {
            scheduler.shutdown();
        }
    }
}
