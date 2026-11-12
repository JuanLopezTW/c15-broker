package co.eci.c15.plataforma.config;

import co.eci.c15.broker.EquiposPartidasEnMemoria;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.security.Principal;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StompSeguridadInterceptorTest {

    private final JwtDecoder tokens = mock(JwtDecoder.class);
    private final EquiposPartidasEnMemoria equipos = new EquiposPartidasEnMemoria();
    private final MessageChannel canal = mock(MessageChannel.class);
    private StompSeguridadInterceptor interceptor;
    private static final String EQUIPO = "m1-e1";

    @BeforeEach
    void setUp() {
        interceptor = new StompSeguridadInterceptor(tokens, equipos, Duration.ofMillis(300), 2);
        // Lo que el broker aprende de partida-iniciada
        equipos.guardar("m1", Map.of(EQUIPO, List.of("auth0|ana", "auth0|beto"), "m1-e2", List.of("auth0|rival")),
                Duration.ofMinutes(25));
    }

    private static Message<byte[]> mensaje(StompHeaderAccessor stomp) {
        stomp.setLeaveMutable(true);
        return MessageBuilder.createMessage(new byte[0], stomp.getMessageHeaders());
    }

    private static Message<byte[]> conectar(String autorizacion) {
        StompHeaderAccessor stomp = StompHeaderAccessor.create(StompCommand.CONNECT);
        if (autorizacion != null) stomp.setNativeHeader("Authorization", autorizacion);
        return mensaje(stomp);
    }

    private static Message<byte[]> comando(StompCommand comando, String destino, String usuario) {
        StompHeaderAccessor stomp = StompHeaderAccessor.create(comando);
        stomp.setDestination(destino);
        if (usuario != null) stomp.setUser((Principal) () -> usuario);
        return mensaje(stomp);
    }

    private boolean pasa(Message<?> mensaje) {
        return interceptor.preSend(mensaje, canal) != null;
    }

    @Test
    void elConnectConTokenValidoIdentificaAlJugador() {
        when(tokens.decode("bueno")).thenReturn(Jwt.withTokenValue("bueno").header("alg", "RS256")
                .subject("auth0|ana").issuedAt(Instant.EPOCH).expiresAt(Instant.MAX).build());
        Message<byte[]> connect = conectar("Bearer bueno");

        interceptor.preSend(connect, canal);

        assertEquals("auth0|ana", StompHeaderAccessor.wrap(connect).getUser().getName());
    }

    @Test
    void elConnectSinTokenOConTokenInvalidoSeRechaza() {
        when(tokens.decode("malo")).thenThrow(new BadJwtException("firma invalida"));

        assertThrows(MessagingException.class, () -> interceptor.preSend(conectar(null), canal));
        assertThrows(MessagingException.class, () -> interceptor.preSend(conectar("otra-cosa"), canal));
        assertThrows(MessagingException.class, () -> interceptor.preSend(conectar("Bearer malo"), canal));
    }

    @Test
    void elClienteSoloPuedeEnviarAApp() {
        assertTrue(pasa(comando(StompCommand.SEND, "/app/salas/s1/sesion", "auth0|ana")));
        assertFalse(pasa(comando(StompCommand.SEND, "/topic/chat/global", "auth0|ana")));
        assertFalse(pasa(comando(StompCommand.SEND, "/topic/partidas/m1/reto", "auth0|ana")));
        assertFalse(pasa(comando(StompCommand.SEND, "/app/salas/s1/sesion", null)));
    }

    @Test
    void losTopicosDeUnEquipoSoloLosEscuchanSusMiembros() {
        String[] topicos = {
                "/topic/chat/partidas/m1/equipos/" + EQUIPO,
                "/topic/partidas/m1/equipos/" + EQUIPO + "/posiciones",
                "/topic/partidas/m1/equipos/" + EQUIPO + "/inventario",
                "/topic/partidas/m1/equipos/" + EQUIPO + "/roles",
        };
        for (String topico : topicos) {
            assertTrue(pasa(comando(StompCommand.SUBSCRIBE, topico, "auth0|ana")), topico);
            assertFalse(pasa(comando(StompCommand.SUBSCRIBE, topico, "auth0|rival")), topico);
        }
    }

    @Test
    void elEquipoDebeSerDeEsaPartida() {
        assertFalse(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/otra/equipos/" + EQUIPO + "/roles", "auth0|ana")));
        assertFalse(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/m1/equipos/no-existe/roles", "auth0|ana")));
    }

    @Test
    void siLosEquiposLleganUnPocoDespuesDeLaSuscripcionSeEsperaYPasa() throws Exception {
        Thread llegan = new Thread(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            equipos.guardar("m2", Map.of("m2-e1", List.of("auth0|ana")), Duration.ofMinutes(25));
        });
        llegan.start();

        assertTrue(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/m2/equipos/m2-e1/posiciones", "auth0|ana")));
        llegan.join();
    }

    @Test
    void unaPartidaQueNuncaLlegaSeRechazaAlVencerLaEspera() {
        long antes = System.nanoTime();

        assertFalse(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/fantasma/equipos/e1/roles", "auth0|ana")));
        assertTrue(System.nanoTime() - antes >= Duration.ofMillis(300).toNanos(), "espero antes de rechazar");
    }

    @Test
    void pasadoElTopeDeEsperasSeRechazaSinEsperar() throws Exception {
        // Dos sesiones esperan partidas inventadas y ocupan el tope (2); la tercera no espera
        List<Thread> ocupan = List.of(
                new Thread(() -> pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/f1/equipos/e1/roles", "auth0|x1"))),
                new Thread(() -> pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/f2/equipos/e1/roles", "auth0|x2"))));
        ocupan.forEach(Thread::start);
        Thread.sleep(100);

        long antes = System.nanoTime();
        assertFalse(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/f3/equipos/e1/roles", "auth0|x3")));
        assertTrue(System.nanoTime() - antes < Duration.ofMillis(200).toNanos(), "con el tope lleno no espera");
        for (Thread t : ocupan) t.join();

        // Al terminar las esperas el tope se libera
        Thread llegan = new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            equipos.guardar("m3", Map.of("m3-e1", List.of("auth0|ana")), Duration.ofMinutes(25));
        });
        llegan.start();
        assertTrue(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/m3/equipos/m3-e1/roles", "auth0|ana")));
        llegan.join();
    }

    @Test
    void quienNoEsDelEquipoSeRechazaSinEsperar() {
        long antes = System.nanoTime();

        assertFalse(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/m1/equipos/" + EQUIPO + "/roles", "auth0|rival")));
        assertTrue(System.nanoTime() - antes < Duration.ofMillis(300).toNanos(), "la partida ya se conoce: no hay que esperar");
    }

    @Test
    void alTerminarLaPartidaNadieEscuchaYaSusTopicosDeEquipo() {
        equipos.borrar("m1");

        assertFalse(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/m1/equipos/" + EQUIPO + "/roles", "auth0|ana")));
    }

    @Test
    void losTopicosGeneralesLosEscuchaCualquiera() {
        assertTrue(pasa(comando(StompCommand.SUBSCRIBE, "/topic/salas", "auth0|ana")));
        assertTrue(pasa(comando(StompCommand.SUBSCRIBE, "/topic/chat/global", "auth0|ana")));
        assertTrue(pasa(comando(StompCommand.SUBSCRIBE, "/topic/partidas/m1/reto", "auth0|rival")));
    }

    @Test
    void sinUsuarioNoSePuedeSuscribir() {
        assertFalse(pasa(comando(StompCommand.SUBSCRIBE, "/topic/salas", null)));
    }

    @Test
    void losDemasFramesPasan() {
        assertTrue(pasa(comando(StompCommand.UNSUBSCRIBE, null, "auth0|ana")));
        assertTrue(pasa(MessageBuilder.withPayload(new byte[0]).build()));
    }
}
