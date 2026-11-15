package co.eci.c15.plataforma.config;

import co.eci.c15.broker.EquiposPartidas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Seguridad del WebSocket: el CONNECT debe traer un token valido, los clientes solo pueden mandar a
 * /app (si no, podrian publicar directo en un /topic y falsificar mensajes) y los topics de un equipo
 * solo los escuchan sus miembros (los equipos los aprende el broker de partida-iniciada). Lo denegado se
 * descarta sin tumbar la conexion.
 *
 * El permiso de un topic de equipo se da solo con la partida ya iniciada: ahi los equipos quedaron fijos (en
 * el lobby se pueden cambiar, y una suscripcion aceptada no se revoca despues). El front se suscribe apenas
 * entra a la partida, y el evento con los equipos puede llegarle al broker un poco despues (va por Kafka).
 * Por eso, si la partida aun no es conocida, se espera hasta {@code c15.seguridad.espera-equipos} a que
 * lleguen; si el jugador no es del equipo se rechaza de una.
 *
 * La espera ocupa un hilo del canal de entrada. Para que nadie pueda congelar el broker suscribiendose a
 * partidas inventadas, hay un tope de esperas a la vez ({@code c15.seguridad.esperas-maximas}): pasado el
 * tope se rechaza sin esperar. Los frames de una misma sesion se atienden en orden, asi que cada jugador
 * tiene como mucho una espera a la vez y solo se demora a si mismo.
 */
@Component
public class StompSeguridadInterceptor implements ChannelInterceptor {

    private static final Logger log = LoggerFactory.getLogger(StompSeguridadInterceptor.class);
    private static final Pattern TOPICO_DE_EQUIPO =
            Pattern.compile("^/topic/(?:chat/)?partidas/([^/]+)/equipos/([^/]+)(?:/.*)?$");
    private static final String PREFIJO_TOKEN = "Bearer ";
    private static final Duration CADA = Duration.ofMillis(100);

    private final JwtDecoder tokens;
    private final EquiposPartidas equipos;
    private final Duration espera;
    private final Semaphore esperas;

    public StompSeguridadInterceptor(JwtDecoder tokens, EquiposPartidas equipos,
                                     @Value("${c15.seguridad.espera-equipos:2s}") Duration espera,
                                     @Value("${c15.seguridad.esperas-maximas:50}") int esperasMaximas) {
        this.tokens = tokens;
        this.equipos = equipos;
        this.espera = espera;
        this.esperas = new Semaphore(esperasMaximas);
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor stomp = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (stomp == null || stomp.getCommand() == null) return message;

        StompCommand comando = stomp.getCommand();
        if (comando == StompCommand.CONNECT) {
            JwtAuthenticationToken usuario = new JwtAuthenticationToken(validarToken(stomp));
            stomp.setUser(usuario);
            log.atInfo().addKeyValue("evento", "stomp.conectado").addKeyValue("userId", usuario.getName())
                    .log("STOMP conectado: {}", usuario.getName());
            return message;
        }
        if (comando == StompCommand.SEND && !permitidoEnviar(stomp)) return denegar(stomp);
        if (comando == StompCommand.SUBSCRIBE && !permitidoSuscribir(stomp)) return denegar(stomp);
        return message;
    }

    /** Un CONNECT rechazado queda registrado (escenario S1) con el motivo, nunca con el token. */
    private Jwt validarToken(StompHeaderAccessor stomp) {
        String autorizacion = stomp.getFirstNativeHeader("Authorization");
        if (autorizacion == null || !autorizacion.startsWith(PREFIJO_TOKEN)) {
            throw rechazar("Falta el token", null);
        }
        try {
            return tokens.decode(autorizacion.substring(PREFIJO_TOKEN.length()));
        } catch (JwtException e) {
            throw rechazar("Token invalido", e);
        }
    }

    private static MessagingException rechazar(String motivo, JwtException causa) {
        log.atWarn().addKeyValue("evento", "stomp.rechazado").addKeyValue("motivo", motivo)
                .log("CONNECT rechazado: {}", motivo);
        return causa == null ? new MessagingException(motivo) : new MessagingException(motivo, causa);
    }

    private static boolean permitidoEnviar(StompHeaderAccessor stomp) {
        String destino = stomp.getDestination();
        return stomp.getUser() != null && destino != null && destino.startsWith("/app/");
    }

    private boolean permitidoSuscribir(StompHeaderAccessor stomp) {
        if (stomp.getUser() == null || stomp.getDestination() == null) return false;
        Matcher equipo = TOPICO_DE_EQUIPO.matcher(stomp.getDestination());
        if (!equipo.matches()) return true;
        String userId = stomp.getUser().getName();
        esperarEquipos(equipo.group(1));
        return equipos.esMiembro(equipo.group(1), equipo.group(2), userId);
    }

    /** Si la partida aun no es conocida, espera un poco a que llegue partida-iniciada (ver la clase). */
    private void esperarEquipos(String matchId) {
        if (equipos.de(matchId).isPresent()) return;
        if (!esperas.tryAcquire()) {
            log.warn("Demasiadas suscripciones esperando equipos: se rechaza sin esperar ({})", matchId);
            return;
        }
        try {
            long limite = System.nanoTime() + espera.toNanos();
            while (equipos.de(matchId).isEmpty() && System.nanoTime() < limite) {
                Thread.sleep(CADA.toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            esperas.release();
        }
    }

    /** Una suscripcion o un envio no permitido se descarta y queda registrado (escenario S2). */
    private static Message<?> denegar(StompHeaderAccessor stomp) {
        String usuario = stomp.getUser() != null ? stomp.getUser().getName() : "sin usuario";
        log.atWarn().addKeyValue("evento", "stomp.denegado").addKeyValue("comando", stomp.getCommand())
                .log("{} denegado a {} para {}", stomp.getCommand(), stomp.getDestination(), usuario);
        return null;
    }
}
