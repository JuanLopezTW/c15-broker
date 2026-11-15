package co.eci.c15.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionConnectedEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cuantas conexiones de tiempo real (STOMP sobre WebSocket) tiene esta replica. Cada {@code periodo} deja una
 * linea evento=ws.conexiones que el tablero tecnico grafica por replica (escenario O2: conexiones por replica).
 * Se cuentan ids de sesion, no sumas: una desconexion avisada dos veces no descuenta doble.
 */
@Component
public class ConexionesWebSocket {

    private static final Logger log = LoggerFactory.getLogger(ConexionesWebSocket.class);

    private final Set<String> abiertas = ConcurrentHashMap.newKeySet();
    private final TaskScheduler scheduler;
    private final Duration periodo;

    public ConexionesWebSocket(@Qualifier("sesionesScheduler") TaskScheduler scheduler,
                               @Value("${c15.conexiones.periodo:30s}") Duration periodo) {
        this.scheduler = scheduler;
        this.periodo = periodo;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void programarReporte() {
        scheduler.scheduleAtFixedRate(this::reportar, periodo);
    }

    @EventListener
    public void onConexion(SessionConnectedEvent conexion) {
        String id = idDe(conexion.getMessage().getHeaders().get("simpSessionId"));
        if (id != null) abiertas.add(id);
    }

    @EventListener
    public void onDesconexion(SessionDisconnectEvent desconexion) {
        abiertas.remove(desconexion.getSessionId());
    }

    public int abiertas() {
        return abiertas.size();
    }

    void reportar() {
        log.atInfo().addKeyValue("evento", "ws.conexiones").addKeyValue("ws.conexiones", abiertas.size())
                .log("Conexiones de tiempo real en esta replica: {}", abiertas.size());
    }

    private static String idDe(Object valor) {
        return valor instanceof String s ? s : null;
    }
}
