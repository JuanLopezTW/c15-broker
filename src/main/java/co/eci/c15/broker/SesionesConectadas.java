package co.eci.c15.broker;

import co.eci.c15.common.events.EventosEntreServicios;
import co.eci.c15.common.events.SesionEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Las conexiones de esta replica que el front registro en un lobby (SALA) o en el mapa de una partida
 * (PARTIDA). Una misma conexion puede tener las dos: el lobby y luego la partida, sin recargar. Se avisan
 * a salas y gameplay por el topic c15.sesion: ABIERTA al registrar, repetida cada {@code renovacion} (asi la
 * presencia en salas no se vence mientras siga abierta) y CERRADA al desconectarse.
 */
@Component
public class SesionesConectadas {

    record Registro(String id, String userId) {
    }

    /** sessionId -> ambito (SALA/PARTIDA) -> donde esta registrada. */
    private final Map<String, Map<String, Registro>> sesiones = new ConcurrentHashMap<>();
    private final EventosEntreServicios eventos;
    private final TaskScheduler scheduler;
    private final Duration renovacion;

    public SesionesConectadas(EventosEntreServicios eventos, @Qualifier("sesionesScheduler") TaskScheduler scheduler,
                              @Value("${c15.sesiones.renovacion:30s}") Duration renovacion) {
        this.eventos = eventos;
        this.scheduler = scheduler;
        this.renovacion = renovacion;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void programarRenovacion() {
        scheduler.scheduleAtFixedRate(this::renovar, renovacion);
    }

    public void registrar(String sessionId, String ambito, String id, String userId) {
        sesiones.computeIfAbsent(sessionId, s -> new ConcurrentHashMap<>()).put(ambito, new Registro(id, userId));
        avisar(SesionEvent.ABIERTA, sessionId, ambito, new Registro(id, userId));
    }

    @EventListener
    public void onDesconexion(SessionDisconnectEvent desconexion) {
        Map<String, Registro> registros = sesiones.remove(desconexion.getSessionId());
        if (registros == null) return;
        registros.forEach((ambito, r) -> avisar(SesionEvent.CERRADA, desconexion.getSessionId(), ambito, r));
    }

    /** Vuelve a avisar las conexiones que siguen abiertas. */
    void renovar() {
        sesiones.forEach((sessionId, registros) ->
                registros.forEach((ambito, r) -> avisar(SesionEvent.ABIERTA, sessionId, ambito, r)));
    }

    private void avisar(String estado, String sessionId, String ambito, Registro r) {
        eventos.publicar(new SesionEvent(estado, ambito, r.id(), r.userId(), sessionId));
    }
}
