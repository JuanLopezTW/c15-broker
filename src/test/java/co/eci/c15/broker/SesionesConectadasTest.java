package co.eci.c15.broker;

import co.eci.c15.common.events.SesionEvent;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SesionesConectadasTest {

    private final List<Object> publicados = new ArrayList<>();
    private final TaskScheduler scheduler = mock(TaskScheduler.class);
    private final SesionesConectadas sesiones = new SesionesConectadas(publicados::add, scheduler, Duration.ofSeconds(30));

    private static SessionDisconnectEvent desconexion(String sessionId) {
        return new SessionDisconnectEvent("test", MessageBuilder.withPayload(new byte[0]).build(), sessionId, CloseStatus.GOING_AWAY);
    }

    @Test
    void alRegistrarSeAvisaAbierta() {
        sesiones.registrar("ws-1", SesionEvent.SALA, "s1", "ana");

        assertEquals(List.of(new SesionEvent("ABIERTA", "SALA", "s1", "ana", "ws-1")), publicados);
    }

    @Test
    void alDesconectarseSeCierranTodosLosRegistrosDeEsaConexion() {
        sesiones.registrar("ws-1", SesionEvent.SALA, "s1", "ana");
        sesiones.registrar("ws-1", SesionEvent.PARTIDA, "s1", "ana");
        publicados.clear();

        sesiones.onDesconexion(desconexion("ws-1"));

        assertEquals(2, publicados.size());
        assertTrue(publicados.contains(new SesionEvent("CERRADA", "SALA", "s1", "ana", "ws-1")));
        assertTrue(publicados.contains(new SesionEvent("CERRADA", "PARTIDA", "s1", "ana", "ws-1")));
    }

    @Test
    void unaConexionQueNoSeRegistroNoAvisaNada() {
        sesiones.onDesconexion(desconexion("ws-sin-registrar"));

        assertTrue(publicados.isEmpty());
    }

    @Test
    void laRenovacionRepiteLasAbiertasYNoLasCerradas() {
        sesiones.registrar("ws-1", SesionEvent.SALA, "s1", "ana");
        sesiones.registrar("ws-2", SesionEvent.SALA, "s1", "beto");
        sesiones.onDesconexion(desconexion("ws-2"));
        publicados.clear();

        sesiones.renovar();

        assertEquals(List.of(new SesionEvent("ABIERTA", "SALA", "s1", "ana", "ws-1")), publicados);
    }

    @Test
    void alArrancarProgramaLaRenovacion() {
        sesiones.programarRenovacion();

        verify(scheduler).scheduleAtFixedRate(any(Runnable.class), org.mockito.ArgumentMatchers.eq(Duration.ofSeconds(30)));
    }
}
