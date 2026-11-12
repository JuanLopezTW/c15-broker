package co.eci.c15.broker;

import co.eci.c15.common.events.SesionEvent;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;

import java.security.Principal;

/**
 * El front avisa en que lobby o en que partida esta cada conexion (los mismos SEND que en el monolito). El
 * jugador sale del token del CONNECT, nunca del mensaje.
 */
@Controller
public class SesionStompController {

    private final SesionesConectadas sesiones;

    public SesionStompController(SesionesConectadas sesiones) {
        this.sesiones = sesiones;
    }

    /** SEND /app/salas/{salaId}/sesion: el lobby. */
    @MessageMapping("/salas/{salaId}/sesion")
    public void enSala(@DestinationVariable("salaId") String salaId, Principal yo,
                       @Header("simpSessionId") String sessionId) {
        sesiones.registrar(sessionId, SesionEvent.SALA, salaId, yo.getName());
    }

    /** SEND /app/partidas/{matchId}/sesion: el mapa de la partida. */
    @MessageMapping("/partidas/{matchId}/sesion")
    public void enPartida(@DestinationVariable("matchId") String matchId, Principal yo,
                          @Header("simpSessionId") String sessionId) {
        sesiones.registrar(sessionId, SesionEvent.PARTIDA, matchId, yo.getName());
    }
}
