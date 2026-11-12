package co.eci.c15.broker;

import co.eci.c15.common.events.MatchFinishedEvent;
import co.eci.c15.common.events.PartidaIniciadaEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Aprende los equipos al iniciar la partida (lo publica salas) y los olvida al terminar (lo publica gameplay). */
@Component
public class EquiposPartidasListener {

    private final EquiposPartidas equipos;
    private final Duration margen;

    public EquiposPartidasListener(EquiposPartidas equipos, @Value("${c15.estado.margen:10m}") Duration margen) {
        this.equipos = equipos;
        this.margen = margen;
    }

    @EventListener
    public void onPartidaIniciada(PartidaIniciadaEvent evento) {
        equipos.guardar(evento.matchId(), evento.equipos(), evento.duracion().plus(margen));
    }

    @EventListener
    public void onPartidaTerminada(MatchFinishedEvent evento) {
        equipos.borrar(evento.matchId());
    }
}
