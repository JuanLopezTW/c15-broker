package co.eci.c15.broker;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Con una sola replica del broker basta guardarlos en memoria (se borran al terminar la partida). */
@Component
@ConditionalOnProperty(name = "c15.estado.redis", havingValue = "false", matchIfMissing = true)
public class EquiposPartidasEnMemoria implements EquiposPartidas {

    private final Map<String, Map<String, List<String>>> partidas = new ConcurrentHashMap<>();

    @Override
    public void guardar(String matchId, Map<String, List<String>> equipos, Duration vida) {
        partidas.put(matchId, Map.copyOf(equipos));
    }

    @Override
    public Optional<Map<String, List<String>>> de(String matchId) {
        return Optional.ofNullable(partidas.get(matchId));
    }

    @Override
    public void borrar(String matchId) {
        partidas.remove(matchId);
    }
}
