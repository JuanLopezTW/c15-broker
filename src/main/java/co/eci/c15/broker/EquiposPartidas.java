package co.eci.c15.broker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Quien es de que equipo en cada partida en curso, para autorizar los topics de equipo. Se aprende de
 * {@code partida-iniciada} y se olvida con {@code partida-terminada} (o al vencer: nunca vive mas que la partida
 * mas el margen). Con varias replicas del broker va en Redis: el evento lo recibe una sola.
 */
public interface EquiposPartidas {

    void guardar(String matchId, Map<String, List<String>> equipos, Duration vida);

    Optional<Map<String, List<String>>> de(String matchId);

    void borrar(String matchId);

    /** Si el jugador es miembro de ese equipo en esa partida. */
    default boolean esMiembro(String matchId, String equipoId, String userId) {
        return de(matchId).map(e -> e.getOrDefault(equipoId, List.of()).contains(userId)).orElse(false);
    }
}
