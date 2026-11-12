package co.eci.c15.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Los equipos de cada partida en Redis (JSON), compartidos entre las replicas del broker. La clave caduca con
 * la partida (duracion + margen): si se pierde el fin, no queda nada colgado.
 */
@Component
// Nunca convive con el adaptador en memoria (lo decide c15.estado.redis); @Primary solo le aclara
// al analisis estatico cual usar, porque no evalua el interruptor.
@ConditionalOnProperty(name = "c15.estado.redis", havingValue = "true")
@Primary
public class EquiposPartidasRedis implements EquiposPartidas {

    static final String PREFIJO = "c15:broker:partida:";
    private static final TypeReference<Map<String, List<String>>> TIPO = new TypeReference<>() {
    };

    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    public EquiposPartidasRedis(StringRedisTemplate redis, ObjectMapper json) {
        this.redis = redis;
        this.json = json;
    }

    static String clave(String matchId) {
        return PREFIJO + matchId + ":equipos";
    }

    @Override
    public void guardar(String matchId, Map<String, List<String>> equipos, Duration vida) {
        try {
            redis.opsForValue().set(clave(matchId), json.writeValueAsString(equipos), vida);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Optional<Map<String, List<String>>> de(String matchId) {
        String valor = redis.opsForValue().get(clave(matchId));
        if (valor == null) return Optional.empty();
        try {
            return Optional.of(json.readValue(valor, TIPO));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void borrar(String matchId) {
        redis.delete(clave(matchId));
    }
}
