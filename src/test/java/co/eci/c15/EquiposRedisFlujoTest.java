package co.eci.c15;

import co.eci.c15.broker.EquiposPartidas;
import co.eci.c15.broker.EquiposPartidasRedis;
import co.eci.c15.common.events.MatchFinishedEvent;
import co.eci.c15.common.events.PartidaIniciadaEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Con varias replicas del broker los equipos van en Redis y caducan con la partida (duracion + margen). */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class EquiposRedisFlujoTest {

    @DynamicPropertySource
    static void redis(DynamicPropertyRegistry propiedades) {
        RedisDePrueba.propiedades(propiedades);
    }

    @Autowired
    private EquiposPartidas equipos;

    @Autowired
    private ApplicationEventPublisher eventos;

    @Autowired
    private StringRedisTemplate redis;

    @Test
    void seAprendenDelInicioCaducanConLaPartidaYSeOlvidanAlTerminar() {
        assertInstanceOf(EquiposPartidasRedis.class, equipos);

        eventos.publishEvent(new PartidaIniciadaEvent("r-m1", Map.of("e1", List.of("auth0|ana")), Duration.ofMinutes(10)));

        assertTrue(equipos.esMiembro("r-m1", "e1", "auth0|ana"));
        assertFalse(equipos.esMiembro("r-m1", "e1", "auth0|rival"));
        assertFalse(equipos.esMiembro("r-m1", "e2", "auth0|ana"));
        Long segundos = redis.getExpire("c15:broker:partida:r-m1:equipos", TimeUnit.SECONDS);
        assertTrue(segundos > 19 * 60 && segundos <= 20 * 60, "10 min de partida + 10 de margen: " + segundos);

        eventos.publishEvent(new MatchFinishedEvent("r-m1", "TIME_UP", null, Map.of(), Map.of()));

        assertTrue(equipos.de("r-m1").isEmpty());
    }
}
