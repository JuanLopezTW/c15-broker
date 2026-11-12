package co.eci.c15;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Broker (tiempo real) del EscapeRoom C15: el unico con WebSocket (/ws). Autentica el CONNECT, autoriza los
 * topics de equipo, hace de relay a RabbitMQ y avisa las sesiones (topic c15.sesion) a salas y gameplay.
 * Sale del monolito (repo ScapeRoomC15); lo comun a todos los servicios esta en {@code plataforma}.
 */
@SpringBootApplication
public class BrokerApplication {

    public static void main(String[] args) {
        SpringApplication.run(BrokerApplication.class, args);
    }
}
