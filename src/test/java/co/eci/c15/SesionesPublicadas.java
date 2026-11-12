package co.eci.c15;

import co.eci.c15.common.events.SesionEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** En las pruebas, lo que recibirian salas y gameplay: los avisos de sesion que publico el broker. */
@Component
class SesionesPublicadas {

    private final List<SesionEvent> recibidas = new CopyOnWriteArrayList<>();

    @EventListener
    void onSesion(SesionEvent sesion) {
        recibidas.add(sesion);
    }

    boolean hay(String estado, String ambito, String id, String userId) {
        return recibidas.stream().anyMatch(s -> s.estado().equals(estado) && s.ambito().equals(ambito)
                && s.id().equals(id) && s.userId().equals(userId));
    }
}
