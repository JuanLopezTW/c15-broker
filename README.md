# c15-broker

**Broker de tiempo real** del EscapeRoom C15 (ARSW, equipo C15-PortalSuba). Es el único servicio con WebSocket:
todos los jugadores se conectan aquí (`/ws`, STOMP nativo, sin SockJS). Sale del monolito
[ScapeRoomC15](https://github.com/JuanLopezTW/ScapeRoomC15), que se queda desplegado como respaldo.

## Qué hace

- **Autentica** el `CONNECT` con el JWT de Auth0 (`Authorization: Bearer …` en los headers de STOMP).
- **Autoriza:**
  - Los clientes solo pueden mandar a `/app/...`.
  - Los topics de un equipo (`/topic/partidas/{m}/equipos/{e}/…` y `/topic/chat/partidas/{m}/equipos/{e}`) solo los escuchan sus miembros. Los equipos los aprende de `c15.partida-iniciada` y los olvida con `c15.partida-terminada`.
- **Relay a RabbitMQ:** los demás servicios publican sus topics en Rabbit y llegan aquí. Con varias réplicas del broker, cada una recibe todo.
- **Sesiones:**
  - Atiende `SEND /app/salas/{id}/sesion` (lobby) y `SEND /app/partidas/{m}/sesion` (mapa).
  - Publica en Kafka `c15.sesion`: `ABIERTA` al registrar, repetida cada 30 s, y `CERRADA` al desconectarse.
  - Salas lo usa para la presencia del lobby y gameplay para sacar al jugador del mapa.
- **Latidos:** el servidor manda latidos `{10000, 0}` y no se los exige al cliente (pestañas en segundo plano).
- **Varias réplicas:** con `ESTADO_REDIS=true`, los equipos de cada partida van en Redis y vencen con la partida.
- **Base de datos:** no tiene.

`co.eci.c15.plataforma` (seguridad, observabilidad, Kafka, STOMP) es una copia de lo que tienen todos los
servicios. Si se cambia aquí, hay que cambiarlo también en los demás.

## Correr

```bash
mvn verify                  # pruebas + cobertura (minimo 80%); las de Kafka y Redis necesitan Docker
mvn spring-boot:run         # necesita Kafka y RabbitMQ (y Redis si ESTADO_REDIS=true): ver .env.example
```

Para levantar todo el sistema junto, usa el compose de `c15-gateway`.
