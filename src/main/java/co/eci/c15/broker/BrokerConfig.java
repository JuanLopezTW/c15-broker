package co.eci.c15.broker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class BrokerConfig {

    /** Repite el aviso de las conexiones abiertas (ver {@link SesionesConectadas}). */
    @Bean
    public ThreadPoolTaskScheduler sesionesScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("sesiones-");
        return scheduler;
    }
}
