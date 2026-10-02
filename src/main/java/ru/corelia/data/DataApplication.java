package ru.corelia.data;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import ru.corelia.config.LocalEnvironment;

/** Запускает сервис, владеющий нативным хранением документов Corelia. */
@SpringBootApplication(scanBasePackages = {"ru.corelia.config", "ru.corelia.support", "ru.corelia.auth", "ru.corelia.http", "ru.corelia.cache", "ru.corelia.transport", "ru.corelia.observability", "ru.corelia.data"})
public class DataApplication {
    public static void main(String[] args) {
        var app = new SpringApplication(DataApplication.class);
        app.setDefaultProperties(LocalEnvironment.load());
        app.run(args);
    }
}
