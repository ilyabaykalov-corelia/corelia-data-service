package ru.corelia.data;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.corelia.auth.AuthIdentityProvider;
import ru.corelia.auth.AuthKeyProvider;
import ru.corelia.config.CoreliaAuthConfig;

/** Подключает provider-neutral параметры проверки JWT для внутреннего data API. */
@Configuration
class DataAuthConfiguration {
    @Bean
    AuthIdentityProvider dataAuthIdentityProvider(CoreliaAuthConfig auth) {
        return new AuthIdentityProvider() {
            @Override public String issuer() { return auth.issuer(); }
            @Override public java.util.Set<String> audiences() { return auth.audiences(); }
        };
    }

    @Bean
    AuthKeyProvider dataAuthKeyProvider(CoreliaAuthConfig auth) {
        return auth::jwksUrl;
    }
}
