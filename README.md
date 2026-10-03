# corelia-data-service

Внутренний владелец документных данных Corelia. Сервис хранит актуальные
снимки, версии, document history, attachment metadata, поисковые индексы и
idempotency receipts в PostgreSQL базе `corelia_data`. Он не публикуется
наружу и не хранит бинарное содержимое файлов.

`corelia-provider-native-data` вызывает его `/internal/v1/data/documents`
контракты от имени gateway, document- и attachment-service. Прямой SQL из
соседних сервисов запрещён. Liquibase master:
`classpath:db/changelog/data-master.yaml`; TLS/mTLS обязательны на `8443`.

```bash
mvn -pl corelia-data-service -am test
```

Локально сервис запускает общий `./scripts/up.sh`. Схема владения и API:
[../docs/liquibase-ownership.md](../docs/liquibase-ownership.md),
[../docs/api.md](../docs/api.md).
