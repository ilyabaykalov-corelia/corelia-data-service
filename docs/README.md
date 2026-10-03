# Документация data-service

Сервис реализует atomic persistence на границе своей PostgreSQL базы:
проверяет ожидаемую версию/change token, фиксирует mutation и receipt без
частичной записи. Это не распространяет транзакцию на S3 или workflow.
Поиск, версии, history и attachment metadata возвращаются только через
внутренние mTLS endpoint.

Операционные параметры: `CORELIA_DATA_DB_URL`,
`CORELIA_DATA_DB_USERNAME` и secret `corelia.data.db.password`. Подробности
версий — [document-versioning](../../docs/document-versioning.md).
