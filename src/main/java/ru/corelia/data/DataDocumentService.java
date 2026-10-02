package ru.corelia.data;

import static ru.corelia.support.Json.*;

import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.corelia.http.ApiException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Атомарно сохраняет начальный snapshot документа и его неизменяемую версию. */
@Service
public class DataDocumentService {
    private final JdbcClient jdbc;

    public DataDocumentService(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public JsonNode create(JsonNode body) {
        String id = required(body, "documentId");
        String type = required(body, "typeCode");
        String status = required(body, "status");
        String actor = required(body, "createdBy");
        String key = required(body, "idempotencyKey");
        String hash = required(body, "requestHash");
        ObjectNode attributes = copy(body.path("attributes"));
        Instant createdAt = instant(body, "createdAt");

        String priorHash = jdbc.sql("select request_hash from idempotency_receipt where idempotency_key = :key")
                .param("key", key).query(String.class).optional().orElse(null);
        if (priorHash != null) {
            if (!priorHash.equals(hash)) throw new ApiException(409, "Ключ идемпотентности уже использован для других данных");
            return object("created", false, "documentId", id);
        }

        try {
            jdbc.sql("""
                    insert into document (id, type_code, status, current_version, change_token, attributes, created_by, created_at, updated_by, updated_at)
                    values (:id, :type, :status, 1, :token, cast(:attributes as jsonb), :actor, :createdAt, :actor, :createdAt)
                    """)
                    .param("id", id).param("type", type).param("status", status).param("token", UUID.randomUUID().toString())
                    .param("attributes", write(attributes)).param("actor", actor).param("createdAt", createdAt).update();
        } catch (org.springframework.dao.DuplicateKeyException error) {
            throw new ApiException(409, "Документ уже существует");
        }

        String versionId = UUID.randomUUID().toString();
        jdbc.sql("""
                insert into document_version (id, document_id, version_no, schema_version, status, attributes, created_by, created_at)
                values (:id, :documentId, 1, :schemaVersion, :status, cast(:attributes as jsonb), :actor, :createdAt)
                """)
                .param("id", versionId).param("documentId", id).param("schemaVersion", number(body, "schemaVersion", 1))
                .param("status", status).param("attributes", write(attributes)).param("actor", actor).param("createdAt", createdAt).update();

        JsonNode history = body.path("history").isObject() ? body.path("history") : object("action", "DOCUMENT_CREATED");
        jdbc.sql("insert into document_audit (id, document_id, payload, occurred_at) values (:id, :documentId, cast(:payload as jsonb), :at)")
                .param("id", UUID.randomUUID().toString()).param("documentId", id).param("payload", write(history)).param("at", createdAt).update();
        JsonNode response = object("created", true, "documentId", id);
        jdbc.sql("insert into idempotency_receipt (idempotency_key, document_id, request_hash, response, created_at) values (:key, :documentId, :hash, cast(:response as jsonb), :at)")
                .param("key", key).param("documentId", id).param("hash", hash).param("response", write(response)).param("at", createdAt).update();
        jdbc.sql("insert into outbox_event (id, aggregate_type, aggregate_id, event_type, payload, created_at) values (:id, 'document', :documentId, 'DOCUMENT_CREATED', cast(:payload as jsonb), :at)")
                .param("id", UUID.randomUUID().toString()).param("documentId", id).param("payload", write(history)).param("at", createdAt).update();
        return response;
    }

    private static String required(JsonNode body, String field) {
        String value = text(body, field);
        if (value.isEmpty()) throw new ApiException(400, "Не указано поле " + field);
        return value;
    }

    private static Instant instant(JsonNode body, String field) {
        try {
            return Instant.parse(required(body, field));
        } catch (RuntimeException error) {
            throw new ApiException(400, "Некорректное время " + field);
        }
    }
}
