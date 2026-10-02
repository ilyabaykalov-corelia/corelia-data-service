package ru.corelia.data;

import static ru.corelia.support.Json.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.corelia.http.ApiException;
import ru.corelia.configuration.DocumentTypeCatalog;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Атомарно сохраняет начальный snapshot документа и его неизменяемую версию. */
@Service
public class DataDocumentService {
    private final JdbcClient jdbc;
    private final DocumentTypeCatalog types;

    public DataDocumentService(JdbcClient jdbc, DocumentTypeCatalog types) {
        this.jdbc = jdbc; this.types = types;
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
        Timestamp createdAt = Timestamp.from(instant(body, "createdAt"));

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

    private void validateFilter(String type, JsonNode filter) {
        if (filter.isMissingNode() || filter.isNull()) return;
        if (!filter.isObject()) throw new ApiException(400, "Некорректный фильтр поиска");
        if (filter.has("field")) {
            String field = required(filter, "field");
            if (!types.fields(type).contains(field)) throw new ApiException(400, "Поле фильтра не входит в schema документа");
            String operator = required(filter, "operator");
            if (!java.util.Set.of("EQ", "NE", "GT", "GTE", "LT", "LTE", "IN", "NOT_IN", "EXISTS", "CONTAINS", "STARTS_WITH").contains(operator))
                throw new ApiException(400, "Неизвестный оператор фильтра");
            return;
        }
        if (filter.has("items")) {
            String operator = required(filter, "operator");
            if (!java.util.Set.of("AND", "OR").contains(operator) || !filter.path("items").isArray()) throw new ApiException(400, "Некорректная группа фильтра");
            for (JsonNode item : filter.path("items")) validateFilter(type, item);
            return;
        }
        if (filter.has("item")) { validateFilter(type, filter.path("item")); return; }
        throw new ApiException(400, "Некорректный фильтр поиска");
    }

    public JsonNode get(String type, String id) {
        return jdbc.sql("""
                select id, type_code, status, current_version, change_token, attributes, created_by, created_at
                from document where id = :id and type_code = :type
                """).param("id", id).param("type", type).query(this::snapshot).optional()
                .orElseThrow(() -> new ApiException(404, "Документ не найден"));
    }

    public JsonNode search(JsonNode body) {
        String type = required(body, "typeCode");
        validateFilter(type, body.path("filter"));
        int offset = Math.max(0, (int) number(body, "offset", 0));
        int limit = Math.min(10000, Math.max(1, (int) number(body, "limit", 1000)));
        QueryClause filter = compileFilter(body.path("filter"), new Counter());
        String where = "d.type_code = :type" + (filter.sql().isEmpty() ? "" : " and " + filter.sql());
        JdbcClient.StatementSpec itemsQuery = jdbc.sql("select d.id, d.type_code, d.status, d.current_version, d.change_token, d.attributes, d.created_by, d.created_at from document d where " + where + " order by d.created_at desc, d.id asc offset :offset limit :limit")
                .param("type", type).param("offset", offset).param("limit", limit);
        JdbcClient.StatementSpec totalQuery = jdbc.sql("select count(*) from document d where " + where).param("type", type);
        for (var parameter : filter.parameters().entrySet()) { itemsQuery = itemsQuery.param(parameter.getKey(), parameter.getValue()); totalQuery = totalQuery.param(parameter.getKey(), parameter.getValue()); }
        List<JsonNode> items = itemsQuery.query(this::snapshot).list();
        long total = totalQuery.query(Long.class).single();
        return object("items", items, "total", total);
    }

    /** Компилирует разрешённый AST в SQL только с именованными JDBC-параметрами. */
    private QueryClause compileFilter(JsonNode filter, Counter counter) {
        if (filter.isMissingNode() || filter.isNull()) return new QueryClause("", Map.of());
        if (filter.has("field")) return compileComparison(filter, counter);
        if (filter.has("items")) {
            var parts = new java.util.ArrayList<String>(); var parameters = new LinkedHashMap<String, Object>();
            for (JsonNode item : filter.path("items")) { QueryClause part = compileFilter(item, counter); parts.add(part.sql()); parameters.putAll(part.parameters()); }
            return new QueryClause("(" + String.join(" " + text(filter, "operator") + " ", parts) + ")", parameters);
        }
        QueryClause item = compileFilter(filter.path("item"), counter);
        return new QueryClause("not (" + item.sql() + ")", item.parameters());
    }

    private QueryClause compileComparison(JsonNode filter, Counter counter) {
        String field = text(filter, "field"), operator = text(filter, "operator"), fieldKey = "field" + counter.next();
        var parameters = new LinkedHashMap<String, Object>(); parameters.put(fieldKey, field);
        String expression = "d.attributes ->> :" + fieldKey;
        if (operator.equals("EXISTS")) return new QueryClause("d.attributes ? :" + fieldKey, parameters);
        JsonNode value = filter.path("value");
        if (operator.equals("IN") || operator.equals("NOT_IN")) {
            var values = new java.util.ArrayList<String>();
            for (JsonNode item : list(value)) { String key = "value" + counter.next(); parameters.put(key, scalar(item)); values.add(":" + key); }
            if (values.isEmpty()) throw new ApiException(400, "Пустой список фильтра");
            return new QueryClause(expression + (operator.equals("IN") ? " in (" : " not in (") + String.join(",", values) + ")", parameters);
        }
        String valueKey = "value" + counter.next(); parameters.put(valueKey, scalar(value));
        String sql = switch (operator) {
            case "EQ" -> expression + " = :" + valueKey;
            case "NE" -> expression + " <> :" + valueKey;
            case "GT" -> expression + " > :" + valueKey;
            case "GTE" -> expression + " >= :" + valueKey;
            case "LT" -> expression + " < :" + valueKey;
            case "LTE" -> expression + " <= :" + valueKey;
            case "CONTAINS" -> expression + " ilike '%' || :" + valueKey + " || '%'";
            case "STARTS_WITH" -> expression + " ilike :" + valueKey + " || '%'";
            default -> throw new ApiException(400, "Некорректный оператор фильтра");
        };
        return new QueryClause(sql, parameters);
    }

    private static String scalar(JsonNode value) {
        if (value.isTextual() || value.isNumber() || value.isBoolean()) return value.asString();
        throw new ApiException(400, "Некорректное значение фильтра");
    }

    private record QueryClause(String sql, Map<String, Object> parameters) {}
    private static final class Counter { private int value; int next() { return ++value; } }

    public JsonNode type(String id) {
        String type = jdbc.sql("select type_code from document where id = :id").param("id", id).query(String.class).optional()
                .orElseThrow(() -> new ApiException(404, "Документ не найден"));
        return object("typeCode", type);
    }

    public JsonNode versions(String id) {
        requireDocument(id);
        return object("items", jdbc.sql("""
                select id, document_id, version_no, schema_version, status, attributes, created_by, created_at, closed_at
                from document_version where document_id = :id order by version_no asc
                """).param("id", id).query(this::version).list());
    }

    public JsonNode attachments(String id) {
        requireDocument(id);
        return object("items", attachmentsForCurrentVersion(id));
    }

    public JsonNode history(String id) {
        requireDocument(id);
        return object("items", jdbc.sql("select payload from document_audit where document_id = :id order by occurred_at asc, id asc")
                .param("id", id).query(String.class).list().stream().map(value -> parse(value)).toList());
    }

    public JsonNode receipt(String key) {
        return jdbc.sql("select request_hash, response from idempotency_receipt where idempotency_key = :key").param("key", key)
                .query((row, ignored) -> object("found", true, "requestHash", row.getString("request_hash"), "response", parse(row.getString("response"))))
                .optional().orElse(object("found", false));
    }

    public JsonNode state(String type, String id) {
        JsonNode document = get(type, id);
        var versions = versions(id).path("items");
        JsonNode current = list(versions).stream().filter(item -> number(item, "number", -1) == number(document, "currentVersion", 0)).findFirst()
                .orElseThrow(() -> new ApiException(500, "Не найдена текущая версия документа"));
        return object("document", document, "currentVersion", current, "versions", versions, "attachments", attachmentsForCurrentVersion(id));
    }

    @Transactional
    public JsonNode commit(String type, String id, JsonNode body) {
        String key = required(body, "idempotencyKey"), hash = required(body, "requestHash");
        JsonNode prior = matchingReceipt(key, hash);
        if (prior != null) return prior;
        JsonNode current = jdbc.sql("select id, type_code, status, current_version, change_token, attributes, created_by, created_at from document where id = :id and type_code = :type for update")
                .param("id", id).param("type", type).query(this::snapshot).optional().orElseThrow(() -> new ApiException(404, "Документ не найден"));
        prior = matchingReceipt(key, hash);
        if (prior != null) return prior;
        if (number(current, "currentVersion", -1) != number(body, "expectedVersion", -2)
                || !text(current, "changeToken").equals(text(body, "expectedChangeToken")))
            throw new ApiException(409, "Документ был изменён конкурентно");
        ObjectNode attributes = copy(body.path("attributes"));
        String status = text(body, "status"); if (status.isEmpty()) status = text(current, "status");
        String token = required(body, "changeToken");
        JsonNode created = body.path("createdVersion");
        Timestamp now = Timestamp.from(Instant.now());
        if (created.isObject()) {
            int version = (int) number(created, "number", -1);
            if (version != number(current, "currentVersion", 0) + 1) throw new ApiException(409, "Некорректный номер новой версии");
            String versionId = text(created, "id").isEmpty() ? java.util.UUID.randomUUID().toString() : text(created, "id");
            jdbc.sql("update document_version set closed_at = :now where document_id = :id and version_no = :version")
                    .param("now", now).param("id", id).param("version", number(current, "currentVersion", 0)).update();
            jdbc.sql("insert into document_version (id, document_id, version_no, schema_version, status, attributes, created_by, created_at) values (:id, :documentId, :number, :schemaVersion, :status, cast(:attributes as jsonb), :actor, :now)")
                    .param("id", versionId).param("documentId", id).param("number", version).param("schemaVersion", number(created, "schemaVersion", 1)).param("status", status).param("attributes", write(attributes)).param("actor", required(created, "createdBy")).param("now", now).update();
            persistManifest(id, versionId, created.path("attachments"), now);
        }
        jdbc.sql("update document set status = :status, current_version = :version, change_token = :token, attributes = cast(:attributes as jsonb), updated_by = :actor, updated_at = :now where id = :id")
                .param("status", status).param("version", created.isObject() ? number(created, "number", 0) : number(current, "currentVersion", 0)).param("token", token).param("attributes", write(attributes)).param("actor", required(body, "actor")).param("now", now).param("id", id).update();
        JsonNode history = body.path("history").isObject() ? body.path("history") : object("action", "DOCUMENT_CHANGED");
        JsonNode response = body.path("response").isObject() ? body.path("response") : object();
        jdbc.sql("insert into document_audit (id, document_id, payload, occurred_at) values (:id, :documentId, cast(:payload as jsonb), :now)").param("id", java.util.UUID.randomUUID().toString()).param("documentId", id).param("payload", write(history)).param("now", now).update();
        jdbc.sql("insert into idempotency_receipt (idempotency_key, document_id, request_hash, response, created_at) values (:key, :documentId, :hash, cast(:response as jsonb), :now)").param("key", key).param("documentId", id).param("hash", hash).param("response", write(response)).param("now", now).update();
        jdbc.sql("insert into outbox_event (id, aggregate_type, aggregate_id, event_type, payload, created_at) values (:id, 'document', :documentId, 'DOCUMENT_CHANGED', cast(:payload as jsonb), :now)").param("id", java.util.UUID.randomUUID().toString()).param("documentId", id).param("payload", write(history)).param("now", now).update();
        return response;
    }

    /** Повторная проверка после блокировки строки устраняет гонку одинаковых команд. */
    private JsonNode matchingReceipt(String key, String hash) {
        JsonNode prior = receipt(key);
        if (!prior.path("found").asBoolean()) return null;
        if (!hash.equals(text(prior, "requestHash"))) throw new ApiException(409, "Ключ идемпотентности уже использован для других данных");
        return prior.path("response");
    }

    private void persistManifest(String documentId, String versionId, JsonNode values, Timestamp now) {
        for (JsonNode value : list(values)) {
            String logicalId = required(value, "logicalId"), attachmentId = required(value, "id");
            jdbc.sql("insert into logical_attachment (id, document_id, created_at) values (:id, :documentId, :now) on conflict (id) do nothing")
                    .param("id", logicalId).param("documentId", documentId).param("now", now).update();
            jdbc.sql("insert into attachment_version (id, logical_attachment_id, document_id, version_no, file_name, content_type, size_bytes, storage_reference, uploaded_at) values (:id, :logicalId, :documentId, :number, :fileName, :contentType, :size, :storageReference, :uploadedAt) on conflict (id) do nothing")
                    .param("id", attachmentId).param("logicalId", logicalId).param("documentId", documentId).param("number", number(value, "version", 1)).param("fileName", required(value, "fileName")).param("contentType", required(value, "contentType")).param("size", number(value, "size", 0)).param("storageReference", required(value, "storageReference")).param("uploadedAt", text(value, "uploadedAt").isEmpty() ? null : Timestamp.from(instant(value, "uploadedAt"))).update();
            jdbc.sql("insert into document_version_attachment (document_version_id, attachment_version_id) values (:versionId, :attachmentId) on conflict do nothing")
                    .param("versionId", versionId).param("attachmentId", attachmentId).update();
        }
    }

    private JsonNode version(ResultSet row, int ignored) throws SQLException {
        ObjectNode result = object("id", row.getString("id"), "documentId", row.getString("document_id"),
                "number", row.getInt("version_no"), "schemaVersion", row.getInt("schema_version"),
                "status", row.getString("status"), "attributes", parse(row.getString("attributes")),
                "createdBy", row.getString("created_by"), "createdAt", row.getTimestamp("created_at").toInstant().toString());
        if (row.getTimestamp("closed_at") != null) result.put("closedAt", row.getTimestamp("closed_at").toInstant().toString());
        result.set("attachments", array(attachmentsForVersion(row.getString("id"))));
        return result;
    }

    private List<JsonNode> attachmentsForCurrentVersion(String documentId) {
        String versionId = jdbc.sql("select id from document_version where document_id = :id order by version_no desc limit 1")
                .param("id", documentId).query(String.class).optional().orElseThrow(() -> new ApiException(500, "Не найдена текущая версия документа"));
        return attachmentsForVersion(versionId);
    }

    private List<JsonNode> attachmentsForVersion(String versionId) {
        return jdbc.sql("""
                select av.id, av.logical_attachment_id, av.document_id, av.file_name, av.content_type, av.size_bytes, av.version_no, av.storage_reference, av.uploaded_at
                from document_version_attachment dva join attachment_version av on av.id = dva.attachment_version_id
                where dva.document_version_id = :versionId order by av.logical_attachment_id, av.version_no
                """).param("versionId", versionId).query((row, ignored) -> {
                    ObjectNode value = object("id", row.getString("id"), "logicalId", row.getString("logical_attachment_id"),
                            "documentId", row.getString("document_id"), "fileName", row.getString("file_name"), "contentType", row.getString("content_type"),
                            "size", row.getLong("size_bytes"), "version", row.getLong("version_no"), "current", true,
                            "storageReference", row.getString("storage_reference"));
                    if (row.getTimestamp("uploaded_at") != null) value.put("uploadedAt", row.getTimestamp("uploaded_at").toInstant().toString());
                    return value;
                }).list().stream().map(value -> (JsonNode) value).toList();
    }

    public JsonNode attachment(String id) {
        return attachmentRow("where av.id = :id", id).stream().findFirst().orElseThrow(() -> new ApiException(404, "Вложение не найдено"));
    }

    public JsonNode attachmentVersions(String id) {
        JsonNode current = attachment(id);
        return object("items", attachmentRow("where av.logical_attachment_id = :id", text(current, "logicalId")));
    }

    private List<JsonNode> attachmentRow(String where, String id) {
        return jdbc.sql("select av.id, av.logical_attachment_id, av.document_id, av.file_name, av.content_type, av.size_bytes, av.version_no, av.storage_reference, av.uploaded_at, exists (select 1 from document_version_attachment dva join document_version dv on dv.id = dva.document_version_id where dva.attachment_version_id = av.id and dv.closed_at is null) as current from attachment_version av " + where + " order by av.version_no")
                .param("id", id).query((row, ignored) -> object("id", row.getString("id"), "logicalId", row.getString("logical_attachment_id"), "documentId", row.getString("document_id"), "fileName", row.getString("file_name"), "contentType", row.getString("content_type"), "size", row.getLong("size_bytes"), "version", row.getLong("version_no"), "current", row.getBoolean("current"), "storageReference", row.getString("storage_reference"), "uploadedAt", row.getTimestamp("uploaded_at") == null ? "" : row.getTimestamp("uploaded_at").toInstant().toString())).list().stream().map(value -> (JsonNode) value).toList();
    }

    private void requireDocument(String id) { type(id); }

    private JsonNode snapshot(ResultSet row, int ignored) throws SQLException {
        ObjectNode result = object("id", row.getString("id"), "typeCode", row.getString("type_code"),
                "status", row.getString("status"), "currentVersion", row.getInt("current_version"),
                "changeToken", row.getString("change_token"), "createdBy", row.getString("created_by"),
                "createdAt", row.getTimestamp("created_at").toInstant().toString());
        result.set("attributes", parse(row.getString("attributes")));
        return result;
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
