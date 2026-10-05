package ru.corelia.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static ru.corelia.support.Json.object;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.corelia.configuration.DocumentTypeCatalog;
import ru.corelia.http.ApiException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/** Проверяет native persistence на PostgreSQL с применением production Liquibase schema. */
@Testcontainers
class DataDocumentServicePostgresTest {
    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");
    private static DataDocumentService documents;
    private static DocumentTypeCatalog types;
    private static JdbcClient jdbc;
    private static TransactionTemplate transactions;

    @BeforeAll static void prepareDatabase() throws Exception {
        var dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        var liquibase = new SpringLiquibase();
        liquibase.setDataSource(dataSource);
        liquibase.setChangeLog("classpath:db/changelog/data-master.yaml");
        liquibase.afterPropertiesSet();
        types = mock(DocumentTypeCatalog.class);
        when(types.fields("TEST")).thenReturn(List.of("title"));
        jdbc = JdbcClient.create(dataSource);
        documents = new DataDocumentService(jdbc, types);
        transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Test void persistsReceiptAndEnforcesOptimisticVersion() {
        String id = UUID.randomUUID().toString();
        JsonNode created = inTransaction(() -> documents.create(create(id, "create-1", "hash-create")));
        assertEquals(true, created.path("created").asBoolean());
        assertEquals(false, inTransaction(() -> documents.create(create(id, "create-1", "hash-create"))).path("created").asBoolean());
        assertEquals("hash-create", documents.receipt("create-1").path("requestHash").asString());

        JsonNode state = documents.state("TEST", id);
        JsonNode command = commit(state, "commit-1", "hash-commit", "token-2");
        assertEquals("token-2", inTransaction(() -> documents.commit("TEST", id, command)).path("changeToken").asString());
        assertEquals("token-2", inTransaction(() -> documents.commit("TEST", id, command)).path("changeToken").asString());
        assertEquals(2, documents.state("TEST", id).path("document").path("currentVersion").asInt());

        var error = assertThrows(ApiException.class, () -> inTransaction(() -> documents.commit("TEST", id,
                commit(state, "commit-stale", "hash-stale", "token-stale"))));
        assertEquals(409, error.status());
    }

    @Test void concurrentRepeatReturnsTheOriginalReceipt() throws Exception {
        String id = UUID.randomUUID().toString();
        inTransaction(() -> documents.create(create(id, "create-concurrent", "hash-create")));
        JsonNode command = commit(documents.state("TEST", id), "commit-concurrent", "hash-commit", "token-2");
        var start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> { start.await(); return inTransaction(() -> documents.commit("TEST", id, command)); });
            var second = executor.submit(() -> { start.await(); return inTransaction(() -> documents.commit("TEST", id, command)); });
            start.countDown();
            assertEquals("token-2", first.get().path("changeToken").asString());
            assertEquals("token-2", second.get().path("changeToken").asString());
        } finally {
            executor.shutdownNow();
        }
        assertEquals(2, documents.state("TEST", id).path("document").path("currentVersion").asInt());
    }

    @Test void keepsAttachmentManifestAndAuditInImmutableVersion() {
        String id = UUID.randomUUID().toString();
        inTransaction(() -> documents.create(create(id, "create-attachment", "hash-create")));
        JsonNode state = documents.state("TEST", id);
        inTransaction(() -> documents.commit("TEST", id,
                commit(state, "commit-attachment", "hash-attachment", "token-2", attachment())));

        JsonNode current = documents.attachments(id).path("items");
        assertEquals(1, current.size());
        assertEquals("attachment-v1", current.get(0).path("id").asString());
        assertEquals("corelia-blob://attachment-v1", current.get(0).path("storageReference").asString());
        assertEquals("ATTACHMENT_ADDED", documents.history(id).path("items").get(1).path("action").asString());
        assertEquals("attachment-v1", documents.versions(id).path("items").get(1).path("attachments").get(0).path("id").asString());
    }

    @Test void searchesOnlySchemaApprovedFields() {
        String matchingId = UUID.randomUUID().toString();
        String otherId = UUID.randomUUID().toString();
        inTransaction(() -> documents.create(create(matchingId, "create-search-matching", "hash-matching", "Искомый")));
        inTransaction(() -> documents.create(create(otherId, "create-search-other", "hash-other", "Другой")));

        JsonNode result = documents.search(object("typeCode", "TEST", "filter",
                object("field", "title", "operator", "EQ", "value", "Искомый")));
        assertEquals(1, result.path("total").asInt());
        assertEquals(matchingId, result.path("items").get(0).path("id").asString());

        var error = assertThrows(ApiException.class, () -> documents.search(object("typeCode", "TEST", "filter",
                object("field", "title') or true --", "operator", "EQ", "value", "Искомый"))));
        assertEquals(400, error.status());
    }

    @Test void searchesAllTypesWithoutTypeCodeWhenNoFilterIsGiven() {
        String id = UUID.randomUUID().toString();
        inTransaction(() -> documents.create(create(id, "create-all-types", "hash-all-types")));

        JsonNode result = documents.search(object("limit", 10000));

        boolean found = false;
        for (JsonNode item : result.path("items")) if (id.equals(item.path("id").asString())) found = true;
        assertEquals(true, found);
    }

    @Test void rollsBackWholeCommitWhenAttachmentManifestIsInvalid() {
        String id = UUID.randomUUID().toString();
        inTransaction(() -> documents.create(create(id, "create-rollback", "hash-create")));
        JsonNode state = documents.state("TEST", id);
        ObjectNode invalidAttachment = (ObjectNode) attachment();
        invalidAttachment.remove("fileName");

        var error = assertThrows(ApiException.class, () -> inTransaction(() -> documents.commit("TEST", id,
                commit(state, "commit-rollback", "hash-rollback", "token-2", invalidAttachment))));
        assertEquals(400, error.status());
        assertEquals(1, documents.state("TEST", id).path("document").path("currentVersion").asInt());
        assertEquals(1, documents.versions(id).path("items").size());
        assertEquals(1, documents.history(id).path("items").size());
        assertEquals(1, count("select count(*) from outbox_event where aggregate_id = :id", id));
        assertEquals(false, documents.receipt("commit-rollback").path("found").asBoolean());
        assertEquals(0, count("select count(*) from logical_attachment where document_id = :id", id));
    }

    @Test void preservesDocumentStateAfterServiceReconstruction() {
        String id = UUID.randomUUID().toString();
        inTransaction(() -> documents.create(create(id, "create-restart", "hash-restart")));
        documents = new DataDocumentService(jdbc, types);
        assertEquals(id, documents.get("TEST", id).path("id").asString());
        assertEquals(1, documents.versions(id).path("items").size());
    }

    @Test void handlesBatchCreateAndSearch() {
        String prefix = "load-" + UUID.randomUUID();
        for (int index = 0; index < 100; index++) {
            String id = UUID.randomUUID().toString();
            inTransaction(() -> documents.create(create(id, "create-load-" + id, "hash-load-" + id, prefix + "-" + id)));
        }
        JsonNode result = documents.search(object("typeCode", "TEST", "limit", 200, "filter",
                object("field", "title", "operator", "STARTS_WITH", "value", prefix)));
        assertEquals(100, result.path("total").asInt());
        assertEquals(100, result.path("items").size());
    }

    @Test void restoresDocumentDataFromPostgresBackup() throws Exception {
        String id = UUID.randomUUID().toString();
        inTransaction(() -> documents.create(create(id, "create-backup", "hash-backup")));
        String restored = "corelia_restore_" + UUID.randomUUID().toString().replace("-", "");
        var result = POSTGRES.execInContainer("sh", "-c", "pg_dump -U " + POSTGRES.getUsername() + " -d " + POSTGRES.getDatabaseName()
                + " -f /tmp/corelia-data.sql && createdb -U " + POSTGRES.getUsername() + " " + restored
                + " && psql -U " + POSTGRES.getUsername() + " -d " + restored + " -f /tmp/corelia-data.sql >/dev/null"
                + " && psql -U " + POSTGRES.getUsername() + " -d " + restored + " -tAc \"select count(*) from document where id = '" + id + "'\"");
        assertEquals(0, result.getExitCode(), result.getStderr());
        assertEquals("1", result.getStdout().trim());
        assertEquals(0, POSTGRES.execInContainer("dropdb", "-U", POSTGRES.getUsername(), restored).getExitCode());
    }

    private static JsonNode create(String id, String key, String hash) {
        return create(id, key, hash, "Тест");
    }

    private static JsonNode create(String id, String key, String hash, String title) {
        return object("documentId", id, "typeCode", "TEST", "status", "CREATED", "createdBy", "tester",
                "createdAt", "2026-10-02T18:00:00Z", "idempotencyKey", key, "requestHash", hash,
                "attributes", object("title", title));
    }

    private static JsonNode commit(JsonNode state, String key, String hash, String token) {
        return commit(state, key, hash, token, null);
    }

    private static JsonNode commit(JsonNode state, String key, String hash, String token, JsonNode attachment) {
        ObjectNode result = (ObjectNode) object("expectedVersion", state.path("document").path("currentVersion").asInt(),
                "expectedChangeToken", state.path("document").path("changeToken").asString(),
                "attributes", object("title", object("value", "Изменён")), "status", "IN_WORK",
                "idempotencyKey", key, "requestHash", hash, "response", object("changeToken", token),
                "history", object("action", attachment == null ? "UPDATED" : "ATTACHMENT_ADDED"), "changeToken", token, "actor", "tester",
                "createdVersion", object("id", UUID.randomUUID().toString(), "number", 2, "schemaVersion", 1,
                        "createdBy", "tester", "attachments", tools.jackson.databind.json.JsonMapper.builder().build().createArrayNode()));
        if (attachment != null) ((ArrayNode) result.path("createdVersion").path("attachments")).add(attachment);
        return result;
    }

    private static JsonNode attachment() {
        return object("id", "attachment-v1", "logicalId", "attachment-logical", "documentId", "unused", "fileName", "file.txt",
                "contentType", "text/plain", "size", 4, "version", 1, "storageReference", "corelia-blob://attachment-v1",
                "uploadedAt", "2026-10-02T18:00:00Z");
    }

    private static JsonNode inTransaction(java.util.concurrent.Callable<JsonNode> action) {
        return transactions.execute(status -> {
            try { return action.call(); }
            catch (RuntimeException error) { throw error; }
            catch (Exception error) { throw new IllegalStateException(error); }
        });
    }

    private static long count(String sql, String id) {
        return jdbc.sql(sql).param("id", id).query(Long.class).single();
    }
}
