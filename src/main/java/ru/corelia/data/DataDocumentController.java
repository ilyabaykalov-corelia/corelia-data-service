package ru.corelia.data;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.corelia.http.ApiRequest;
import tools.jackson.databind.JsonNode;

/** Внутренние команды native document storage. */
@RestController
@RequestMapping("/internal/v1/data/documents")
public class DataDocumentController {
    private final ApiRequest requests;
    private final DataDocumentService documents;

    public DataDocumentController(ApiRequest requests, DataDocumentService documents) {
        this.requests = requests;
        this.documents = documents;
    }

    @PostMapping
    public JsonNode create(HttpServletRequest request) {
        return documents.create(requests.body(request));
    }

    @PostMapping("/search")
    public JsonNode search(HttpServletRequest request) {
        return documents.search(requests.body(request));
    }

    @GetMapping("/{type}/{id}")
    public JsonNode get(@PathVariable String type, @PathVariable String id) {
        return documents.get(type, id);
    }
}
