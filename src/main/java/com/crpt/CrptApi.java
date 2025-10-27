package com.crpt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Клиент для вызова "Единого метода" Честного знака - создание документа
 * "Ввод в оборот (производство РФ)".
 * <ul>
 *   <li>Потокобезопасный: использует неизменяемые поля и общий {@link HttpClient}.</li>
 *   <li>Соблюдает ограничение частоты: перед отправкой запроса выполняет {@link RateLimiter#acquire()}.</li>
 *   <li>Собирает тело запроса строго по требованиям: {@code product_document} = Base64(JSON),
 *       {@code signature} = Base64(CAdES) над строкой {@code product_document}, {@code type=LP_INTRODUCE_GOODS}.</li>
 *   <li>Товарная группа передаётся в query-параметре {@code pg}.</li>
 * </ul>
 */
public final class CrptApi {

    private static final String CREATE_DOCUMENT_PATH = "/lk/documents/create";
    private static final String PRODUCT_GROUP_QUERY_PARAMETER = "pg";
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String HEADER_ACCEPT = "Accept";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String ACCEPT_ALL_VALUE = "*/*";
    private static final String AUTHORIZATION_HEADER_PREFIX = "Bearer ";
    private static final String DOCUMENT_FORMAT_MANUAL = "MANUAL";
    private static final String DOCUMENT_TYPE_INTRODUCE_GOODS = "LP_INTRODUCE_GOODS";
    private static final int HTTP_SUCCESS_MIN_INCLUSIVE = 200;
    private static final int HTTP_SUCCESS_MAX_EXCLUSIVE = 300;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI baseUri;
    private final RateLimiter requestRateLimiter;
    private final Duration requestTimeout;
    private final Base64.Encoder base64Encoder = Base64.getEncoder();

    /**
     * Создаёт клиент с заданными лимитом, базовым URL и таймаутом.
     *
     * @param rateLimitWindowTimeUnit единица окна лимитера (например, секунды/минуты)
     * @param maxRequestsPerWindow максимальное число запросов в одном окне (> 0)
     * @param baseUrl базовый URL API (например,
     *                "https://ismp.crpt.ru/api/v3" для прод
     *                или "https://markirovka.demo.crpt.tech/api/v3" для демо)
     * @param httpTimeout таймаут отдельного HTTP-запроса, должен быть > 0
     * @throws IllegalArgumentException при некорректных аргументах
     */
    public CrptApi(TimeUnit rateLimitWindowTimeUnit,
                   int maxRequestsPerWindow,
                   String baseUrl,
                   Duration httpTimeout) {

        if (rateLimitWindowTimeUnit == null) {
            throw new IllegalArgumentException("rateLimitWindowTimeUnit is null");
        }

        if (maxRequestsPerWindow <= 0) {
            throw new IllegalArgumentException("maxRequestsPerWindow must be greater than zero");
        }

        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("baseUrl is blank");
        }

        if (httpTimeout == null || httpTimeout.isNegative() || httpTimeout.isZero()) {
            throw new IllegalArgumentException("httpTimeout must be positive");
        }

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(httpTimeout)
                .version(HttpClient.Version.HTTP_2)
                .build();
        this.objectMapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.baseUri = URI.create(removeTrailingSlash(baseUrl));
        this.requestRateLimiter = new RateLimiter(rateLimitWindowTimeUnit, maxRequestsPerWindow);
        this.requestTimeout = httpTimeout;
    }

    /**
     * Создаёт документ "Ввод в оборот (производство РФ)".
     * <p>
     * Метод соблюдает лимит частоты вызовов. Документ сериализуется в JSON,
     * затем кодируется в Base64 и помещается в поле product_document.
     * Формируется тело единого метода с полями document_format, type,
     * product_document и signature. Запрос отправляется POST на
     * /lk/documents/create с параметром pg в строке запроса и заголовком
     * Authorization с токеном. При успешном ответе (коды 2xx) возвращает
     * тело ответа как строку. В остальных случаях выбрасывает CrptApiException.
     *
     * @param bearerToken токен доступа в формате Bearer
     * @param productGroup товарная группа, передается как параметр pg
     * @param document исходный документ для кодирования в product_document
     * @param signatureBase64 открепленная подпись CAdES в Base64 над строкой product_document
     * @return тело HTTP-ответа как строка
     * @throws CrptApiException при сетевых или сериализационных ошибках и при кодах ответа вне диапазона 2xx
     * @throws NullPointerException если любой параметр равен null
     */
    public String createIntroduceGoodsDocument(String bearerToken,
                                               String productGroup,
                                               IntroduceGoodsDocument document,
                                               String signatureBase64) {

        Objects.requireNonNull(bearerToken, "bearerToken is null");
        Objects.requireNonNull(productGroup, "productGroup is null");
        Objects.requireNonNull(document, "document is null");
        Objects.requireNonNull(signatureBase64, "signatureBase64 is null");

        requestRateLimiter.acquire();

        try {
            String productDocumentBase64 = encodeDocument(document);
            CreateRequest payload = new CreateRequest(
                    DOCUMENT_FORMAT_MANUAL,
                    DOCUMENT_TYPE_INTRODUCE_GOODS,
                    productDocumentBase64,
                    signatureBase64
            );
            String requestBody = objectMapper.writeValueAsString(payload);

            URI requestUri = baseUri.resolve(
                    CREATE_DOCUMENT_PATH + "?" + PRODUCT_GROUP_QUERY_PARAMETER + "="
                            + encodeQueryParameter(productGroup)
            );

            HttpRequest httpRequest = HttpRequest.newBuilder(requestUri)
                    .timeout(requestTimeout)
                    .header(HEADER_AUTHORIZATION, AUTHORIZATION_HEADER_PREFIX + bearerToken)
                    .header(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON)
                    .header(HEADER_ACCEPT, ACCEPT_ALL_VALUE)
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = httpClient.send(
                    httpRequest,
                    HttpResponse.BodyHandlers.ofString()
            );

            int statusCode = response.statusCode();
            if (statusCode >= HTTP_SUCCESS_MIN_INCLUSIVE && statusCode < HTTP_SUCCESS_MAX_EXCLUSIVE) {
                return response.body();
            }

            throw new CrptApiException(
                    "CHZ API error: HTTP " + statusCode + " - " + response.body(),
                    statusCode
            );
        } catch (CrptApiException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new CrptApiException("Failed to create document: " + exception.getMessage(), exception);
        }
    }

    /**
     * Сериализует документ в JSON и кодирует результат в Base64 (значение для {@code product_document}).
     *
     * @param document объект исходного документа
     * @return Base64-строка от JSON-представления документа
     * @throws JsonProcessingException если сериализация в JSON завершилась ошибкой
     */
    private String encodeDocument(IntroduceGoodsDocument document) throws JsonProcessingException {
        byte[] documentBytes = objectMapper.writeValueAsBytes(document);
        return base64Encoder.encodeToString(documentBytes);
    }

    /**
     * URL-экранирует значение.
     *
     * @param value исходное значение
     * @return закодированная строка для URL
     */
    private static String encodeQueryParameter(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /**
     * Убирает завершающий {@code /} у URL (если есть).
     *
     * @param url исходный URL
     * @return URL без завершающего слэша
     */
    private static String removeTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /**
     * JSON-тело запроса для «Единого метода» Честного знака:
     * {@code POST /lk/documents/create}.
     * <ul>
     *   <li>{@code document_format} — формат документа (например, {@code "MANUAL"});</li>
     *   <li>{@code type} — тип операции (для ввода в оборот: {@code "LP_INTRODUCE_GOODS"});</li>
     *   <li>{@code product_document} — Base64 от JSON исходного документа;</li>
     *   <li>{@code signature} — Base64 откреплённой CAdES-подписи над строкой {@code product_document}.</li>
     * </ul>
     * Товарная группа передаётся <b>в query</b> параметром {@code pg} и в это тело не входит.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static final class CreateRequest {

        @JsonProperty("document_format")
        private final String documentFormat;

        @JsonProperty("type")
        private final String type;

        @JsonProperty("product_document")
        private final String productDocument;

        @JsonProperty("signature")
        private final String signature;

        /**
         * Создаёт неизменяемый объект тела «единого метода» с обязательными полями.
         *
         * @param documentFormat значение {@code document_format}, например {@code "MANUAL"} (не пусто)
         * @param type значение {@code type}, например {@code "LP_INTRODUCE_GOODS"} (не пусто)
         * @param productDocument значение {@code product_document}: Base64(JSON) исходного документа (не пусто)
         * @param signature значение {@code signature}: Base64(CAdES) подписи над строкой {@code product_document} (не пусто)
         * @throws IllegalArgumentException если любой из параметров {@code null} или пустой/пробельный
         */
        CreateRequest(String documentFormat, String type, String productDocument, String signature) {
            this.documentFormat = requireNonBlank(documentFormat, "documentFormat");
            this.type = requireNonBlank(type, "type");
            this.productDocument = requireNonBlank(productDocument, "productDocument");
            this.signature = requireNonBlank(signature, "signature");
        }

        /**
         * Проверяет, что строковый параметр не null и не пуст/не состоит из пробельных символов.
         *
         * @param value значение параметра
         * @param name имя параметра (для сообщения об ошибке)
         * @return то же значение value, если оно корректно
         * @throws IllegalArgumentException если value == null или value.isBlank()
         */
        private static String requireNonBlank(String value, String name) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(name + " is blank");
            }
            return value;
        }
    }

    /**
     * Модель "исходного документа" для операции "Ввод в оборот (производство РФ)".
     * <p>
     * Этот объект сериализуется в JSON, после чего JSON кодируется в Base64 и
     * отправляется в поле {@code product_document} "Единого метода".
     * <br>
     * Набор обязательных полей и бизнес-правила зависят от конкретной товарной группы (pg).
     * В классе поля помечены как опциональные (NON_NULL), чтобы можно было
     * заполнять только применимое для вашей группы.
     *
     * <h3>Замечания</h3>
     * <ul>
     *   <li>Формат дат - как правило {@code yyyy-MM-dd}, если документация группы не требует иного.</li>
     *   <li>Строковые поля следует передавать без лишних пробелов; отсутствующие - оставлять {@code null}.</li>
     *   <li>{@link #products} содержит позиции (коды УИТ/УИТу и др.). Состав полей зависит от ТГ.</li>
     * </ul>
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class IntroduceGoodsDocument {

        @JsonProperty("description")
        private Description description;

        @JsonProperty("doc_id")
        private String documentId;

        @JsonProperty("doc_status")
        private String documentStatus;

        @JsonProperty("doc_type")
        private String documentType;

        @JsonProperty("importRequest")
        private Boolean importRequest;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("participant_inn")
        private String participantInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        private String productionDate;

        @JsonProperty("production_type")
        private String productionType;

        @JsonProperty("products")
        private List<Product> products;

        @JsonProperty("reg_date")
        private String registrationDate;

        @JsonProperty("reg_number")
        private String registrationNumber;

        public Description getDescription() {
            return description;
        }

        public void setDescription(Description description) {
            this.description = description;
        }

        public String getDocumentId() {
            return documentId;
        }

        public void setDocumentId(String documentId) {
            this.documentId = documentId;
        }

        public String getDocumentStatus() {
            return documentStatus;
        }

        public void setDocumentStatus(String documentStatus) {
            this.documentStatus = documentStatus;
        }

        public String getDocumentType() {
            return documentType;
        }

        public void setDocumentType(String documentType) {
            this.documentType = documentType;
        }

        public Boolean getImportRequest() {
            return importRequest;
        }

        public void setImportRequest(Boolean importRequest) {
            this.importRequest = importRequest;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public void setOwnerInn(String ownerInn) {
            this.ownerInn = ownerInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public void setProducerInn(String producerInn) {
            this.producerInn = producerInn;
        }

        public String getProductionDate() {
            return productionDate;
        }

        public void setProductionDate(String productionDate) {
            this.productionDate = productionDate;
        }

        public String getProductionType() {
            return productionType;
        }

        public void setProductionType(String productionType) {
            this.productionType = productionType;
        }

        public List<Product> getProducts() {
            return products;
        }

        public void setProducts(List<Product> products) {
            this.products = products;
        }

        public String getRegistrationDate() {
            return registrationDate;
        }

        public void setRegistrationDate(String registrationDate) {
            this.registrationDate = registrationDate;
        }

        public String getRegistrationNumber() {
            return registrationNumber;
        }

        public void setRegistrationNumber(String registrationNumber) {
            this.registrationNumber = registrationNumber;
        }

        /**
         * Дополнительная информация по документу/участнику.
         * <p>Содержит, например, ИНН участника оборота.</p>
         */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static final class Description {

            @JsonProperty("participantInn")
            private String participantInn;

            public String getParticipantInn() {
                return participantInn;
            }

            public void setParticipantInn(String participantInn) {
                this.participantInn = participantInn;
            }
        }

        /**
         * Товарная позиция в документе ввода в оборот.
         * <p>Набор обязательных полей зависит от товарной группы.</p>
         */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public static final class Product {

            @JsonProperty("certificate_document")
            private String certificateDocument;

            @JsonProperty("certificate_document_date")
            private String certificateDocumentDate;

            @JsonProperty("certificate_document_number")
            private String certificateDocumentNumber;

            @JsonProperty("owner_inn")
            private String ownerInn;

            @JsonProperty("producer_inn")
            private String producerInn;

            @JsonProperty("production_date")
            private String productionDate;

            @JsonProperty("tnved_code")
            private String tnvedCode;

            @JsonProperty("uit_code")
            private String uitCode;

            @JsonProperty("uitu_code")
            private String uituCode;

            public String getCertificateDocument() {
                return certificateDocument;
            }

            public void setCertificateDocument(String certificateDocument) {
                this.certificateDocument = certificateDocument;
            }

            public String getCertificateDocumentDate() {
                return certificateDocumentDate;
            }

            public void setCertificateDocumentDate(String certificateDocumentDate) {
                this.certificateDocumentDate = certificateDocumentDate;
            }

            public String getCertificateDocumentNumber() {
                return certificateDocumentNumber;
            }

            public void setCertificateDocumentNumber(String certificateDocumentNumber) {
                this.certificateDocumentNumber = certificateDocumentNumber;
            }

            public String getOwnerInn() {
                return ownerInn;
            }

            public void setOwnerInn(String ownerInn) {
                this.ownerInn = ownerInn;
            }

            public String getProducerInn() {
                return producerInn;
            }

            public void setProducerInn(String producerInn) {
                this.producerInn = producerInn;
            }

            public String getProductionDate() {
                return productionDate;
            }

            public void setProductionDate(String productionDate) {
                this.productionDate = productionDate;
            }

            public String getTnvedCode() {
                return tnvedCode;
            }

            public void setTnvedCode(String tnvedCode) {
                this.tnvedCode = tnvedCode;
            }

            public String getUitCode() {
                return uitCode;
            }

            public void setUitCode(String uitCode) {
                this.uitCode = uitCode;
            }

            public String getUituCode() {
                return uituCode;
            }

            public void setUituCode(String uituCode) {
                this.uituCode = uituCode;
            }
        }
    }

    /**
     * Исключение клиента Честного знака.
     * <p>
     * При наличии кода ответа HTTP он сохраняется в {@link #getHttpStatusCode()}.
     */
    public static final class CrptApiException extends RuntimeException {

        private final Integer httpStatusCode;

        /**
         * Создаёт исключение без кода HTTP.
         *
         * @param message текст ошибки
         */
        public CrptApiException(String message) {
            super(message);
            this.httpStatusCode = null;
        }

        /**
         * Создаёт исключение без кода HTTP с первопричиной.
         *
         * @param message текст ошибки
         * @param cause   первопричина
         */
        public CrptApiException(String message, Throwable cause) {
            super(message, cause);
            this.httpStatusCode = null;
        }

        /**
         * Создаёт исключение с кодом ответа HTTP.
         *
         * @param message        текст ошибки
         * @param httpStatusCode код HTTP-ответа сервера
         */
        public CrptApiException(String message, int httpStatusCode) {
            super(message);
            this.httpStatusCode = httpStatusCode;
        }

        public Integer getHttpStatusCode() {
            return httpStatusCode;
        }
    }

    /**
     * Лимитер частоты.
     * <p>
     * За каждый фиксированный интервал времени (ровно {@code 1 * windowTimeUnit})
     * допускается не более {@code maxRequestsPerWindow} вызовов. Если лимит исчерпан,
     * {@link #acquire()} блокирует вызывающий поток до начала следующего окна.
     */
    private static final class RateLimiter {

        private static final long DEFAULT_WINDOW_LENGTH_IN_TIME_UNITS = 1L;

        private final int maxRequestsPerWindow;
        private final Semaphore permits;
        private final ScheduledExecutorService scheduler;
        private final TimeUnit windowTimeUnit;

        /**
         * Создаёт лимитер: допускает не более {@code maxRequestsPerWindow} вызовов за {@code 1 * windowTimeUnit}.
         *
         * @param windowTimeUnit единица окна (например, {@link TimeUnit#SECONDS})
         * @param maxRequestsPerWindow максимум вызовов в одном окне (> 0)
         * @throws IllegalArgumentException если {@code windowTimeUnit == null} или {@code maxRequestsPerWindow <= 0}
         */
        RateLimiter(TimeUnit windowTimeUnit, int maxRequestsPerWindow) {
            if (windowTimeUnit == null) {
                throw new IllegalArgumentException("windowTimeUnit is null");
            }
            if (maxRequestsPerWindow <= 0) {
                throw new IllegalArgumentException("maxRequestsPerWindow must be > 0");
            }
            this.windowTimeUnit = windowTimeUnit;
            this.maxRequestsPerWindow = maxRequestsPerWindow;
            this.permits = new Semaphore(maxRequestsPerWindow, true);
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
            this.scheduler.scheduleAtFixedRate(
                    this::resetWindowPermits,
                    DEFAULT_WINDOW_LENGTH_IN_TIME_UNITS,
                    DEFAULT_WINDOW_LENGTH_IN_TIME_UNITS,
                    this.windowTimeUnit
            );
        }

        /**
         * Получает "разрешение" на вызов в рамках текущего окна.
         * <p>
         * Если разрешения есть - возвращает управление сразу. Если исчерпаны - блокирует
         * текущий поток до тех пор, пока в начале следующего окна разрешения не будут обновлены.
         *
         * @throws IllegalStateException если поток был прерван во время ожидания
         */
        void acquire() {
            try {
                permits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Thread interrupted while waiting for rate limit permit", e);
            }
        }

        /**
         * Останавливает фоновый планировщик.
         */
        void shutdown() {
            this.scheduler.shutdownNow();
        }

        /**
         * Обновляет число доступных разрешений на старте окна.
         */
        private void resetWindowPermits() {
            int available = permits.availablePermits();
            int toRelease = maxRequestsPerWindow - available;
            if (toRelease > 0) {
                permits.release(toRelease);
            }
        }
    }
}
