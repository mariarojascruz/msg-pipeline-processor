package com.msgpipeline.processor.adapter.out.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msgpipeline.processor.domain.model.Message;
import com.msgpipeline.processor.domain.port.out.EventPublisherPort;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.*;

import java.time.Instant;
import java.util.Map;

/**
 * =========================================================================
 * CLASE: EventBridgeAdapter — Adaptador de Salida (AWS EventBridge)
 * CAPA: Infraestructura — Adaptador de Salida (Output Adapter)
 * ARQUITECTURA: Hexagonal
 * =========================================================================
 *
 * NUEVO EN SESIÓN 05: Implementación del puerto EventPublisherPort
 * usando AWS EventBridge SDK v2.
 *
 * PATRÓN: Observer (implementación con EventBridge)
 *   El Processor es el "Sujeto" (Publisher).
 *   El Audit Lambda es el "Observador" (Subscriber).
 *   EventBridge es el "Bus de Eventos" que desacopla ambos.
 *
 * @Profile("aws"): Solo activo en Lambda (AWS).
 *   En 'local': InMemoryEventAdapter (sin EventBridge real).
 *
 * FLUJO DEL EVENTO:
 *   1. EventBridgeAdapter.publishMessageReceived(message)
 *   2. Construye PutEventsRequestEntry con:
 *      - source: "msg-pipeline.processor"
 *      - detail-type: "MessageReceived"
 *      - detail: JSON con datos del mensaje
 *      - eventBusName: msg-pipeline-events-sesion-05
 *   3. EventBridgeClient.putEvents() → envía a AWS EventBridge
 *   4. EventBridge evalúa Rules → match con msg-pipeline-processed-rule
 *   5. Rule enruta al target: msg-pipeline-audit-sesion-05 (Lambda)
 *
 * EVENTBRIDGE TERMS:
 *   - Event Bus: canal de eventos (msg-pipeline-events-sesion-05)
 *   - Event: mensaje de datos estructurado (JSON)
 *   - Rule: patrón de filtrado (source + detail-type + campos del detail)
 *   - Target: destino del evento cuando match la Rule (Lambda Audit)
 *
 * STATIC FIELDS:
 *   EventBridgeClient es thread-safe y caro de crear.
 *   Se inicializa como campo estático en el bloque static del handler
 *   para reutilizarlo en warm starts (no se recrea en cada invocación).
 * =========================================================================
 */
@Slf4j
@Component
@Profile("aws")
public class EventBridgeAdapter implements EventPublisherPort {

    // EventBridgeClient: thread-safe, reutilizable entre invocaciones warm
    // (bean singleton de Spring — el contexto ya se cachea en el cold start del handler)
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private EventBridgeClient eventBridgeClient;

    @Value("${app.aws.event-bus-name:msg-pipeline-events-sesion-05}")
    private String eventBusName;

    @Value("${app.aws.region:us-east-1}")
    private String region;

    @PostConstruct
    void init() {
        // Se ejecuta una vez que Spring inyectó @Value (no se puede hacer en un bloque static)
        eventBridgeClient = EventBridgeClient.builder()
                .region(Region.of(region))
                .build();
        // Las credenciales las toma del IAM Role del Lambda automáticamente
        // (no hardcodeamos access keys — NUNCA se deben hardcodear)
    }

    /**
     * Publica el evento "MessageReceived" en EventBridge.
     *
     * FORMATO DEL EVENTO ENVIADO A EVENTBRIDGE:
     * {
     *   "Source": "msg-pipeline.processor",
     *   "DetailType": "MessageReceived",
     *   "Detail": {
     *     "messageId": "uuid...",
     *     "messageType": "EMAIL",
     *     "channel": "EMAIL",
     *     "recipientEmail": "test@ejemplo.com",
     *     "status": "PENDING",
     *     "createdAt": "2025-...",
     *     "requestId": "api-gw-request-id",
     *     "timestamp": "2025-..."
     *   },
     *   "EventBusName": "msg-pipeline-events-sesion-05"
     * }
     *
     * LA RULE EN EVENTBRIDGE filtra por:
     *   { "source": ["msg-pipeline.processor"] }
     *   Y enruta al TARGET: msg-pipeline-audit-sesion-05 (Lambda)
     *
     * @param message Mensaje guardado en DynamoDB con status=PENDING
     * @return        ID del evento asignado por EventBridge
     */
    @Override
    public String publishMessageReceived(Message message) {
        log.info("Publicando evento en EventBridge [messageId={}] [bus={}]",
                message.getMessageId(), eventBusName);

        // ── Construir el detail del evento (payload JSON) ─────────────────
        //
        // El "detail" es el payload custom del evento.
        // El Audit Lambda recibirá exactamente este JSON en event.getDetail()
        //
        String detail = buildEventDetail(message);

        // ── Construir la entrada del evento EventBridge ───────────────────
        //
        // PutEventsRequestEntry encapsula todos los campos de un evento.
        //   source:      identifica el servicio origen (para filtrado en Rules)
        //   detailType:  tipo del evento (para filtrado en Rules)
        //   detail:      payload JSON del evento (el Audit Lambda lo recibirá)
        //   eventBusName: bus personalizado del proyecto
        //
        PutEventsRequestEntry entry = PutEventsRequestEntry.builder()
                .source("msg-pipeline.processor")          // Filtro en Rule: source
                .detailType("MessageReceived")             // Filtro en Rule: detail-type
                .detail(detail)                            // Payload para el Audit Lambda
                .eventBusName(eventBusName)                // Bus personalizado
                .build();

        // ── Ejecutar PutEvents ────────────────────────────────────────────
        //
        // PutEvents puede enviar hasta 10 eventos en una sola llamada (batch).
        // Aquí enviamos solo 1 por invocación.
        //
        PutEventsRequest request = PutEventsRequest.builder()
                .entries(entry)
                .build();

        PutEventsResponse response = eventBridgeClient.putEvents(request);

        // ── Verificar errores ─────────────────────────────────────────────
        //
        // PutEvents NO lanza excepción si un evento individual falla.
        // Debemos verificar failedEntryCount y los errores por entrada.
        //
        if (response.failedEntryCount() > 0) {
            String errorCode = response.entries().get(0).errorCode();
            String errorMsg = response.entries().get(0).errorMessage();
            log.error("Error en PutEvents [messageId={}] [errorCode={}] [errorMsg={}]",
                    message.getMessageId(), errorCode, errorMsg);
            throw new RuntimeException("EventBridge PutEvents falló: " + errorMsg);
        }

        String eventId = response.entries().get(0).eventId();
        log.info("Evento publicado exitosamente [messageId={}] [eventId={}]",
                message.getMessageId(), eventId);

        return eventId;
    }

    // ── Método auxiliar ───────────────────────────────────────────────────

    /**
     * Construye el payload JSON del evento EventBridge (campo "detail").
     *
     * El Audit Lambda recibirá exactamente este JSON como el detail del evento.
     * Los campos deben ser suficientes para que el Audit Lambda pueda:
     *   1. Encontrar el ítem en DynamoDB (messageId)
     *   2. Actualizar el status a COMPLETED
     *   3. Publicar la notificación SNS
     */
    private String buildEventDetail(Message message) {
        Map<String, Object> detail = Map.of(
                "messageId", message.getMessageId(),
                "messageType", message.getMessageType() != null ? message.getMessageType() : "",
                "channel", message.getChannel() != null ? message.getChannel() : "",
                "recipientEmail", message.getRecipientEmail() != null ? message.getRecipientEmail() : "",
                "status", "PENDING",
                "createdAt", message.getCreatedAt() != null ? message.getCreatedAt() : "",
                "requestId", message.getRequestId() != null ? message.getRequestId() : "",
                "timestamp", Instant.now().toString()
        );

        try {
            return objectMapper.writeValueAsString(detail);
        } catch (JsonProcessingException e) {
            // Fallback: JSON mínimo con solo el messageId
            log.error("Error serializando detail del evento: {}", e.getMessage());
            return String.format(
                    "{\"messageId\":\"%s\",\"status\":\"PENDING\",\"timestamp\":\"%s\"}",
                    message.getMessageId(), Instant.now());
        }
    }
}
