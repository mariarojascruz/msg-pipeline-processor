package com.msgpipeline.processor.adapter.out.notification;

import com.msgpipeline.processor.domain.model.Message;
import com.msgpipeline.processor.domain.port.out.NotificationPort;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * Adaptador SNS para el Processor de Sesión 06.
 * @Profile("aws"): solo activo en Lambda.
 * Publica notificación de mensaje procesado en el tópico SNS.
 */
@Slf4j
@Component
@Profile("aws")
public class SnsNotificationAdapter implements NotificationPort {

    private SnsClient snsClient;

    @Value("${app.aws.sns-topic-arn:}")
    private String snsTopicArn;

    @Value("${app.aws.region:us-east-1}")
    private String region;

    @PostConstruct
    void init() {
        // Se construye aquí (no en un bloque static) para poder usar la región
        // configurada via @Value, que aún no está inyectada durante la carga de la clase
        snsClient = SnsClient.builder()
                .region(Region.of(region))
                .build();
    }

    @Override
    public void notificar(Message message) {
        if (snsTopicArn == null || snsTopicArn.isBlank()) {
            log.warn("SNS_TOPIC_ARN no configurado — omitiendo notificacion [messageId={}]",
                    message.getMessageId());
            return;
        }

        String mensajeTexto = String.format("""
                Mensaje Procesado — msg-pipeline Sesion 06 (Step Functions)
                
                Detalles:
                  ID:           %s
                  Tipo:         %s
                  Canal:        %s
                  Destinatario: %s
                  Status:       %s
                  Procesado:    %s
                
                El mensaje fue coordinado por Step Functions (msg-pipeline-workflow-sesion-06).
                
                Anku Academy — Especializacion Spring Boot + AWS Serverless
                """,
                message.getMessageId(),
                message.getMessageType(),
                message.getChannel(),
                message.getRecipientEmail(),
                message.getStatus(),
                message.getProcessedAt()
        );

        PublishResponse response = snsClient.publish(PublishRequest.builder()
                .topicArn(snsTopicArn)
                .subject("Mensaje Procesado — msg-pipeline Sesion 06")
                .message(mensajeTexto)
                .build());

        log.info("SNS Publish exitoso [messageId={}] [snsMessageId={}]",
                message.getMessageId(), response.messageId());
    }
}
