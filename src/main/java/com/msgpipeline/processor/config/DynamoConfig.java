package com.msgpipeline.processor.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * =========================================================================
 * CLASE: DynamoConfig — Configuración del cliente DynamoDB
 * CAPA: Infraestructura — Configuración
 * ARQUITECTURA: Hexagonal
 * =========================================================================
 *
 * @Profile("aws"): Solo crea estos beans cuando el perfil 'aws' está activo.
 *   En perfil 'local', DynamoDbClient NO se crea (no se necesita).
 *   InMemoryMessageRepository no usa DynamoDbClient.
 *
 * CREDENCIALES AWS:
 *   NO hardcodear access keys. El Lambda obtiene credenciales automáticamente
 *   del IAM Role asignado a la función Lambda.
 *   AWS SDK v2 busca credenciales en este orden:
 *     1. Variables de entorno (AWS_ACCESS_KEY_ID, etc.) → NO usar en Lambda
 *     2. IAM Role del Lambda → ✅ USAR SIEMPRE EN PRODUCCIÓN
 *     3. Perfil ~/.aws/credentials → solo para desarrollo local con CLI
 *
 * @Bean tableName: exponemos el tableName como bean String para que
 *   DynamoMessageRepository pueda inyectarlo con @Autowired.
 *   Alternativa: usar @Value directamente en DynamoMessageRepository.
 * =========================================================================
 */
@Slf4j
@Configuration
@Profile("aws")
@RequiredArgsConstructor
public class DynamoConfig {

    @Value("${app.aws.dynamodb-table:msg-pipeline-messages}")
    private String dynamodbTable;

    @Value("${app.aws.region:us-east-1}")
    private String region;

    /**
     * Cliente DynamoDB SDK v2.
     *
     * DynamoDbClient es thread-safe y debe ser un Singleton.
     * Spring gestiona el ciclo de vida — se crea una sola vez (cold start).
     * En warm starts, se reutiliza el mismo cliente.
     *
     * IMPORTANTE: No llamar a DynamoDbClient.builder().build() en cada
     * invocación del handler — es costoso y aumenta la latencia.
     */
    @Bean
    public DynamoDbClient dynamoDbClient() {
        log.info("Inicializando DynamoDbClient [region={}] [tabla={}]", region, dynamodbTable);
        return DynamoDbClient.builder()
                .region(Region.of(region))
                // Credenciales: IAM Role del Lambda (automático)
                // No especificamos credenciales explícitas → AWS SDK las toma del rol
                .build();
    }

    /**
     * Nombre de la tabla DynamoDB expuesto como bean.
     * DynamoMessageRepository lo inyecta via @RequiredArgsConstructor.
     */
    @Bean
    public String tableName() {
        log.info("Tabla DynamoDB configurada: {}", dynamodbTable);
        return dynamodbTable;
    }
}
