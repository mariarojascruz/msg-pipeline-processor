package com.msgpipeline.processor.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Configuracion de DynamoDB para perfil 'aws'. */
@Slf4j
@Configuration
@Profile("aws")
public class DynamoConfig {

    @Value("${app.aws.dynamodb-table:msg-pipeline-messages}")
    private String dynamodbTable;

    @Value("${app.aws.region:us-east-1}")
    private String region;

    @Bean
    public DynamoDbClient dynamoDbClient() {
        log.info("Inicializando DynamoDbClient [region={}] [tabla={}]", region, dynamodbTable);
        return DynamoDbClient.builder()
                .region(Region.of(region))
                .build();
    }

    @Bean
    public String tableName() {
        return dynamodbTable;
    }
}
