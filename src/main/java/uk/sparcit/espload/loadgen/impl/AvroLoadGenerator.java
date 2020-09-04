package uk.sparcit.espload.loadgen.impl;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import uk.sparcit.espload.exception.ESPLoadException;
import uk.sparcit.espload.loadgen.BaseLoadGenerator;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.processor.AvroSchemaProcessor;
import uk.sparcit.espload.serializer.EnrichedRecord;
import org.apache.avro.Schema;
import org.apache.jmeter.threads.JMeterContextService;
import uk.sparcit.espload.util.ProducerKeysHelper;
import uk.sparcit.espload.util.SchemaRegistryKeyHelper;

@Slf4j
public class AvroLoadGenerator implements BaseLoadGenerator {

  private SchemaRegistryClient schemaRegistryClient;

  private SchemaMetadata metadata;

  private final AvroSchemaProcessor avroSchemaProcessor;

  public AvroLoadGenerator() {
    avroSchemaProcessor = new AvroSchemaProcessor();
  }

  public void setUpGeneratorFromRegistry(String avroSchemaName, List<FieldValueMapping> fieldExprMappings) {
    try {
      this.avroSchemaProcessor.processSchema(retrieveSchema(avroSchemaName), metadata, fieldExprMappings);
    } catch (Exception exc){
      log.error("Please make sure that properties data type and expression function return type are compatible with each other", exc);
      throw new ESPLoadException(exc);
    }
  }

  public void setUpGenerator(String schema, List<FieldValueMapping> fieldExprMappings) {
    try {
      SchemaMetadata metadata = new SchemaMetadata(1, 1, schema);
      Schema.Parser parser = new Schema.Parser();
      this.avroSchemaProcessor.processSchema(parser.parse(schema), metadata, fieldExprMappings);
    } catch (Exception exc){
      log.error("Please make sure that properties data type and expression function return type are compatible with each other", exc);
      throw new ESPLoadException(exc);
    }
  }

  public EnrichedRecord nextMessage() {
    return avroSchemaProcessor.next();
  }

  private Schema retrieveSchema(String avroSchemaName) throws IOException, RestClientException {

    Map<String, String> originals = new HashMap<>();
    Properties ctxProperties = JMeterContextService.getContext().getProperties();

    if (Objects.nonNull(ctxProperties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL))) {
      originals.put(SCHEMA_REGISTRY_URL_CONFIG, ctxProperties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL));

      if (ProducerKeysHelper.FLAG_YES.equals(ctxProperties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG))) {
        if (SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE
            .equals(ctxProperties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY))) {
          originals.put(BASIC_AUTH_CREDENTIALS_SOURCE,
              ctxProperties.getProperty(BASIC_AUTH_CREDENTIALS_SOURCE));
          originals.put(USER_INFO_CONFIG, ctxProperties.getProperty(USER_INFO_CONFIG));
        } else {
          originals.put(BEARER_AUTH_CREDENTIALS_SOURCE,
              ctxProperties.getProperty(BEARER_AUTH_CREDENTIALS_SOURCE));
          originals.put(BEARER_AUTH_TOKEN_CONFIG, ctxProperties.getProperty(BEARER_AUTH_TOKEN_CONFIG));
        }
      }
      schemaRegistryClient = new CachedSchemaRegistryClient(originals.get(SCHEMA_REGISTRY_URL_CONFIG), 1000, originals);
    } else {
      throw new ESPLoadException("No Schema Registry URL in System");
    }
    return getSchemaBySubject(avroSchemaName);
  }

  private Schema getSchemaBySubject(String avroSubjectName) throws IOException, RestClientException {
    metadata = schemaRegistryClient.getLatestSchemaMetadata(avroSubjectName);
    return schemaRegistryClient.getById(metadata.getId());
  }
}
