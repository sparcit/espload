package uk.sparcit.espload.input;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import uk.sparcit.espload.exception.ESPLoadException;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.processor.AvroSchemaProcessor;
import uk.sparcit.espload.serializer.EnrichedRecord;
import org.apache.avro.SchemaBuilder;
import org.apache.groovy.util.Maps;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.util.JMeterUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.lanwen.wiremock.ext.WiremockResolver;
import ru.lanwen.wiremock.ext.WiremockResolver.Wiremock;
import ru.lanwen.wiremock.ext.WiremockUriResolver;
import uk.sparcit.espload.util.ProducerKeysHelper;
import uk.sparcit.espload.util.SchemaRegistryKeyHelper;

@ExtendWith({
    WiremockResolver.class,
    WiremockUriResolver.class
})
class AvroSchemaProcessorTest {

  @BeforeEach
  public void setUp() {
    File file = new File("src/test/resources");
    String absolutePath = file.getAbsolutePath();
    JMeterUtils.loadJMeterProperties(absolutePath + "/kloadgen.properties");
    JMeterContext jmcx = JMeterContextService.getContext();
    jmcx.setVariables(new JMeterVariables());
    JMeterUtils.setLocale(Locale.ENGLISH);
  }

  @Test
  public void textAvroSchemaProcessor(@Wiremock WireMockServer server) throws IOException, RestClientException, ESPLoadException {
    List<FieldValueMapping> fieldValueMappingList = asList(
        new FieldValueMapping("name", "string", 0, "Jose"),
        new FieldValueMapping("age", "int", 0, "43"));

    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG, ProducerKeysHelper.FLAG_YES);
    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY, SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE);
    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL, "http://localhost:" + server.port());
    JMeterContextService.getContext().getProperties().put(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
    JMeterContextService.getContext().getProperties().put(USER_INFO_CONFIG, "foo:foo");

    AvroSchemaProcessor avroSchemaProcessor = new AvroSchemaProcessor();
    avroSchemaProcessor.processSchema(SchemaBuilder.builder().record("testing").fields().requiredString("name").optionalInt("age").endRecord(),
        new SchemaMetadata(1, 1, ""), fieldValueMappingList);
    EnrichedRecord message = avroSchemaProcessor.next();
    assertThat(message).isNotNull();
    assertThat(message).isInstanceOf(EnrichedRecord.class);
    assertThat(message.getGenericRecord()).isNotNull();
    assertThat(message.getGenericRecord()).hasFieldOrPropertyWithValue("values", asList("Jose", 43).toArray());

  }

  @Test
  public void textAvroSchemaProcessorArrayMap(@Wiremock WireMockServer server) throws IOException, RestClientException, ESPLoadException {
    List<FieldValueMapping> fieldValueMappingList = Collections.singletonList(
        new FieldValueMapping("values[4]", "string-map-array", 2, "n:1, t:2"));

    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG, ProducerKeysHelper.FLAG_YES);
    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY, SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE);
    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL, "http://localhost:" + server.port());
    JMeterContextService.getContext().getProperties().put(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
    JMeterContextService.getContext().getProperties().put(USER_INFO_CONFIG, "foo:foo");

    AvroSchemaProcessor avroSchemaProcessor = new AvroSchemaProcessor();
    avroSchemaProcessor.processSchema(SchemaBuilder
        .builder()
        .record("arrayMap")
        .fields()
        .name("values")
        .type()
        .array()
        .items()
        .type(SchemaBuilder
            .builder()
            .map()
            .values()
            .stringType()
            .getValueType())
        .noDefault()
        .endRecord(),
        new SchemaMetadata(1, 1, ""),
        fieldValueMappingList);

    EnrichedRecord message = avroSchemaProcessor.next();
    assertThat(message).isNotNull();
    assertThat(message).isInstanceOf(EnrichedRecord.class);
    assertThat(message.getGenericRecord()).isNotNull();
    assertThat(message.getGenericRecord().get("values")).isInstanceOf(List.class);
    List<Map<String, Object>> result = (List<Map<String, Object>>) message.getGenericRecord().get("values");
    assertThat(result).hasSize(4);
    assertThat(result).contains(Maps.of("n","1","t","2"), Maps.of("n","1","t","2"));
  }
}