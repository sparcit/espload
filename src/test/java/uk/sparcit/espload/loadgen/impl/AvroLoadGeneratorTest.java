package uk.sparcit.espload.loadgen.impl;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.File;
import java.util.List;
import java.util.Locale;
import uk.sparcit.espload.exception.ESPLoadException;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.serializer.EnrichedRecord;
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
import uk.sparcit.espload.util.SchemaRegistryKeyHelper;

@ExtendWith({
    WiremockResolver.class,
    WiremockUriResolver.class
})
class AvroLoadGeneratorTest {

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
  public void testAvroLoadGenerator(@Wiremock WireMockServer server) throws ESPLoadException {

    List<FieldValueMapping> fieldValueMappingList = asList(
        new FieldValueMapping("Name", "string", 0, "Jose"),
        new FieldValueMapping("Age", "int", 0, "43"));

    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL, "http://localhost:" + server.port());
    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_USERNAME_KEY, "foo");
    JMeterContextService.getContext().getProperties().put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_PASSWORD_KEY, "foo");

    AvroLoadGenerator avroLoadGenerator = new AvroLoadGenerator();
    avroLoadGenerator.setUpGeneratorFromRegistry("avroSubject", fieldValueMappingList);
    Object message = avroLoadGenerator.nextMessage();
    assertThat(message).isNotNull();
    assertThat(message).isInstanceOf(EnrichedRecord.class);

    EnrichedRecord enrichedRecord = (EnrichedRecord) message;
    assertThat(enrichedRecord.getGenericRecord()).isNotNull();
    assertThat(enrichedRecord.getGenericRecord()).hasFieldOrPropertyWithValue("values", asList("Jose", 43).toArray());
  }
}