package uk.sparcit.espload.config.avroserialized;

import static org.assertj.core.api.Assertions.assertThat;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.util.Locale;
import org.apache.jmeter.util.JMeterUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AvroSerializedConfigElementBeanInfoTest {

  private static final String AVRO_SUBJECT = "avroSubject";

  private static final String SCHEMA_PROPERTIES = "schemaProperties";

  private AvroSerializedConfigElementBeanInfo avroSerializedConfigElementBeanInfo;

  @BeforeEach
  public void setUp() throws IntrospectionException {
    JMeterUtils.setLocale(Locale.ENGLISH);
    avroSerializedConfigElementBeanInfo = new AvroSerializedConfigElementBeanInfo();
  }

  @Test
  public void shouldGenerateElements() {
    PropertyDescriptor[] propertyDescriptors = avroSerializedConfigElementBeanInfo.getPropertyDescriptors();
    assertThat(propertyDescriptors).hasSize(2);
    assertThat(propertyDescriptors[0].getName()).isEqualTo(AVRO_SUBJECT);
    assertThat(propertyDescriptors[1].getName()).isEqualTo(SCHEMA_PROPERTIES);
  }
}