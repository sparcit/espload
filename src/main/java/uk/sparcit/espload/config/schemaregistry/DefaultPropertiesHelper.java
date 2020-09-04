package uk.sparcit.espload.config.schemaregistry;

import com.google.common.collect.ImmutableList;
import java.util.List;
import uk.sparcit.espload.model.PropertyMapping;
import uk.sparcit.espload.util.ProducerKeysHelper;
import uk.sparcit.espload.util.SchemaRegistryKeyHelper;

class DefaultPropertiesHelper {

  protected static final List<PropertyMapping> DEFAULTS = ImmutableList.of(
      PropertyMapping.builder().propertyName(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG).propertyValue(ProducerKeysHelper.FLAG_NO).build(),
      PropertyMapping.builder().propertyName(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY).propertyValue(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE).build(),
      PropertyMapping.builder().propertyName(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_USERNAME_KEY).propertyValue(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_USERNAME_DEFAULT).build(),
      PropertyMapping.builder().propertyName(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_PASSWORD_KEY).propertyValue(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_PASSWORD_DEFAULT).build(),
      PropertyMapping.builder().propertyName(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BEARER_KEY).propertyValue(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BEARER_DEFAULT).build());
}
