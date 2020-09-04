
package uk.sparcit.espload.sampler;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import uk.sparcit.espload.exception.ESPLoadException;
import uk.sparcit.espload.loadgen.BaseLoadGenerator;
import uk.sparcit.espload.loadgen.impl.AvroLoadGenerator;
import uk.sparcit.espload.model.FieldValueMapping;
import uk.sparcit.espload.model.HeaderMapping;
import uk.sparcit.espload.serializer.EnrichedRecord;
import uk.sparcit.espload.util.StatelessRandomTool;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import uk.sparcit.espload.util.ProducerKeysHelper;
import uk.sparcit.espload.util.PropsKeysHelper;
import uk.sparcit.espload.util.SchemaRegistryKeyHelper;

@Slf4j
public class ConfluentKafkaSampler extends AbstractJavaSamplerClient implements Serializable {

    private static final long serialVersionUID = 1L;

//    private KafkaProducer<String, Object> producer;
    private KafkaProducer<Object, Object> producer;

    private String topic;

    private String messageKey;

    private boolean key_message_flag = false;

    private final ObjectMapper objectMapperJson = new ObjectMapper();

    private final StatelessRandomTool statelessRandomTool;
//    private final StatelessRandomTool keyRandom;

    private final BaseLoadGenerator generator;
    private final BaseLoadGenerator keyGen;

    public ConfluentKafkaSampler() {
        generator = new AvroLoadGenerator();
        keyGen = new AvroLoadGenerator();
        statelessRandomTool = new StatelessRandomTool();
//        keyRandom = new StatelessRandomTool();
    }

    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerKeysHelper.BOOTSTRAP_SERVERS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.ZOOKEEPER_SERVERS, ProducerKeysHelper.ZOOKEEPER_SERVERS_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.KAFKA_TOPIC_CONFIG, ProducerKeysHelper.KAFKA_TOPIC_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.COMPRESSION_TYPE_CONFIG, ProducerKeysHelper.COMPRESSION_TYPE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BATCH_SIZE_CONFIG, ProducerKeysHelper.BATCH_SIZE_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.LINGER_MS_CONFIG, ProducerKeysHelper.LINGER_MS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.BUFFER_MEMORY_CONFIG, ProducerKeysHelper.BUFFER_MEMORY_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.ACKS_CONFIG, ProducerKeysHelper.ACKS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.SEND_BUFFER_CONFIG, ProducerKeysHelper.SEND_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.RECEIVE_BUFFER_CONFIG, ProducerKeysHelper.RECEIVE_BUFFER_CONFIG_DEFAULT);
        defaultParameters.addArgument(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);
        defaultParameters.addArgument(PropsKeysHelper.KEYED_MESSAGE_KEY, PropsKeysHelper.KEYED_MESSAGE_DEFAULT);
        defaultParameters.addArgument(PropsKeysHelper.MESSAGE_KEY_PLACEHOLDER_KEY, PropsKeysHelper.MSG_KEY_PLACEHOLDER);
        defaultParameters.addArgument(PropsKeysHelper.MESSAGE_VAL_PLACEHOLDER_KEY, PropsKeysHelper.MSG_PLACEHOLDER);
        defaultParameters.addArgument(ProducerKeysHelper.KERBEROS_ENABLED, ProducerKeysHelper.FLAG_NO);
        defaultParameters.addArgument(ProducerKeysHelper.JAAS_ENABLED, ProducerKeysHelper.FLAG_NO);
        defaultParameters.addArgument(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG, ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.JAVA_SEC_KRB5_CONFIG, ProducerKeysHelper.JAVA_SEC_KRB5_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.SASL_KERBEROS_SERVICE_NAME, ProducerKeysHelper.SASL_KERBEROS_SERVICE_NAME_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.SASL_MECHANISM, ProducerKeysHelper.SASL_MECHANISM_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG, ProducerKeysHelper.ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.VALUE_NAME_STRATEGY, ProducerKeysHelper.TOPIC_NAME_STRATEGY);
        defaultParameters.addArgument(ProducerKeysHelper.SSL_ENABLED, ProducerKeysHelper.FLAG_NO);
        defaultParameters.addArgument(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "<Key Password>");
        defaultParameters.addArgument(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "<Keystore Location>");
        defaultParameters.addArgument(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "<Keystore Password>");
        defaultParameters.addArgument(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "<Truststore Location>");
        defaultParameters.addArgument(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "<Truststore Password>");

        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getParameter(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, context.getParameter(ProducerConfig.ACKS_CONFIG));
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, context.getParameter(ProducerConfig.SEND_BUFFER_CONFIG));
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, context.getParameter(ProducerConfig.RECEIVE_BUFFER_CONFIG));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, context.getParameter(ProducerConfig.BATCH_SIZE_CONFIG));
        props.put(ProducerConfig.LINGER_MS_CONFIG, context.getParameter(ProducerConfig.LINGER_MS_CONFIG));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getParameter(ProducerConfig.BUFFER_MEMORY_CONFIG));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, context.getParameter(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, context.getParameter(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        props.put(ProducerKeysHelper.SASL_MECHANISM, context.getParameter(ProducerKeysHelper.SASL_MECHANISM));
        if (Objects.nonNull(context.getParameter(ProducerKeysHelper.VALUE_NAME_STRATEGY))) {
            props.put(ProducerKeysHelper.VALUE_NAME_STRATEGY, context.getParameter(ProducerKeysHelper.VALUE_NAME_STRATEGY));
        }

        Properties properties = JMeterContextService.getContext().getProperties();
        if (Objects.nonNull(properties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL))) {
            props.put(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL,
                    properties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL));

            if (ProducerKeysHelper.FLAG_YES.equals(properties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG))) {
                if (SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE
                        .equals(properties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY))) {
                    props.put(BASIC_AUTH_CREDENTIALS_SOURCE,
                            properties.getProperty(BASIC_AUTH_CREDENTIALS_SOURCE));
                    props.put(USER_INFO_CONFIG, properties.getProperty(USER_INFO_CONFIG));
                } else if (SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BEARER_KEY.equals(properties.getProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY))) {
                    props.put(BEARER_AUTH_CREDENTIALS_SOURCE,
                            properties.getProperty(BEARER_AUTH_CREDENTIALS_SOURCE));
                    props.put(BEARER_AUTH_TOKEN_CONFIG, properties.getProperty(BEARER_AUTH_TOKEN_CONFIG));
                }
            }
            props.put(ProducerKeysHelper.ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG, context.getParameter(ProducerKeysHelper.ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG));

/*
            if (FLAG_YES.equals(context.getParameter(KEYED_MESSAGE_KEY))) {
                key_message_flag= true;
                messageKey = UUID.randomUUID().toString();
            }
*/

            if (Objects.nonNull(context.getJMeterVariables().get(PropsKeysHelper.AVRO_SUBJECT_NAME))) {
                generator.setUpGeneratorFromRegistry(
                        JMeterContextService.getContext().getVariables().get(PropsKeysHelper.AVRO_SUBJECT_NAME),
                        (List<FieldValueMapping>) JMeterContextService.getContext().getVariables().getObject(PropsKeysHelper.SCHEMA_PROPERTIES));
                if (Objects.nonNull(context.getJMeterVariables().get(PropsKeysHelper.KEY_SUBJECT_NAME))) {
                    key_message_flag = true;
                    keyGen.setUpGeneratorFromRegistry(
                            JMeterContextService.getContext().getVariables().get(PropsKeysHelper.KEY_SUBJECT_NAME),
                            (List<FieldValueMapping>) JMeterContextService.getContext().getVariables().getObject(PropsKeysHelper.KEY_SCHEMA_PROPERTIES));
                }

            } else if (Objects.nonNull(context.getJMeterVariables().get(PropsKeysHelper.AVRO_SCHEMA))) {
                generator.setUpGenerator(
                        JMeterContextService.getContext().getVariables().get(PropsKeysHelper.AVRO_SCHEMA),
                        (List<FieldValueMapping>) JMeterContextService.getContext().getVariables().getObject(PropsKeysHelper.SCHEMA_PROPERTIES));
                if (Objects.nonNull(context.getJMeterVariables().get(PropsKeysHelper.KEY_SCHEMA))) {
                    key_message_flag= true;
                    keyGen.setUpGenerator(
                            JMeterContextService.getContext().getVariables().get(PropsKeysHelper.KEY_SCHEMA),
                            (List<FieldValueMapping>) JMeterContextService.getContext().getVariables().getObject(PropsKeysHelper.KEY_SCHEMA_PROPERTIES));
                }
            } else {
                log.error("AVRO Subject Name could not be retrieved from Kafka Load Generator Config element nor \n");
                log.error("AVRO Schema retrieved from Schema File Load Generator Config element \n");
                throw new ESPLoadException("Kafka Load Generator Config or Schema File Load Generator Config elements unavailable in this test or configured incorrectly." +
                        " One of these configured correctly is required for the test.");
            }

        } else {
            log.error("The Schema Registry url could not be retrieved from Schema Registry Config Element. Ensure this is present and has a valid url ");
            throw new ESPLoadException("The Schema Registry url could not be retrieved from Schema Registry Config Element");

        }

        Iterator<String> parameters = context.getParameterNamesIterator();
        parameters.forEachRemaining(parameter -> {
            if (parameter.startsWith("_")) {
                props.put(parameter.substring(1), context.getParameter(parameter));
            }
        });

        if (ProducerKeysHelper.FLAG_YES.equalsIgnoreCase(context.getParameter(ProducerKeysHelper.SSL_ENABLED))) {

            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, context.getParameter(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, context.getParameter(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, context.getParameter(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        }

        if (ProducerKeysHelper.FLAG_YES.equalsIgnoreCase(context.getParameter(ProducerKeysHelper.KERBEROS_ENABLED))) {
            System.setProperty(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG, context.getParameter(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG));
            System.setProperty(ProducerKeysHelper.JAVA_SEC_KRB5_CONFIG, context.getParameter(ProducerKeysHelper.JAVA_SEC_KRB5_CONFIG));
            props.put(ProducerKeysHelper.SASL_KERBEROS_SERVICE_NAME, context.getParameter(ProducerKeysHelper.SASL_KERBEROS_SERVICE_NAME));
        }

        if (ProducerKeysHelper.FLAG_YES.equalsIgnoreCase(context.getParameter(ProducerKeysHelper.JAAS_ENABLED))) {
            if (StringUtils.contains(context.getParameter(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG), File.separatorChar)) {
                System.setProperty(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG, context.getParameter(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG));
            } else {
                props.put(SASL_JAAS_CONFIG, context.getParameter(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG));
            }
        }


        topic = context.getParameter(ProducerKeysHelper.KAFKA_TOPIC_CONFIG);
        producer = new KafkaProducer<>(props);
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        JMeterContext jMeterContext = JMeterContextService.getContext();
        EnrichedRecord messageVal = generator.nextMessage();
        EnrichedRecord messageKey;


            //noinspection unchecked
        List<HeaderMapping> kafkaHeaders = safeGetKafkaHeaders(jMeterContext);
        if (Objects.nonNull(messageVal)) {
//            ProducerRecord<String, Object> producerRecord;
            ProducerRecord<Object, Object> producerRecord;
            try {
                if (key_message_flag) {
                    messageKey = keyGen.nextMessage();
                    producerRecord = new ProducerRecord<>(topic, messageKey.getGenericRecord(), messageVal.getGenericRecord());
                } else {
                    producerRecord = new ProducerRecord<>(topic, messageVal.getGenericRecord());
                }
                Map<String, String> jsonTypes = new HashMap<>();
                jsonTypes.put("contentType", "java.lang.String");
                producerRecord.headers()
                    .add("spring_json_header_types", objectMapperJson.writeValueAsBytes(jsonTypes));
                for (HeaderMapping kafkaHeader : kafkaHeaders) {
                    producerRecord.headers().add(kafkaHeader.getHeaderName(),
                        objectMapperJson.writeValueAsBytes(
                            statelessRandomTool
                                .generateRandom(kafkaHeader.getHeaderName(), kafkaHeader.getHeaderValue(), 10, Collections.emptyList())));
                }

                sampleResult.setSamplerData(producerRecord.value().toString());

                Future<RecordMetadata> result = producer.send(producerRecord, (metadata, e) -> {
                    if (e != null) {
                        log.error("Send failed for record {}", producerRecord, e);
//                        getNewLogger().error("Send failed for record {}", producerRecord, e);
                        throw new ESPLoadException("Failed to sent message due ", e);
                    }
                });

                log.info("Message send with key::value: {}::{}", producerRecord.key(),producerRecord.value());
//                getNewLogger().info("Send message to body: {}", producerRecord.value());
                sampleResult.setResponseData(result.get().toString(), StandardCharsets.UTF_8.name());
                sampleResult.setSuccessful(true);
                sampleResult.sampleEnd();

            } catch (Exception e) {
                log.error("Failed to send message", e);
//                getNewLogger().error("Failed to send message", e);
                sampleResult.setResponseData(e.getMessage(), StandardCharsets.UTF_8.name());
                sampleResult.setSuccessful(false);
                sampleResult.sampleEnd();
            }
        } else {
            log.error("Failed to Generate message");
//            getNewLogger().error("Failed to Generate message");
            sampleResult.setResponseData("Failed to Generate message", StandardCharsets.UTF_8.name());
            sampleResult.setSuccessful(false);
            sampleResult.sampleEnd();
        }
        return sampleResult;
    }

    private List<HeaderMapping> safeGetKafkaHeaders(JMeterContext jMeterContext) {
        List<HeaderMapping> headerMappingList = new ArrayList<>();
        Object headers = jMeterContext.getSamplerContext().get(ProducerKeysHelper.KAFKA_HEADERS);
        if (null != headers) {
            headerMappingList.addAll((List) headers);
        }
        return headerMappingList;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }
}
