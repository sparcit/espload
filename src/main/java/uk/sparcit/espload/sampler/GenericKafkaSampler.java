
package uk.sparcit.espload.sampler;

import static java.util.Collections.emptyList;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
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

@Slf4j
public class GenericKafkaSampler extends AbstractJavaSamplerClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private KafkaProducer<String, Object> producer;

    private String topic;

    private String msg_key_placeHolder;

    private boolean key_message_flag = false;

    private final StatelessRandomTool statelessRandomTool;

    private final BaseLoadGenerator generator;

    public GenericKafkaSampler() {
        generator = new AvroLoadGenerator();
        statelessRandomTool = new StatelessRandomTool();
    }

    @Override
    public Arguments getDefaultParameters() {

        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerKeysHelper.BOOTSTRAP_SERVERS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.ZOOKEEPER_SERVERS, ProducerKeysHelper.ZOOKEEPER_SERVERS_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.KAFKA_TOPIC_CONFIG, ProducerKeysHelper.KAFKA_TOPIC_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerKeysHelper.KEY_SERIALIZER_CLASS_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProducerKeysHelper.VALUE_SERIALIZER_CLASS_CONFIG_DEFAULT);
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
        defaultParameters.addArgument(ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG, ProducerKeysHelper.JAVA_SEC_AUTH_LOGIN_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.JAVA_SEC_KRB5_CONFIG, ProducerKeysHelper.JAVA_SEC_KRB5_CONFIG_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.SASL_KERBEROS_SERVICE_NAME, ProducerKeysHelper.SASL_KERBEROS_SERVICE_NAME_DEFAULT);
        defaultParameters.addArgument(ProducerKeysHelper.SASL_MECHANISM, ProducerKeysHelper.SASL_MECHANISM_DEFAULT);

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
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        props.put(ProducerConfig.ACKS_CONFIG, context.getParameter(ProducerConfig.ACKS_CONFIG));
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, context.getParameter(ProducerConfig.SEND_BUFFER_CONFIG));
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, context.getParameter(ProducerConfig.RECEIVE_BUFFER_CONFIG));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, context.getParameter(ProducerConfig.BATCH_SIZE_CONFIG));
        props.put(ProducerConfig.LINGER_MS_CONFIG, context.getParameter(ProducerConfig.LINGER_MS_CONFIG));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getParameter(ProducerConfig.BUFFER_MEMORY_CONFIG));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, context.getParameter(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, context.getParameter(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        props.put(ProducerKeysHelper.SASL_MECHANISM, context.getParameter(ProducerKeysHelper.SASL_MECHANISM));
        if (Objects.nonNull(JMeterContextService.getContext().getVariables().get(ProducerKeysHelper.SCHEMA_REGISTRY_URL))) {
            props.put(ProducerKeysHelper.SCHEMA_REGISTRY_URL, JMeterContextService.getContext().getVariables().get(ProducerKeysHelper.SCHEMA_REGISTRY_URL));
            generator.setUpGeneratorFromRegistry(
                JMeterContextService.getContext().getVariables().get(PropsKeysHelper.AVRO_SUBJECT_NAME),
                (List<FieldValueMapping>) JMeterContextService.getContext().getVariables().getObject(PropsKeysHelper.SCHEMA_PROPERTIES));
        } else {
            generator.setUpGenerator(
                JMeterContextService.getContext().getVariables().get(PropsKeysHelper.AVRO_SCHEMA),
                (List<FieldValueMapping>) JMeterContextService.getContext().getVariables().getObject(PropsKeysHelper.SCHEMA_PROPERTIES));
        }
        props.put(ProducerKeysHelper.ENABLE_AUTO_SCHEMA_REGISTRATION_CONFIG, "false");

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

        if (ProducerKeysHelper.FLAG_YES.equals(context.getParameter(PropsKeysHelper.KEYED_MESSAGE_KEY))) {
            key_message_flag = true;
            msg_key_placeHolder = UUID.randomUUID().toString();
        }
        topic = context.getParameter(ProducerKeysHelper.KAFKA_TOPIC_CONFIG);
        producer = new KafkaProducer<>(props);
    }

    @SuppressFBWarnings({"DM_DEFAULT_ENCODING", "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE"})
    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        SampleResult sampleResult = new SampleResult();
        sampleResult.sampleStart();
        JMeterContext jMeterContext = JMeterContextService.getContext();
        EnrichedRecord messageVal = generator.nextMessage();
        //noinspection unchecked
        List<HeaderMapping> kafkaHeaders = safeGetKafkaHeaders(jMeterContext);

        if (Objects.nonNull(messageVal)) {

            ProducerRecord<String, Object> producerRecord;
            try {
                if (key_message_flag) {
                    producerRecord = new ProducerRecord<>(topic, msg_key_placeHolder, messageVal.getGenericRecord());
//                    producerRecord = new ProducerRecord(topic, msg_key_placeHolder, messageVal);
                } else {
                    producerRecord = new ProducerRecord<>(topic, messageVal.getGenericRecord());
                }

                for (HeaderMapping kafkaHeader : kafkaHeaders) {
                    producerRecord.headers().add(kafkaHeader.getHeaderName(),
                        statelessRandomTool.generateRandom(kafkaHeader.getHeaderName(), kafkaHeader.getHeaderValue(),
                            10,
                            emptyList()).toString().getBytes(StandardCharsets.UTF_8));
                }

                sampleResult.setSamplerData(producerRecord.value().toString());

                Future<RecordMetadata> result = producer.send(producerRecord, (metadata, e) -> {
                    if (e != null) {
                        log.error("Send failed for record {}", producerRecord, e);
//                        getNewLogger().error("Send failed for record {}", producerRecord, e);
                        throw new ESPLoadException("Failed to sent message due ", e);
                    }
                });

                log.info("Send message to body: {}", producerRecord.value());
//                getNewLogger().info("Send message to body: {}", producerRecord.value());
                sampleResult.setResponseData(result.get().toString(), StandardCharsets.UTF_8.name());
                sampleResult.setSuccessful(true);
                sampleResult.sampleEnd();

            } catch (Exception e) {
                log.error("Failed to send message", e);
//                getNewLogger().error("Failed to send message", e);
                sampleResult.setResponseData(e.getMessage() != null ? e.getMessage() : "", StandardCharsets.UTF_8.name());
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

    @Override
    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }

    private List<HeaderMapping> safeGetKafkaHeaders(JMeterContext jMeterContext) {
        List<HeaderMapping> headerMappingList = new ArrayList<>();
        Object headers = jMeterContext.getSamplerContext().get(ProducerKeysHelper.KAFKA_HEADERS);
        if (null != headers) {
            headerMappingList.addAll((List) headers);
        }
        return headerMappingList;
    }
}
