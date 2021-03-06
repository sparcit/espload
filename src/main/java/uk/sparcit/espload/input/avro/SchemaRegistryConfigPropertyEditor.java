package uk.sparcit.espload.input.avro;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_TOKEN_CONFIG;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorSupport;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.*;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import uk.sparcit.espload.model.PropertyMapping;
import uk.sparcit.espload.util.PropsKeysHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.gui.ClearGui;
import org.apache.jmeter.gui.GuiPackage;
import org.apache.jmeter.testbeans.gui.*;
import org.apache.jmeter.threads.JMeterContextService;
//import org.apache.jmeter.visualizers.SearchTextExtension;
import uk.sparcit.espload.util.ProducerKeysHelper;
import uk.sparcit.espload.util.SchemaRegistryKeyHelper;


@Slf4j
public class SchemaRegistryConfigPropertyEditor extends PropertyEditorSupport implements ActionListener, TestBeanPropertyEditor, ClearGui {

    private final JTextField schemaRegistryUrl = new JTextField();

    private final JButton testSchemaRepoBtn = new JButton("Test Registry");

//    private final JEditorPane listofSchemas = new JEditorPane();

    private final JPanel panel = new JPanel();

    private PropertyDescriptor propertyDescriptor;

    public SchemaRegistryConfigPropertyEditor() {
        this.init();
    }

    public SchemaRegistryConfigPropertyEditor(Object source) {
        super(source);
        this.init();
        this.setValue(source);
    }

    public SchemaRegistryConfigPropertyEditor(PropertyDescriptor descriptor) {
        super(descriptor);
        this.propertyDescriptor = descriptor;
        this.init();
    }

    private void init() {
        panel.setLayout(new BorderLayout());

        panel.add(schemaRegistryUrl);

        panel.add(testSchemaRepoBtn, BorderLayout.AFTER_LINE_ENDS);
        this.testSchemaRepoBtn.addActionListener(this);
//        panel.add(listofSchemas, BorderLayout.SOUTH);
    }

    @Override
    public String getAsText() {
        return this.schemaRegistryUrl.getText();
    }

    @Override
    public Component getCustomEditor() {
        return this.panel;
    }

    @Override
    public void setAsText(String text) throws IllegalArgumentException {
       throw new UnsupportedOperationException("Operation not Supported:" + text);
    }

    @SneakyThrows
    @Override
    public void setValue(Object value) {
        this.schemaRegistryUrl.setText(value.toString());
        propertyDescriptor.setValue("schemaRegistryUrl", value.toString());
    }

    public void setSchemaRegistryUrl(String schemaUrl) {
        this.schemaRegistryUrl.setText(schemaUrl);
        propertyDescriptor.setValue("schemaRegistryUrl", schemaUrl);
    }

    @SneakyThrows
    @Override
    public Object getValue() {
        return schemaRegistryUrl.getText();
    }

    @Override
    public boolean supportsCustomEditor() {
        return true;
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent) {

        try {
            //Get current test GUI component
            TestBeanGUI testBeanGUI = (TestBeanGUI) GuiPackage.getInstance().getCurrentGui();
            Field customizer = TestBeanGUI.class.getDeclaredField(PropsKeysHelper.CUSTOMIZER);
            customizer.setAccessible(true);

            //From TestBeanGUI retrieve Bean Customizer as it includes all editors like ClassPropertyEditor, TableEditor
            GenericTestBeanCustomizer testBeanCustomizer = (GenericTestBeanCustomizer) customizer.get(testBeanGUI);
            Field editors = GenericTestBeanCustomizer.class.getDeclaredField(PropsKeysHelper.EDITORS);
            editors.setAccessible(true);

            //Retrieve TableEditor and set all fields with default values to it
            PropertyEditor[] propertyEditors = (PropertyEditor[]) editors.get(testBeanCustomizer);
            Map<String, String> schemaProperties = new HashMap<>();
            TextAreaEditor textAreaEditor = new TextAreaEditor();
            for (PropertyEditor propertyEditor : propertyEditors) {
                if (propertyEditor instanceof TableEditor) {
                    //noinspection unchecked
                    schemaProperties = fromListToPropertiesMap((List<PropertyMapping>) propertyEditor.getValue());
                }
                if (propertyEditor instanceof TextAreaEditor) {
                    //noinspection unchecked
                    textAreaEditor = (TextAreaEditor) propertyEditor;
                }
            }
            Map<String, String> originals = new HashMap<>();
            originals.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl.getText());
            if (ProducerKeysHelper.FLAG_YES.equalsIgnoreCase(schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG))) {
                JMeterContextService.getContext().getProperties().setProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG, ProducerKeysHelper.FLAG_YES);
                if (SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE.equalsIgnoreCase(schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY))) {
                    JMeterContextService.getContext().getProperties()
                        .setProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY, SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE);

                    originals.put(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
                    originals.put(USER_INFO_CONFIG,
                        schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_USERNAME_KEY) + ":" + schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_PASSWORD_KEY));
                }
            }
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(getAsText(), 1000, originals);

            List<String> subjects = new ArrayList<>(schemaRegistryClient.getAllSubjects());
            JMeterContextService.getContext().getProperties().setProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_URL, schemaRegistryUrl.getText());
            JMeterContextService.getContext().getProperties().setProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_SUBJECTS, StringUtils.join(subjects, ","));
//            listofSchemas.setText(StringUtils.join(subjects, "\n"));
            textAreaEditor.setAsText(StringUtils.join(subjects, "\n"));
            if (ProducerKeysHelper.FLAG_YES.equalsIgnoreCase(schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG))) {
                JMeterContextService.getContext().getProperties().setProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_FLAG, ProducerKeysHelper.FLAG_YES);
                if (SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE.equalsIgnoreCase(schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY))) {
                    JMeterContextService.getContext().getProperties()
                        .setProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY, SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BASIC_TYPE);
                    JMeterContextService.getContext().getProperties().setProperty(BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
                    JMeterContextService.getContext().getProperties().setProperty(USER_INFO_CONFIG,
                        schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_USERNAME_KEY) + ":" + schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_PASSWORD_KEY));
                } else if (SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BEARER_TYPE.equalsIgnoreCase(schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY))) {
                    JMeterContextService.getContext().getProperties()
                        .setProperty(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_KEY, SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BEARER_TYPE);
                    JMeterContextService.getContext().getProperties().setProperty(BEARER_AUTH_CREDENTIALS_SOURCE, "STATIC_TOKEN");
                    JMeterContextService.getContext().getProperties()
                        .setProperty(BEARER_AUTH_TOKEN_CONFIG, schemaProperties.get(SchemaRegistryKeyHelper.SCHEMA_REGISTRY_AUTH_BEARER_KEY));
                }
            }
            JOptionPane.showMessageDialog(null, "Successful contacting Schema Registry at : " + schemaRegistryUrl.getText() +
                    "\n Number of subjects in the Registry : " + subjects.size(), "Successful connection to Schema Registry",
                JOptionPane.INFORMATION_MESSAGE);
        } catch (IOException | RestClientException | NoSuchFieldException | IllegalAccessException e) {
            JOptionPane
                .showMessageDialog(null, "Failed retrieve schema properties : " + e.getMessage(), "ERROR: Failed to retrieve properties!",
                    JOptionPane.ERROR_MESSAGE);
            log.error(e.getMessage(), e);
        }

    }

    @Override
    public void setDescriptor(PropertyDescriptor propertyDescriptor) {
        this.propertyDescriptor = propertyDescriptor;
    }

    @Override
    public void clearGui() {
        this.schemaRegistryUrl.setText("");
//        this.listofSchemas.setText("");
    }

    private Map<String, String> fromListToPropertiesMap(List<PropertyMapping> schemaProperties) {
        Map<String, String> propertiesMap = new HashMap<>();
        for (PropertyMapping property : schemaProperties) {
            propertiesMap.put(property.getPropertyName(), property.getPropertyValue());
        }
        return propertiesMap;
    }
}
