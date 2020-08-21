package net.coru.kloadgen.util;

public class PropsKeysHelper {

    public static final String MSG_KEY_PLACEHOLDER = "KEY";
    public static final String MSG_PLACEHOLDER = "MESSAGE";
    public static final String CUSTOMIZER = "customizer";
    public static final String EDITORS = "editors";
    public static final String KEYED_MESSAGE_KEY = "keyed.message";
    public static final String KEYED_MESSAGE_DEFAULT = "NO";
    public static final String MESSAGE_KEY_PLACEHOLDER_KEY = "message.key.placeholder";
    public static final String MESSAGE_VAL_PLACEHOLDER_KEY = "message.value.placeholder";

    public static final String AVRO_SUBJECT_NAME = "subject.name";
    public static final String AVRO_SCHEMA = "subject.schema";
    public static final String SCHEMA_PROPERTIES = "schema.properties";
    public static final String KEY_SUBJECT_NAME = "key.subject.name";
    public static final String KEY_SCHEMA = "key.schema";
    public static final String KEY_SCHEMA_PROPERTIES = "key.schema.properties";
    public static final String KEY_OR_VALUE = "keyorvalue";
    public static final KEYORVALUE KEYORVALUE_KEY = KEYORVALUE.Key;
    public static final KEYORVALUE KEYORVALUE_VALUE = KEYORVALUE.Value;

    public enum KEYORVALUE {
        Key,
        Value
    }
}
