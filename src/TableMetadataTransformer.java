import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMetadataTransformer {

  private static String PRIMARY_KEY_PREFIX = "primaryKey";
  private static String ATTRIBUTE_KEY_PREFIX = "attribute";

  private List<String> tableAttributeStorePath;
  private String tableName;




  public static List<String> getPrimaryKeysFromPrimaryKeyTuple(Tuple valueTuple) {
    List<String> res = new ArrayList<>();

    for (int i = 0; i < valueTuple.size(); i++) {
      res.add(valueTuple.getString(i));
    }

    return res;
  }

  public static Tuple getTablePrimaryKeyTuple(String tableName) {
    return new Tuple().add(PRIMARY_KEY_PREFIX).add(tableName);
  }

  public FDBKVPair getAttributeKVPair(String attributeName, AttributeType attributeType) {
    Tuple keyTuple = getTableAttributeKeyTuple(attributeName);
    Tuple valueTuple = new Tuple().add(attributeType.ordinal());

    return new FDBKVPair(tableAttributeStorePath, keyTuple, valueTuple);
  }

  public static Tuple getTableAttributeKeyTuple(String attributeName) {
    return new Tuple().add(ATTRIBUTE_KEY_PREFIX).add(attributeName);
  }

  public static boolean isKeyTablePrimaryKey(Tuple t) {
    return !t.isEmpty() && t.getString(0).equals(PRIMARY_KEY_PREFIX);
  }

  public static boolean isKeyTableAttributeKey(Tuple t) {
    return !t.isEmpty() && t.getString(0).equals(ATTRIBUTE_KEY_PREFIX);
  }

  public TableMetadataTransformer(String tableName) {
    tableAttributeStorePath = new ArrayList<>();
    tableAttributeStorePath.addAll(DBConf.METADATA_STORE_PATH);
    tableAttributeStorePath.add(tableName);
    tableAttributeStorePath.add(DBConf.METADATA_TABLE_ATTR_STORE);

    this.tableName = tableName;
  }

  public List<String> getTableAttributeStorePath() {
    return tableAttributeStorePath;
  }

  public void setTableAttributeStorePath(List<String> tableAttributeStorePath) {
    this.tableAttributeStorePath = tableAttributeStorePath;
  }

  public TableMetadata deserialize(List<FDBKVPair> pairs) {
    TableMetadata tableMetadata = new TableMetadata();
    List<String> primaryKeys = new ArrayList<>();
    for (FDBKVPair kv : pairs) {
      Tuple key = kv.getKey();
      Tuple value = kv.getValue();

      if (key.getString(0).equals(ATTRIBUTE_KEY_PREFIX)) {
        String attrName = key.getString(1);
        tableMetadata.addAttribute(attrName,
            AttributeType.values() [Math.toIntExact((Long) value.get(0))]);
      } else if (key.getString(0).equals(PRIMARY_KEY_PREFIX)) {
        primaryKeys = getPrimaryKeysFromPrimaryKeyTuple(value);
      }
    }

    tableMetadata.setPrimaryKeys(primaryKeys);
    return tableMetadata;
  }

  public List<FDBKVPair> serialize(TableMetadata table) {
    List<FDBKVPair> res = new ArrayList<>();

    HashMap<String, AttributeType> attributeMap = table.getAttributes();
    // prepare kv pairs for Attribute
    for (Map.Entry<String, AttributeType> kv : attributeMap.entrySet()) {
      Tuple keyTuple = getTableAttributeKeyTuple(kv.getKey());
      Tuple valueTuple = new Tuple().add(kv.getValue().ordinal());

      res.add(new FDBKVPair(tableAttributeStorePath, keyTuple, valueTuple));
    }

    Tuple tablePrimKey = getTablePrimaryKeyTuple(tableName);
    Tuple tablePrimValue = new Tuple();
    for (String pk : table.getPrimaryKeys()) {
      tablePrimValue = tablePrimValue.add(pk);
    }
    res.add(new FDBKVPair(tableAttributeStorePath, tablePrimKey, tablePrimValue));

    return res;
  }
}
