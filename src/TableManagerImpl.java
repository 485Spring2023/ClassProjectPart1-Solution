import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * TableManagerImpl implements interfaces in {#TableManager}. You should put your implementation
 * in this class.
 */
public class TableManagerImpl implements TableManager{

  private Database db;
  private List<String> tableNames = new ArrayList<>();

  public TableManagerImpl() {
    db = FDBHelper.initialization();
  }

  @Override
  public StatusCode createTable(String tableName, String[] attributeNames, AttributeType[] attributeType,
                         String[] primaryKeyAttributeNames) {
    // your code
    // First, check if table already exists
    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);

    if (FDBHelper.doesSubdirectoryExists(db, tableSubdirectory)) {
      return StatusCode.TABLE_ALREADY_EXISTS;
    }

    if (attributeNames == null || attributeType == null || primaryKeyAttributeNames == null) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    if (attributeNames.length == 0 || attributeType.length == 0 || attributeType.length != attributeNames.length) {
      return StatusCode.TABLE_CREATION_ATTRIBUTE_INVALID;
    }

    if (primaryKeyAttributeNames.length == 0) {
      return StatusCode.TABLE_CREATION_NO_PRIMARY_KEY;
    }

    TableMetadata tblMetadata = new TableMetadata();
    HashMap<String, AttributeType> attributes = new HashMap<>();
    for (int i = 0; i < attributeNames.length; i++) {
      attributes.put(attributeNames[i], attributeType[i]);
    }

    tblMetadata.setAttributes(attributes);
    StatusCode isPrimaryKeyAdded = tblMetadata.setPrimaryKeys(Arrays.asList(primaryKeyAttributeNames));
    if (isPrimaryKeyAdded != StatusCode.SUCCESS) {
      return StatusCode.TABLE_CREATION_PRIMARY_KEY_NOT_FOUND;
    }



    // persist the creation
    Transaction tx = FDBHelper.openTransaction(db);

    FDBHelper.createOrOpenSubspace(db, Collections.singletonList(tableName));

    List<FDBKVPair> pairs = new TableMetadataTransformer(tableName).serialize(tblMetadata);
    FDBHelper.persistFDBKVPairs(db, tx, pairs);
    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode deleteTable(String tableName) {
    // your code
    // First, check if table exists
    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);
    if (!FDBHelper.doesSubdirectoryExists(db, tableSubdirectory)) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    Transaction tx = FDBHelper.openTransaction(db);
    FDBHelper.removeSubspace(db, tx, tableSubdirectory);
    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public HashMap<String, TableMetadata> listTables() {
    // your code
    HashMap<String, TableMetadata> res = new HashMap<>();
    List<String> existingTableNames = FDBHelper.getAllDirectSubspaceName(db);

    for (String tblName : existingTableNames) {
      TableMetadataTransformer tblTransformer = new TableMetadataTransformer(tblName);
      List<String> tblAttributeDirPath = tblTransformer.getTableAttributeStorePath();
      List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(db,
          tblAttributeDirPath);
      TableMetadata tblMetadata = tblTransformer.deserialize(kvPairs);
      res.put(tblName, tblMetadata);
    }
    return res;
  }

  @Override
  public StatusCode addAttribute(String tableName, String attributeName, AttributeType attributeType) {
    // your code
    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);
    if (!FDBHelper.doesSubdirectoryExists(db, tableSubdirectory)) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    // retrieve attributes of the table, check if attributes exists
    TableMetadataTransformer tblTransformer = new TableMetadataTransformer(tableName);
    List<String> tblAttributeDirPath = tblTransformer.getTableAttributeStorePath();
    TableMetadata tblMetadata = tblTransformer.deserialize(FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tblAttributeDirPath));

    HashMap<String, AttributeType> attributes = tblMetadata.getAttributes();
    if (attributes.containsKey(attributeName)) {
      return StatusCode.ATTRIBUTE_ALREADY_EXISTS;
    }

    Transaction tx = FDBHelper.openTransaction(db);

    FDBHelper.setFDBKVPair(db, tx, tblTransformer.getAttributeKVPair(attributeName, attributeType));

    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAttribute(String tableName, String attributeName) {
    // your code

    List<String> tableSubdirectory = new ArrayList<>();
    tableSubdirectory.add(tableName);
    if (!FDBHelper.doesSubdirectoryExists(db, tableSubdirectory)) {
      return StatusCode.TABLE_NOT_FOUND;
    }

    // retrieve attributes of the table, check if attributes exists
    TableMetadataTransformer tblTransformer = new TableMetadataTransformer(tableName);
    List<String> tblAttributeDirPath = tblTransformer.getTableAttributeStorePath();
    TableMetadata tblMetadata = tblTransformer.deserialize(FDBHelper.getAllKeyValuePairsOfSubdirectory(db, tblAttributeDirPath));

    HashMap<String, AttributeType> attributes = tblMetadata.getAttributes();
    if (!attributes.containsKey(attributeName)) {
      return StatusCode.ATTRIBUTE_NOT_FOUND;
    }

    Transaction tx = FDBHelper.openTransaction(db);
    FDBHelper.removeKeyValuePair(db, tx, tblAttributeDirPath,
        TableMetadataTransformer.getTableAttributeKeyTuple(attributeName));
    FDBHelper.commitTransaction(tx);

    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropAllTables() {
    // your code
    FDBHelper.clear(db);
    return StatusCode.SUCCESS;
  }
}
