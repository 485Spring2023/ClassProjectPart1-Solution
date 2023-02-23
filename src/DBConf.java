import java.util.Collections;
import java.util.List;

public class DBConf {
  public static int FDB_API_VERSION = 710;
  public static String METADATA_STORE = "metadataStore";
  public static String METADATA_TABLE_ATTR_STORE = "attributeStore";
  public static List<String> METADATA_STORE_PATH = Collections.singletonList(METADATA_STORE);

}
