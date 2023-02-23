import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class FDBHelper {

  public static int FDB_API_VERSION = 710;

  public static int MAX_TRANSACTION_COMMIT_RETRY_TIMES = 20;

  public static Database initialization() {
    FDB fdb = FDB.selectAPIVersion(FDB_API_VERSION);

    Database db = null;
    try {
      db = fdb.open();
    } catch (Exception e) {
      System.out.println("ERROR: the database is not successfully opened: " + e);
    }
    return db;
  }


  public static void setFDBKVPair(Database db, Transaction tx, FDBKVPair kv) {
    DirectorySubspace tgtSubspace = FDBHelper.createOrOpenSubspace(db, kv.getSubspacePath());
    tx.set(tgtSubspace.pack(kv.getKey()), kv.getValue().pack());
  }

  public static void persistFDBKVPairs(Database db, Transaction tx, List<FDBKVPair> kvPairs) {
    for (FDBKVPair kv : kvPairs) {
      setFDBKVPair(db, tx, kv);
    }
  }

  public static List<String> getAllDirectSubspaceName(Database db, List<String> path) {
    if (!doesSubdirectoryExists(db, path)) {
      return new ArrayList<>();
    }
    DirectorySubspace dir = FDBHelper.createOrOpenSubspace(db, path);
    List<String> subpaths = dir.list(db).join();

    return subpaths;
  }

  public static List<FDBKVPair> getAllKeyValuePairsOfSubdirectory(Database db, List<String> path) {
    List<FDBKVPair> res = new ArrayList<>();
    if (!doesSubdirectoryExists(db, path)) {
      return res;
    }

    DirectorySubspace dir = FDBHelper.createOrOpenSubspace(db, path);
    Range range = dir.range();

    Transaction readTx = openTransaction(db);

    List<KeyValue> kvs = readTx.getRange(range).asList().join();
    for (KeyValue kv : kvs) {
      Tuple key = dir.unpack(kv.getKey());
      Tuple value = Tuple.fromBytes(kv.getValue());
      res.add(new FDBKVPair(path, key, value));
    }

    commitTransaction(readTx);
    return res;
  }

  public static void clear(Database db) {
    Transaction tx = openTransaction(db);
    final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
    final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
    tx.clear(st, en);
    commitTransaction(tx);
  }

  public static DirectorySubspace createOrOpenSubspace(Database db, List<String> path) {
    return DirectoryLayer.getDefault().createOrOpen(db, path).join();
  }

  public static boolean doesSubdirectoryExists(Database db, List<String> path) {
    return DirectoryLayer.getDefault().exists(db, path).join();
  }

  public static void removeSubspace(Database db, Transaction tx, List<String> path) {
    DirectoryLayer.getDefault().remove(tx, path).join();
  }

  public static void removeKeyValuePair(Database db, Transaction tx, List<String> path, Tuple keyTuple) {
    if (!doesSubdirectoryExists(db, path)) {
      return;
    }

    DirectorySubspace dir = FDBHelper.createOrOpenSubspace(db, path);

    tx.clear(dir.pack(keyTuple));
  }

  public static Transaction openTransaction(Database db) {
    return db.createTransaction();
  }

  public static boolean commitTransaction(Transaction tx) {
    return tryCommitTx(tx, 0);
  }

  public static boolean tryCommitTx(Transaction tx, int retryCounter) {
    try {
      tx.commit().join();
      return true;
    } catch (FDBException e) {
      if (retryCounter < MAX_TRANSACTION_COMMIT_RETRY_TIMES) {
        retryCounter++;
        tryCommitTx(tx, retryCounter);
      } else {
        tx.cancel();
        return false;
      }
    }
    return false;
  }

  public static void abortTransaction(Transaction tx) {
    tx.cancel();
  }
}
