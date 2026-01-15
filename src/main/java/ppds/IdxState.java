package ppds;

/**
 * Bir thread'in indeks üzerindeki konumunu takip eder.
 */
public class IdxState {
    public final String indexName;
    public Record lastRecord = null; // get veya getNext tarafından güncellenir

    public IdxState(String indexName) {
        this.indexName = indexName;
    }
}