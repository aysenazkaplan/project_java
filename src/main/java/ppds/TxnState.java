package ppds;

import java.util.*;
import java.util.concurrent.locks.Lock;

public class TxnState {
    public final List<UndoOp> undoLog = new ArrayList<>();
    // İşlem boyunca elde tutulan kilitler. LinkedHashSet sırayı korur.
    public final Set<Lock> heldLocks = new LinkedHashSet<>();

    public static class UndoOp {
        public final String indexName;
        public final Record record;
        public final boolean wasInserted;

        public UndoOp(String indexName, Record record, boolean wasInserted) {
            this.indexName = indexName;
            this.record = record;
            this.wasInserted = wasInserted;
        }
    }
}