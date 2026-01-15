package ppds;

import org.apache.commons.lang3.tuple.Pair;
import ppds.Types.ErrCode;
import ppds.Types.KeyType;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IXServer implements Server {
    private final Map<String, Index> indices = new ConcurrentHashMap<>();

    private static class Index {
        final String name;
        final KeyType type;
        final TreeMap<RecordKey, String> data = new TreeMap<>();
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        Index(String name, KeyType type) { this.name = name; this.type = type; }
    }

    // Kilit Edinme Yardımcısı (Strict 2PL)
    private ErrCode acquireLock(TxnState txn, Lock lock) {
        if (txn == null) {
            lock.lock(); // İşlem dışı (autocommit): hemen kilitle
            return ErrCode.SUCCESS;
        }
        if (txn.heldLocks.contains(lock)) return ErrCode.SUCCESS;

        try {
            // Deadlock tespiti için timeout (500ms - 1sn idealdir)
            if (lock.tryLock(500, TimeUnit.MILLISECONDS)) {
                txn.heldLocks.add(lock);
                return ErrCode.SUCCESS;
            }
            return ErrCode.DEADLOCK;
        } catch (InterruptedException e) {
            return ErrCode.DEADLOCK;
        }
    }

    private void releaseLocks(TxnState txn) {
        if (txn == null) return;
        List<Lock> locks = new ArrayList<>(txn.heldLocks);
        Collections.reverse(locks); // Kilitleri ters sırayla bırakmak daha güvenlidir
        for (Lock l : locks) {
            l.unlock();
        }
        txn.heldLocks.clear();
    }

    @Override
    public ErrCode create(KeyType type, String name) {
        if (indices.containsKey(name)) return ErrCode.DB_EXISTS;
        indices.put(name, new Index(name, type));
        return ErrCode.SUCCESS;
    }

    @Override
    public ErrCode drop(String name) {
        if (indices.remove(name) == null) return ErrCode.FAILURE;
        return ErrCode.SUCCESS;
    }

    @Override
    public Pair<ErrCode, IdxState> openIndex(String name) {
        if (!indices.containsKey(name)) return Pair.of(ErrCode.DB_DNE, null);
        return Pair.of(ErrCode.SUCCESS, new IdxState(name));
    }

    @Override
    public ErrCode closeIndex(IdxState idxState) {
        return (idxState != null && indices.containsKey(idxState.indexName)) ? ErrCode.SUCCESS : ErrCode.DB_DNE;
    }

    @Override
    public Pair<ErrCode, TxnState> beginTransaction() {
        return Pair.of(ErrCode.SUCCESS, new TxnState());
    }

    @Override
    public ErrCode commitTransaction(TxnState txnState) {
        if (txnState == null) return ErrCode.TXN_DNE;
        txnState.undoLog.clear();
        releaseLocks(txnState);
        return ErrCode.SUCCESS;
    }

    @Override
    public ErrCode abortTransaction(TxnState txnState) {
        if (txnState == null) return ErrCode.TXN_DNE;
        for (int i = txnState.undoLog.size() - 1; i >= 0; i--) {
            TxnState.UndoOp op = txnState.undoLog.get(i);
            Index idx = indices.get(op.indexName);
            if (idx == null) continue;
            // Abort sırasında yazma kilidi şarttır
            idx.lock.writeLock().lock();
            try {
                RecordKey rk = new RecordKey(op.record.getKey(), op.record.getPayload());
                if (op.wasInserted) idx.data.remove(rk);
                else idx.data.put(rk, op.record.getPayload());
            } finally {
                idx.lock.writeLock().unlock();
            }
        }
        releaseLocks(txnState);
        return ErrCode.SUCCESS;
    }

    @Override
    public ErrCode get(IdxState idxState, TxnState txnState, Record record) {
        Index idx = indices.get(idxState.indexName);
        if (idx == null) return ErrCode.DB_DNE;

        Lock lock = idx.lock.readLock();
        ErrCode lRet = acquireLock(txnState, lock);
        if (lRet != ErrCode.SUCCESS) return lRet;

        try {
            RecordKey searchKey = new RecordKey(record.getKey(), "");
            Map.Entry<RecordKey, String> entry = idx.data.ceilingEntry(searchKey);

            if (entry != null && compareKeys(entry.getKey().key, record.getKey()) == 0) {
                record.setPayload(entry.getValue());
                idxState.lastRecord = new Record(entry.getKey().key, entry.getValue());
                return ErrCode.SUCCESS;
            }
            idxState.lastRecord = null; // Başarısız aramada imleci temizle
            return ErrCode.KEY_NOTFOUND;
        } finally {
            if (txnState == null) lock.unlock();
        }
    }

    @Override
    public ErrCode getNext(IdxState idxState, TxnState txnState, Record record) {
        Index idx = indices.get(idxState.indexName);
        if (idx == null) return ErrCode.DB_DNE;
        if (idxState.lastRecord == null) return ErrCode.FAILURE;

        Lock lock = idx.lock.readLock();
        ErrCode lRet = acquireLock(txnState, lock);
        if (lRet != ErrCode.SUCCESS) return lRet;

        try {
            RecordKey lastKey = new RecordKey(idxState.lastRecord.getKey(), idxState.lastRecord.getPayload());
            Map.Entry<RecordKey, String> nextEntry = idx.data.higherEntry(lastKey);

            if (nextEntry != null) {
                record.setKey(nextEntry.getKey().key);
                record.setPayload(nextEntry.getValue());
                idxState.lastRecord = new Record(nextEntry.getKey().key, nextEntry.getValue());
                return ErrCode.SUCCESS;
            }
            return ErrCode.DB_END;
        } finally {
            if (txnState == null) lock.unlock();
        }
    }

    @Override
    public ErrCode insertRecord(IdxState idxState, TxnState txnState, Key<?> k, String payload) {
        Index idx = indices.get(idxState.indexName);
        if (idx == null) return ErrCode.DB_DNE;

        Lock lock = idx.lock.writeLock();
        ErrCode lRet = acquireLock(txnState, lock);
        if (lRet != ErrCode.SUCCESS) return lRet;

        try {
            RecordKey rk = new RecordKey(k, payload);
            if (idx.data.containsKey(rk)) return ErrCode.ENTRY_EXISTS;

            idx.data.put(rk, payload);
            if (txnState != null) {
                txnState.undoLog.add(new TxnState.UndoOp(idx.name, new Record(k, payload), true));
            }
            return ErrCode.SUCCESS;
        } finally {
            if (txnState == null) lock.unlock();
        }
    }

    @Override
    public ErrCode delRecord(IdxState idxState, TxnState txnState, Record record) {
        Index idx = indices.get(idxState.indexName);
        if (idx == null) return ErrCode.DB_DNE;

        Lock lock = idx.lock.writeLock();
        ErrCode lRet = acquireLock(txnState, lock);
        if (lRet != ErrCode.SUCCESS) return lRet;

        try {
            if (record.getPayload() != null && !record.getPayload().isEmpty()) {
                RecordKey rk = new RecordKey(record.getKey(), record.getPayload());
                String removed = idx.data.remove(rk);
                if (removed == null) return ErrCode.ENTRY_DNE;
                if (txnState != null) txnState.undoLog.add(new TxnState.UndoOp(idx.name, new Record(record.getKey(), removed), false));
            } else {
                // Belirli bir anahtara ait tüm kayıtları sil
                List<RecordKey> toRemove = new ArrayList<>();
                RecordKey start = new RecordKey(record.getKey(), "");
                SortedMap<RecordKey, String> tail = idx.data.tailMap(start);
                for (RecordKey rk : tail.keySet()) {
                    if (compareKeys(rk.key, record.getKey()) == 0) toRemove.add(rk);
                    else break;
                }
                if (toRemove.isEmpty()) return ErrCode.KEY_NOTFOUND;
                for (RecordKey rk : toRemove) {
                    String val = idx.data.remove(rk);
                    if (txnState != null) txnState.undoLog.add(new TxnState.UndoOp(idx.name, new Record(rk.key, val), false));
                }
            }
            return ErrCode.SUCCESS;
        } finally {
            if (txnState == null) lock.unlock();
        }
    }

    private int compareKeys(Key<?> k1, Key<?> k2) {
        return new RecordKey(k1, "").compareRawKeys(k1, k2);
    }

    private static class RecordKey implements Comparable<RecordKey> {
        final Key<?> key;
        final String payload;
        RecordKey(Key<?> key, String payload) { this.key = key; this.payload = payload; }

        @Override
        public int compareTo(RecordKey o) {
            int cmp = compareRawKeys(this.key, o.key);
            return (cmp != 0) ? cmp : this.payload.compareTo(o.payload);
        }

        @SuppressWarnings("unchecked")
        int compareRawKeys(Key<?> k1, Key<?> k2) {
            switch (k1.getKey()) {
                case INT: return ((Integer) k1.getKeyval()).compareTo((Integer) k2.getKeyval());
                case LONG: return ((Long) k1.getKeyval()).compareTo((Long) k2.getKeyval());
                case STRING: return ((String) k1.getKeyval()).compareTo((String) k2.getKeyval());
                default: return 0;
            }
        }
    }
}