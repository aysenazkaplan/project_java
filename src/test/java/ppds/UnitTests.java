package ppds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import ppds.Types.ErrCode;
import ppds.Types.KeyType;

public class UnitTests {
    public IdxState ixstate = null;
    public TxnState txstate = null;
    public Key<?> k_a = new Key<>(KeyType.STRING, "a_key");
    public Key<?> k_b = new Key<>(KeyType.STRING, "b_key");
    public Key<?> k_c = new Key<>(KeyType.STRING, "c_key");
    public Key<?> k_d = new Key<>(KeyType.STRING, "d_key");
    private int CAN_STOP_TRANS = 0;
    private int DID_TRANSACTION_PASS = 0;
    private int DID_SECONDARY_PASS = 0;
    public KeyType type = KeyType.STRING;
    public IXServer ixserver = new IXServer();

    @Test
    public void allTest() throws InterruptedException {
        ErrCode ret1, ret2 = null;
        ret1 = ixserver.create(type, "primary_index");
        assertEquals(ErrCode.SUCCESS.name(), ret1.toString());

        Thread tran_test_thread = new Thread(new Runnable() {
            public void run() {
                TxnState txn = null;
                Pair<ErrCode, IdxState> pIdx = ixserver.openIndex("primary_index");
                ErrCode retT = pIdx.getLeft();
                IdxState idx = pIdx.getRight();
                if (retT != ErrCode.SUCCESS) {
                    System.out.println("Cannot open primary index from transaction tester thread");
                    DID_TRANSACTION_PASS = -1;
                    return;
                }
                int count = 0;
                while (CAN_STOP_TRANS == 0 || count == 0) {
                    while (true) {
                        count++;
                        Pair<ErrCode, TxnState> pTxn = ixserver.beginTransaction();
                        ErrCode err = pTxn.getLeft();
                        txn = pTxn.getRight();
                        if (err != ErrCode.SUCCESS) {
                            System.out.println("Could not begin transaction in test_transaction_func");
                            if (err == ErrCode.DEADLOCK) {
                                System.out.println("DEADLOCK received");
                                if (ixserver.abortTransaction(txn) != ErrCode.SUCCESS)
                                    System.out.println("Could not abort deadlocked transaction");
                                continue;
                            }
                            DID_TRANSACTION_PASS = -1;
                            return;
                        }
                        Record record = new Record(new Key<>(KeyType.STRING, "b_key"));
                        err = ixserver.get(idx, txn, record);
                        if (err != ErrCode.KEY_NOTFOUND) {
                            System.out.println("test_transaction_func found entry with key 'b'");
                            if (err == ErrCode.DEADLOCK) {
                                System.out.println("DEADLOCK received");
                                if (ixserver.abortTransaction(txn) != ErrCode.SUCCESS)
                                    System.out.println("Could not abort deadlocked transaction");
                                continue;
                            }
                            DID_TRANSACTION_PASS = -1;
                            return;
                        }
                        err = ixserver.commitTransaction(txn);
                        if (err != ErrCode.SUCCESS) {
                            System.out.println("Could not end transaction in test_transaction_func");
                            if (err == ErrCode.DEADLOCK) {
                                System.out.println("DEADLOCK received");
                                if (ixserver.abortTransaction(txn) != ErrCode.SUCCESS)
                                    System.out.println("Could not abort deadlocked transaction");
                                continue;
                            }
                            DID_TRANSACTION_PASS = -1;
                            return;
                        }
                        DID_TRANSACTION_PASS = 1;
                        break;
                    }
                }
                System.out.println("Successfully passed transaction test! loop count = " + count);
            }
        });

        Thread sec_test_thread = new Thread(new Runnable() {
            public void run() {
                ErrCode err;
                IdxState idx = null;
                TxnState txn = null;
                Record record;
                Key<?> k_a = new Key<>(KeyType.STRING, "a_key");
                Key<?> k_b = new Key<>(KeyType.STRING, "b_key");
                Key<?> k_c = new Key<>(KeyType.STRING, "c_key");
                Key<?> k_d = new Key<>(KeyType.STRING, "d_key");

                if (ixserver.create(KeyType.STRING, "secondary_index") != ErrCode.SUCCESS) {
                    System.out.println("Could not create secondary index");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                Pair<ErrCode, IdxState> pIdx = ixserver.openIndex("secondary_index");
                idx = pIdx.getRight();
                if (pIdx.getLeft() != ErrCode.SUCCESS) {
                    System.out.println("Cannot open secondary index");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                Pair<ErrCode, TxnState> pTxn = ixserver.beginTransaction();
                txn = pTxn.getRight();
                if (pTxn.getLeft() != ErrCode.SUCCESS) {
                    System.out.println("Failed to begin txn");
                    DID_SECONDARY_PASS = -1;
                    return;
                }

                // get(a) on empty DB
                record = new Record(k_a);
                if (ixserver.get(idx, txn, record) != ErrCode.KEY_NOTFOUND) {
                    System.out.println("Get 'a' on empty DB did not properly report KEY_NOTFOUND");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                // insert (b, 1)
                if (ixserver.insertRecord(idx, txn, k_b, "value one") != ErrCode.SUCCESS) {
                    System.out.println("Could not insert (b, 1) into secondary DB");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                // getNext should return (b, 1)
                record.clearPayload();
                if (ixserver.getNext(idx, txn, record) != ErrCode.SUCCESS) {
                    System.out.println("getNext on single entry in secondary DB failed");
                    DID_SECONDARY_PASS = -1;
                    return;
                } else {
                    Object kv = record.getKey().getKeyval();
                    if (!(kv instanceof String) || !((String) kv).equalsIgnoreCase("b_key")
                            || !record.getPayload().equalsIgnoreCase("value one")) {
                        System.out.println("Failed to return (b, 1) from getNext");
                        DID_SECONDARY_PASS = -1;
                        return;
                    }
                }
                // getNext should hit DB_END
                if (ixserver.getNext(idx, txn, record) != ErrCode.DB_END) {
                    System.out.println("getNext does not return DB_END properly");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                // insert (c, 1)
                if (ixserver.insertRecord(idx, txn, k_c, "value one") != ErrCode.SUCCESS) {
                    System.out.println("Could not insert (c, 1) into secondary DB");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                // get(b)
                record = new Record(k_b);
                if (ixserver.get(idx, txn, record) != ErrCode.SUCCESS) {
                    System.out.println("Could not get secondary DB");
                    DID_SECONDARY_PASS = -1;
                    return;
                } else {
                    Object kv = record.getKey().getKeyval();
                    if (!(kv instanceof String) || !((String) kv).equalsIgnoreCase("b_key")
                            || !record.getPayload().equalsIgnoreCase("value one")) {
                        System.out.println("Failed to return payload (b) from get");
                        DID_SECONDARY_PASS = -1;
                        return;
                    }
                }
                // getNext should return (c, 1)
                record.clearPayload();
                if (ixserver.getNext(idx, txn, record) != ErrCode.SUCCESS) {
                    System.out.println("Could not getNext in secondary DB");
                    DID_SECONDARY_PASS = -1;
                    return;
                } else {
                    Object kv = record.getKey().getKeyval();
                    if (!(kv instanceof String) || !((String) kv).equalsIgnoreCase("c_key")
                            || !record.getPayload().equalsIgnoreCase("value one")) {
                        System.out.println("Failed to return (c, 1) from getNext");
                        DID_SECONDARY_PASS = -1;
                        return;
                    }
                }
                // insert(c, 2)
                if (ixserver.insertRecord(idx, txn, k_c, "value two") != ErrCode.SUCCESS) {
                    System.out.println("Could not insert (c, 2) into secondary DB");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                // getNext should return (c, 2) or DB_END
                record.clearPayload();
                err = ixserver.getNext(idx, txn, record);
                if (err != ErrCode.SUCCESS) {
                    if (err != ErrCode.DB_END) {
                        System.out.println("Could not getNext in secondary DB");
                        DID_SECONDARY_PASS = -1;
                        return;
                    }
                } else {
                    Object kv = record.getKey().getKeyval();
                    if (!(kv instanceof String) || !((String) kv).equalsIgnoreCase("c_key")
                            || !record.getPayload().equalsIgnoreCase("value two")) {
                        System.out.println("Failed to return (c, 2) from getNext");
                        DID_SECONDARY_PASS = -1;
                        return;
                    }
                }
                // insert a small payload of one character
                if (ixserver.insertRecord(idx, txn, k_d, "z") != ErrCode.SUCCESS) {
                    System.out.println("Could not insert a 1-character payload into secondary DB");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                // make sure that it can be retrieved properly
                record = new Record(k_d);
                if (ixserver.get(idx, txn, record) != ErrCode.SUCCESS) {
                    System.out.println("Could not retrieve small payload");
                    DID_SECONDARY_PASS = -1;
                    return;
                } else {
                    Object kv = record.getKey().getKeyval();
                    if (!(kv instanceof String) || !((String) kv).equalsIgnoreCase("d_key")
                            || !record.getPayload().equalsIgnoreCase("z")) {
                        System.out.println("Failed to return record with small payload");
                        DID_SECONDARY_PASS = -1;
                        return;
                    }
                }
                if (ixserver.commitTransaction(txn) != ErrCode.SUCCESS) {
                    System.out.println("Could not end secondary index tester");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                if (ixserver.closeIndex(idx) != ErrCode.SUCCESS) {
                    System.out.println("Could not close secondary index");
                    DID_SECONDARY_PASS = -1;
                    return;
                }
                System.out.println("Successfully passed secondary index tests");
                DID_SECONDARY_PASS = 1;
                return;
            }
        });

        tran_test_thread.start();
        sec_test_thread.start();

        // Expected intial state at this point is that the primary_index is completely empty
        // Open the primary index
        Pair<ErrCode, IdxState> pIdx = ixserver.openIndex("primary_index");
        ret1 = pIdx.getLeft();
        ixstate = pIdx.getRight();
        assertEquals("could not open index", ErrCode.SUCCESS.name(), ret1.toString());

        while (true) {
            // begin main transaction
            Pair<ErrCode, TxnState> pTxn = ixserver.beginTransaction();
            ret1 = pTxn.getLeft();
            txstate = pTxn.getRight();
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to begin main txn", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // insert tuple (a, 1)
            ret1 = ixserver.insertRecord(ixstate, txstate, k_a, "value one");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to insert (a, 1)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // get unique(a) to return (a, 1)
            Record record = new Record(k_a);
            ret1 = ixserver.get(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to get when DB contains single record", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to return (a,1) from get", "a_key", kv);
                assertEquals("failed to return (a,1) from get", "value one", record.getPayload());
            }

            // insert tuple (b, 1)
            ret1 = ixserver.insertRecord(ixstate, txstate, k_b, "value one");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to insert (b,1)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // get unique(a) to return (a, 1);
            record = new Record(k_a);
            ret1 = ixserver.get(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to get record a", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to return (a,1) from get", "a_key", kv);
                assertEquals("failed to return (a,1) from get", "value one", record.getPayload());
            }

            // get next to return (b, 1)
            record.clearPayload();
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to getNext", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to return (b,1) from getNext", "b_key", kv);
                assertEquals("failed to return (b,1) from getNext", "value one", record.getPayload());
            }

            // insert (c, 1)
            ret1 = ixserver.insertRecord(ixstate, txstate, k_c, "value one");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to insert (c,1)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // insert (c, 1) should not be able to insert identical tuple
            ret1 = ixserver.insertRecord(ixstate, txstate, k_c, "value one");
            if (ret1 != ErrCode.ENTRY_EXISTS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("successfully inserted duplicate entry (c,1)", ErrCode.ENTRY_EXISTS.name(), ret1.toString());
            }

            // getNext should return (c, 1)
            record.clearPayload();
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to getNext", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to return (c,1) from getNext", "c_key", kv);
                assertEquals("failed to return (c,1) from getNext", "value one", record.getPayload());
            }

            // getNext should fail with DB_END
            record.clearPayload();
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.DB_END) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("did not properly find end of DB with getNext", ErrCode.DB_END.name(), ret1.toString());
            }

            // delete (c, 1)
            record = new Record(k_c, "value one");
            ret1 = ixserver.delRecord(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to delete specific entry (c, 1)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // get(c) should return nothing
            record = new Record(k_c);
            ret1 = ixserver.get(ixstate, txstate, record);
            if (ret1 != ErrCode.KEY_NOTFOUND) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("found an entry that should not exist: " + record.getKey().getKeyval() + ": " + record.getPayload(), ErrCode.KEY_NOTFOUND.name(), ret1.toString());
            }

            // insert (b, 2)
            ret1 = ixserver.insertRecord(ixstate, txstate, k_b, "value two");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to insert (b,2)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            //get(b) should return (b,1) or (b,2)
            record = new Record(k_b);
            ret1 = ixserver.get(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("did not properly get unique when keyed on two entries", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to return (b,1) or (b,2) from getNext with duplicate key values", "b_key", kv);
                List<String> valid_pl = Arrays.asList("value one", "value two");
                assertTrue("failed to return (b,1) or (b,2) from getNext with duplicate key values", valid_pl.contains(record.getPayload()));
            }

            // getNext should return (b,2) or (b,1);
            record.clearPayload();
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to getNext on duplicate key values", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to return (b,1) or (b,2) from getNext with duplicate key values", "b_key", kv);
                List<String> valid_pl = Arrays.asList("value one", "value two");
                assertTrue("failed to return (b,1) or (b,2) from getNext with duplicate key values", valid_pl.contains(record.getPayload()));
            }

            // delete (b) should erase all values keyed on b
            record = new Record(k_b);
            ret1 = ixserver.delRecord(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to delete multiple payloads on same key", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // get(b) should not find any entries
            record = new Record(k_b);
            ret1 = ixserver.get(ixstate, txstate, record);
            if (ret1 != ErrCode.KEY_NOTFOUND) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("get on a key which has been deleted did not return KEY_NOTFOUND", ErrCode.KEY_NOTFOUND.name(), ret1.toString());
            }
            // insert (a, 2)
            ret1 = ixserver.insertRecord(ixstate, txstate, k_a, "value two");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not insert (a, 2)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // delete(a, 1)
            record = new Record(k_a, "value one");
            ret1 = ixserver.delRecord(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not delete (a, 1)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // get(a) should return (a, 2)
            record = new Record(k_a);
            ret1 = ixserver.get(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not get(a) after deleted (a, 1)", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to retrieve (a,2) from get call", "a_key", kv);
                assertEquals("failed to retrieve (a,2) from get call", "value two", record.getPayload());
            }
            // end transaction, commit all changes
            ret1 = ixserver.commitTransaction(txstate);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("unable to commit transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }
            break;
        }

        // At this point the DB should have only one entry, (a, 2)

        while (true) {
            txstate = null;
            // begin main transaction
            Pair<ErrCode, TxnState> pTxn  = ixserver.beginTransaction();
            txstate = pTxn.getRight();
            if (pTxn.getLeft() != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not begin second main transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }

            //getNext before a get should return the first (and only, in this case) entry in a DB: (a,2)
            Record record = new Record(k_a);
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not find expected (a, 2) in reconnected database", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to retrieve (a,2) from getNext", "a_key", kv);
                assertEquals("failed to retrieve (a,2) from getNext", "value two", record.getPayload());
            }

            // getNext should hit DB_END
            record.clearPayload();
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.DB_END) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("found extra tuple in DB", ErrCode.DB_END.name(), ret1.toString());
            }

            // insert (b, 1)
            ret1 = ixserver.insertRecord(ixstate, txstate, k_b, "value one");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not insert (b,1)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // abort transaction so that (b, 1) is not committed
            ret1 = ixserver.abortTransaction(txstate);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("unable to abort second main transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // verify that the transaction rolled back properly
            record = new Record(k_b);
            while (true) {
                ret1 = ixserver.get(ixstate, txstate, record);
                if (ret1 != ErrCode.KEY_NOTFOUND) {
                    if (ret2 == ErrCode.DEADLOCK) {
                        assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                        continue;
                    }
                    assertEquals("aborting a transaction did not roll back properly", ErrCode.KEY_NOTFOUND.name(), ret1.toString());
                    break;
                } else {
                    break;
                }
            }
            break;
        }

        // Below transaction tests if getNext returns the 1st key after
        // a failed call to get with key not found.
        while (true) {
            Pair<ErrCode, TxnState> pTxn  = ixserver.beginTransaction();
            txstate = pTxn.getRight();
            ret1 = pTxn.getLeft();
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not begin third main transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // insert (d, 1)
            ret1 = ixserver.insertRecord(ixstate, txstate, k_d, "value one");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not insert (d, 1)", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // get(b) should not find any entries
            Record record = new Record(k_b);
            ret1 = ixserver.get(ixstate, txstate, record);
            if (ret1 != ErrCode.KEY_NOTFOUND) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("get on a key which has been deleted did not return KEY_NOTFOUND", ErrCode.KEY_NOTFOUND.name(), ret1.toString());
            }

            // getNext should return (d, 1)
            record.clearPayload();
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to getNext", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                Object kv = record.getKey().getKeyval();
                assertEquals("failed to return (d,1) from getNext", "d_key", kv);
                assertEquals("failed to return (d,1) from getNext", "value one", record.getPayload());
            }

            // end transaction, commit all changes
            ret1 = ixserver.commitTransaction(txstate);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("unable to commit transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }
            break;
        }

        // Create a third index
        ret1 = ixserver.create(type, "terciary_index");
        assertEquals("could not create terciary index", ErrCode.SUCCESS.name(), ret1.toString());


        // Open the third index
        pIdx = ixserver.openIndex("terciary_index");
        IdxState terc_idx = pIdx.getRight();
        if (pIdx.getLeft() != ErrCode.SUCCESS)
            System.out.println("Cannot open terciary index");

        // get(a) on the primary outside of a transaction should return (a,2)
        Record record = new Record(k_a);
        ret1 = ixserver.get(ixstate, null, record);
        assertEquals("could not get(a) without a transaction during multi_tbl_txn", ErrCode.SUCCESS.name(), ret1.toString());
        Object kv = record.getKey().getKeyval();
        assertEquals("failed to retrieve (a,2) from get call outside of transaction", "a_key", kv);
        assertEquals("failed to retrieve (a,2) from get call outside of transaction", "value two", record.getPayload());

        // Below transaction tests transaction behavior with multiple tables
        while (true) {
            // Begin transaction
            Pair<ErrCode, TxnState> pTxn  = ixserver.beginTransaction();
            ret1 = pTxn.getLeft();
            txstate = pTxn.getRight();
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not begin multi-table main transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // getNext should return (a, 2)
            record = new Record(k_a);
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to getNext after beginning txn", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                kv = record.getKey().getKeyval();
                assertEquals("failed to return (a,2) from getNext", "a_key", kv);
                assertEquals("failed to return (a,2) from getNext", "value two", record.getPayload());
            }

            // insert (b, 1) into the terciary index
            ret1 = ixserver.insertRecord(terc_idx, txstate, k_b, "value one");
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not insert (b, 1) into terciary index", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // getNext on the primary index should return (d,1)
            record.clearPayload();
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to getNext after beginning txn", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                kv = record.getKey().getKeyval();
                assertEquals("failed to return (d,1) from getNext", "d_key", kv);
                assertEquals("failed to return (d,1) from getNext", "value one", record.getPayload());
            }

            // commit the transaction
            ret1 = ixserver.commitTransaction(txstate);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("unable to commit multi-table transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // close the primary index
            ret1 = ixserver.closeIndex(ixstate);
            assertEquals("unable to close the primary index", ErrCode.SUCCESS.name(), ret1.toString());
            break;
        }

        while (true) {
            // begin a transaction
            Pair<ErrCode, TxnState> pTxn  = ixserver.beginTransaction();
            ret1 = pTxn.getLeft();
            txstate = pTxn.getRight();
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("could not begin open_index transaction", ErrCode.SUCCESS.name(), ret1.toString());
            }

            // open the primary index while transaction is open
            pIdx = ixserver.openIndex("primary_index");
            ixstate = pIdx.getRight();
            if (pIdx.getLeft() != ErrCode.SUCCESS)
                System.out.println("Cannot open primary index with a transaction");

            // getNext should return (a, 2)
            record = new Record(k_a);
            ret1 = ixserver.getNext(ixstate, txstate, record);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("failed to getNext after opening index in txn", ErrCode.SUCCESS.name(), ret1.toString());
            } else {
                kv = record.getKey().getKeyval();
                assertEquals("failed to return (a,2) from getNext", "a_key", kv);
                assertEquals("failed to return (a,2) from getNext", "value two", record.getPayload());
            }

            // Commit transaction
            ret1 = ixserver.commitTransaction(txstate);
            if (ret1 != ErrCode.SUCCESS) {
                if (ret1 == ErrCode.DEADLOCK) {
                    ret2 = ixserver.abortTransaction(txstate);
                    assertEquals("could not abort deadlocked transaction", ErrCode.SUCCESS.name(), ret2.toString());
                    continue;
                }
                assertEquals("unable to commit transaction during which index was opened", ErrCode.SUCCESS.name(), ret1.toString());
            }
            break;
        }

        System.out.println("Successfully passed main function tests");
        CAN_STOP_TRANS = 1;
        tran_test_thread.join();
        sec_test_thread.join();
        assertEquals(1, DID_SECONDARY_PASS);
        assertEquals(1, DID_TRANSACTION_PASS);
    }
}
