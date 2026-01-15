package ppds;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import ppds.Types.ErrCode;
import ppds.Types.KeyType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SpeedTest {

    //these constants are subject to change when the official benchmark is run
    int seed = 1468;

    // inserts a thread does while populating the indices
    int NUM_POP_INSERTS = 4000;

    // tests per thread
    int NUM_TESTS_PER_THREAD = 16000;

    public double globalTime = 0;

    int MAX_NUM_INDICES = 50;

    // Global variables for the randomly generated constants for the test
    public int nIndices;

    public int MAX_STRING_LEN = 128;
    private AtomicInteger NUM_TXN_COMP = new AtomicInteger(0);
    private AtomicInteger NUM_TXN_FAIL = new AtomicInteger(0);
    private AtomicInteger NUM_DEADLOCK = new AtomicInteger(0);

    // Array of index name strings
    public List<String> indexNames = new ArrayList<>();

    // Array of index key types
    public List<KeyType> indexTypes = new ArrayList<>();

    // Array of random short keys
    public List<Integer> shortKeys;

    // Array of random int keys
    public List<Integer> intKeys;

    // Array of random charvar keys
    public List<String> strKeys;

    // Array of random payloads
    public List<String> payloads = new ArrayList<>();

    public List<Integer> randSeed;
    public List<Integer> randNumCounter;
    public List<List<Integer>> randNumArrays;

    IXServer ixserver = new IXServer();


    public class CreateTestIndex implements Runnable {
        private final int _index;
        public CreateTestIndex(int index) {
            this._index = index;
        }

        @Override
        public void run() {
            if(ixserver.create(indexTypes.get(_index), indexNames.get(_index)) != ErrCode.SUCCESS) {
                System.out.println("Could not create index " + indexNames.get(_index));
            }
        }
    }

    public class Populate implements Runnable {

        private final int _index;
        public Populate(int index) {
            this._index = index;
        }

        @Override
        public void run() {
            int numPopulateInserts = 0;
            IdxState idxState = null;
            while (numPopulateInserts < NUM_POP_INSERTS) {
                numPopulateInserts++;

                int indexNum = myrand(_index) % nIndices;

                //open a random index
                Pair<ErrCode, IdxState> pIdx = ixserver.openIndex(indexNames.get(indexNum));
                idxState = pIdx.getRight();
                if (pIdx.getLeft() != ErrCode.SUCCESS) {
                    System.out.println("Thread " + indexNum + " could not open index " + indexNames.get(indexNum));
                    //for an unexpected error, scrap this insert completely and try again
                    continue;
                }

                //make a random key and payload to insert
                Key<?> key = generate_key(indexNum, _index);
                String payload = generate_payload(_index);

                //insert the record
                while(true) {
                    ErrCode ret = ixserver.insertRecord(idxState, null, key, payload);
                    if((ret != ErrCode.SUCCESS) && (ret != ErrCode.ENTRY_EXISTS)) {
                        if(ret == ErrCode.DEADLOCK) {
                            //try again until not deadlocked
                            NUM_DEADLOCK.incrementAndGet();
                            continue;
                        }
                        System.out.println("thread " + _index + " failed to insert record for index " + indexNum + ". ErrCode = " + ret);
                    }
                    break;
                }

                if(ixserver.closeIndex(idxState) != ErrCode.SUCCESS) {
                    System.out.println("could not close index " + indexNames.get(indexNum));
                }
            }
        }
    }


    public class TestExecution implements Runnable {

        private final int _index;
        public TestExecution(int index) {
            this._index = index;
        }

        @Override
        public void run() {
            IdxState idxState = null;
            TxnState txnState = null;
            int testCounter = 0;
            ErrCode ret;

            while (testCounter < NUM_TESTS_PER_THREAD) {
                testCounter++;

                //determine what kind of test this will be
                int testType = myrand(_index) % 10;

                //determine on which index it will be run
                int indexNum = myrand(_index) % nIndices;

                //open the index
                Pair<ErrCode, IdxState> pIdx = ixserver.openIndex(indexNames.get(indexNum));
                idxState = pIdx.getRight();
                if (pIdx.getLeft() != ErrCode.SUCCESS) {
                    NUM_TXN_FAIL.incrementAndGet();

                    //for an unexpected error, scrap this insert completely and try again
                    continue;
                }

                //take a snapshot of where the RNG is for this thread before this test begins
                int randCounter = randNumCounter.get(_index);

                //start the transaction

                while (true) {
                    Pair<ErrCode, TxnState> pTxn = ixserver.beginTransaction();
                    ret = pTxn.getLeft();
                    txnState = pTxn.getRight();
                    if (ret != ErrCode.SUCCESS) {
                        if (ret == ErrCode.DEADLOCK) {
                            NUM_DEADLOCK.incrementAndGet();
                            continue;
                        }
                        System.out.println("failed to begin populate txn for index " + indexNum + " ErrCode = " + ret);

                        NUM_TXN_FAIL.incrementAndGet();

                        break;
                    }

                    if (testType < 1) {         //10% scan
                        ret = scan_test(idxState, txnState, _index, indexNum);
                    } else if (testType < 4) {  //30% get
                        ret = get_test(idxState, txnState, _index, indexNum);
                    } else {                    //60% update
                        ret = update_test(idxState, txnState, _index, indexNum);
                    }

                    if(ret == ErrCode.SUCCESS) {
                        //if the test ran successfully, commit txn
                        if((ret = ixserver.commitTransaction(txnState)) != ErrCode.SUCCESS) {
                            if (ret == ErrCode.DEADLOCK) {
                                NUM_DEADLOCK.incrementAndGet();
                                break;
                            }

                            NUM_TXN_FAIL.incrementAndGet();
                            break;
                        }

                        NUM_TXN_COMP.incrementAndGet();
                    } else {
                        if (ret == ErrCode.DEADLOCK) {
                            //if it was a deadlock, abort the transaction
                            if (ixserver.abortTransaction(txnState) != ErrCode.SUCCESS) {
                                NUM_TXN_FAIL.incrementAndGet();
                                break;
                            }

                            //roll back the test to repeat
                            randNumCounter.set(_index, randCounter);
                            continue;
                        }

                        //otherwise, simply abort txn
                        if ((ret = ixserver.abortTransaction(txnState)) != ErrCode.SUCCESS) {
                            if (ret == ErrCode.DEADLOCK) {
                                NUM_DEADLOCK.incrementAndGet();
                                break;
                            }
                            NUM_TXN_FAIL.incrementAndGet();
                            break;
                        }
                    }
                    break;
                }

                //close the index
                if ((ret = ixserver.closeIndex(idxState)) != ErrCode.SUCCESS) {
                    System.out.println("Thread " + _index + " failed to close an index after a test was run. ErrCode = " + ret);
                }
            }

        }
    }

    /*
    return SUCCESS if txn should be commited
    DEADLOCK if txn should be reattempted because of deadlock
    FAILURE if txn should be aborted
    */
    public ErrCode scan_test(IdxState idxState, TxnState txnState, int threadNum, int indexNum) {
        ErrCode ret;
        int K = (myrand(threadNum) % 100) + 100; // uniformly distributed between 100 and 200

        Key<?> key = generate_key(indexNum, threadNum);
        Record record = new Record(key);

        if (((ret = ixserver.get(idxState, txnState, record)) != ErrCode.SUCCESS) && (ret != ErrCode.KEY_NOTFOUND)) {
            if (ret == ErrCode.DEADLOCK) {
                NUM_DEADLOCK.incrementAndGet();
                return ErrCode.DEADLOCK;
            }

            NUM_TXN_FAIL.incrementAndGet();
            return ErrCode.FAILURE;
        }

        int count = 0;
        while (count < K) {
            count++;

            if (((ret = ixserver.getNext(idxState, txnState, record)) != ErrCode.SUCCESS) && (ret != ErrCode.DB_END)) {
                if (ret == ErrCode.DEADLOCK) {
                    NUM_DEADLOCK.incrementAndGet();
                    return ErrCode.DEADLOCK;
                }

                NUM_TXN_FAIL.incrementAndGet();
                return ErrCode.FAILURE;
            }
        }

        return ErrCode.SUCCESS;
    }

    /*
    return SUCCESS if txn should be commited
    DEADLOCK if txn should be reattempted because of deadlock
    FAILURE if txn should be aborted
    */
    public ErrCode get_test(IdxState idxState, TxnState txnState, int threadNum, int indexNum) {
        ErrCode ret;

        int L = (myrand(threadNum) % 10) + 20; // uniformly distributed between 20 and 30

        int count = 0;
        while (count < L) {
            count++;

            Key<?> key = generate_key(indexNum, threadNum);
            Record record = new Record(key);

            if (((ret = ixserver.get(idxState, txnState, record)) != ErrCode.SUCCESS) && (ret != ErrCode.KEY_NOTFOUND)) {
                if (ret == ErrCode.DEADLOCK) {
                    NUM_DEADLOCK.incrementAndGet();
                    return ErrCode.DEADLOCK;
                }

                NUM_TXN_FAIL.incrementAndGet();

                return ErrCode.FAILURE;
            }
        }

        return ErrCode.SUCCESS;
    }

    /*
    return SUCCESS if txn should be commited
    DEADLOCK if txn should be reattempted because of deadlock
    FAILURE if txn should be aborted
    */
    public ErrCode update_test(IdxState idxState, TxnState txnState, int threadNum, int indexNum) {
        ErrCode ret;
        int M = (myrand(threadNum) % 5) + 5; // uniformly distributed between 5 and 10

        int count = 0;
        while (count < M) {
            count++;

            //make a random key and payload to insert
            Key<?> key = generate_key(indexNum, threadNum);
            String payload = generate_payload(threadNum);

            if (((ret = ixserver.insertRecord(idxState, txnState, key, payload)) != ErrCode.SUCCESS) && (ret != ErrCode.ENTRY_EXISTS)) {
                if (ret == ErrCode.DEADLOCK) {
                    NUM_DEADLOCK.incrementAndGet();
                    return ErrCode.DEADLOCK;
                }

                NUM_TXN_FAIL.incrementAndGet();

                return ErrCode.FAILURE;
            }

            //delete a random key
            //generate the key
            key = generate_key(indexNum, threadNum);
            Record record = new Record(key);
            if (((ret = ixserver.delRecord(idxState, txnState, record)) != ErrCode.SUCCESS) && (ret != ErrCode.KEY_NOTFOUND)) {
                if (ret == ErrCode.DEADLOCK) {
                    NUM_DEADLOCK.incrementAndGet();
                    return ErrCode.DEADLOCK;
                }

                NUM_TXN_FAIL.incrementAndGet();
                return ErrCode.FAILURE;
            }
        }

        return ErrCode.SUCCESS;
    }

    int myrand(int threadNum)
    {
        int counter = randNumCounter.get(threadNum);
        randNumCounter.set(threadNum, counter+1);

        List<Integer> randNumArray = randNumArrays.get(threadNum);
        return randNumArray.get(counter);
    }

    public String p_randStr(int size)
    {
        String text = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

        Random random = new Random();

        int i, len = random.nextInt(Integer.MAX_VALUE) % (size - 1);
        StringBuilder result = new StringBuilder();
        for (i = 0; i < len; i++) {
            result.append(text.charAt(random.nextInt(Integer.MAX_VALUE) % (text.length()-1)));
        }

        return result.toString();
    }


    public void initialize(int seed) {
        Random random = new Random(seed);

        if(nIndices == 0) {
            nIndices = (random.nextInt(Integer.MAX_VALUE) % (MAX_NUM_INDICES -1)) + 1;
        }

        // Create the index names
        for (int i = 0; i < nIndices; i++) {
            indexNames.add("index" + i);
            KeyType type = KeyType.values()[random.nextInt(Integer.MAX_VALUE) % 3];
            indexTypes.add(type);
        }

        //create the seed locking mechanism for each thread
        randSeed = new ArrayList<>(MAX_NUM_INDICES);
        for (int i = 0; i < MAX_NUM_INDICES; i++) {
            randSeed.add(random.nextInt(Integer.MAX_VALUE));
        }

        //initialize the random data-related arrays
        randNumCounter = new Vector<>(MAX_NUM_INDICES);
        for(int i = 0; i < MAX_NUM_INDICES; i++) {
            randNumCounter.add(0);
        }

        randNumArrays = new ArrayList<>(MAX_NUM_INDICES);

        //create random shortKey array (for all threads to share) of size MAX_NUM_INDICES * NUM_POP_INSERTS
        shortKeys = new ArrayList<>(MAX_NUM_INDICES * NUM_POP_INSERTS);
        for (int i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
            shortKeys.add(random.nextInt(Integer.MAX_VALUE));
        }

        //create random intKey array (for all threads to share)
        intKeys = new ArrayList<>(MAX_NUM_INDICES * NUM_POP_INSERTS);
        for (int i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
            intKeys.add(random.nextInt(Integer.MAX_VALUE));
        }

        //create random strKeys array (for all threads to share)
        strKeys = new ArrayList<>(MAX_NUM_INDICES * NUM_POP_INSERTS);
        for (int i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
            strKeys.add(p_randStr(MAX_STRING_LEN));
        }

        //create random payload string array (for all threads to share)
        payloads = new ArrayList<>(MAX_NUM_INDICES * NUM_POP_INSERTS);
        for (int i = 0; i < MAX_NUM_INDICES * NUM_POP_INSERTS; i++) {
            payloads.add(p_randStr(Record.MAX_PAYLOAD_LEN - 1));
        }

        //2 random numbers for each insert: which index to insert into, key and payload
        //33 rand nums for each test: which index tested, type of test, # times test is run, and 30 keys + 1 payload
        int numRandNums = NUM_POP_INSERTS * (3) + NUM_TESTS_PER_THREAD * (3 + 30 + 1);

        //make the sub-arrays for the random numbers and strings
        for (int i = 0; i < MAX_NUM_INDICES; i++) {
            List<Integer> randNumArray = new ArrayList<>(numRandNums);
            //generate all random data for all of the threads, one thread at a time
            for (int j = 0; j < numRandNums; j++) {
                randNumArray.add(random.nextInt(Integer.MAX_VALUE));
            }
            randNumArrays.add(randNumArray);
        }
    }

    /*
    Generates a random string no longer than 'size'.
    */
    String generate_payload(int threadNum)
    {
        int counter = randNumCounter.get(threadNum);
        randNumCounter.set(threadNum, counter+1);

        List<Integer> randNumArray = randNumArrays.get(threadNum);
        int randNum = (randNumArray.get(counter) % (MAX_NUM_INDICES * NUM_POP_INSERTS));

        return payloads.get(randNum);
    }

    /*
    Generates a random value for the given Key.
    */
    Key<?> generate_key(int indexNum, int threadNum)
    {
        int counter = randNumCounter.get(threadNum);
        randNumCounter.set(threadNum, counter+1);

        List<Integer> randNumArray = randNumArrays.get(threadNum);
        int index = randNumArray.get(counter);

        KeyType type = indexTypes.get(indexNum);
        Key<?> key = null;

        switch (type) {
            case INT:
                index = index % (MAX_NUM_INDICES * NUM_POP_INSERTS);
                key = new Key<>(type,  (shortKeys.get(index) % (Integer.MAX_VALUE- 1)) + 1);
                break;
            case LONG:
                index = index % (MAX_NUM_INDICES * NUM_POP_INSERTS - 1);
                key = new Key<>(type, ((((long)intKeys.get(index) << 32) | (long)intKeys.get(index+1)) % (Long.MAX_VALUE - 1)) + 1);
                break;
            case STRING:
                index = index % (MAX_NUM_INDICES * NUM_POP_INSERTS);
                key = new Key<>(type, strKeys.get(index));
                break;
        }
        return key;
    }

    public int run(int seed) {
        System.out.println("Running the Speed Test, seed = " + seed);

        initialize(seed);

        long t0 = System.nanoTime();

        System.out.println("Creating " + nIndices + "indices.");

        List<Thread> createThreads = new ArrayList<>();
        for (int i = 0; i < nIndices; i++) {
            Thread t = new Thread(new CreateTestIndex(i));
            t.start();
            createThreads.add(t);
        }

        for (int i = 0; i < nIndices; i++) {
            try {
                createThreads.get(i).join();
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                return -1;
            }
        }

        System.out.println("Populating indices " + nIndices + ".");

        List<Thread> populateThreads = new ArrayList<>();
        for (int i = 0; i < MAX_NUM_INDICES; i++) {
            Thread t = new Thread(new Populate(i));
            t.start();
            populateThreads.add(t);

        }

        for (int i = 0; i < MAX_NUM_INDICES; i++) {
            try {
                populateThreads.get(i).join();
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                return -1;
            }
        }

        long t_end = System.nanoTime();
        System.out.println("Time to populate: " + ((double) (t_end - t0)) / 1000000 + " ms");

        long t2 = System.nanoTime();

        List<Thread> testExecutionThreads = new ArrayList<>();
        for (int i = 0; i < MAX_NUM_INDICES; i++) {
            Thread t = new Thread(new TestExecution(i));
            t.start();
            testExecutionThreads.add(t);
        }

        for (int i = 0; i < MAX_NUM_INDICES; i++) {
            try {
                testExecutionThreads.get(i).join();
            } catch (Exception ex) {
                System.out.println(ex.getMessage());
                return -1;
            }
        }

        t_end = System.nanoTime();
        System.out.println("Time to test: " + ((double) (t_end - t2)) / 1000000 + " ms");

        System.out.println("Testing complete.");
        System.out.println("NUM_DEADLOCK: " + NUM_DEADLOCK);
        System.out.println("NUM_TXN_FAIL: " + NUM_TXN_FAIL);
        System.out.println("NUM_TXN_COMP: " + NUM_TXN_COMP);

        t_end = System.nanoTime();

        System.out.println("Overall time to run: " + ((double) (t_end - t0)) / 1000000 + " milliseconds.");

        globalTime += ((double) (t_end - t0)) / 1000000;

        return 0;
    }

    @Test
    public void runTest() {
        System.out.println("speed_test called with " + NUM_POP_INSERTS + " populate inserts per thread and " + NUM_TESTS_PER_THREAD + " tests per thread");
        run(seed);

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("speed_test.results"));
            writer.write("NUM_DEADLOCK: " + NUM_DEADLOCK);
            writer.newLine();
            writer.write("NUM_TXN_FAIL: " + NUM_TXN_FAIL);
            writer.newLine();
            writer.write("NUM_TXN_COMP: " + NUM_TXN_COMP);
            writer.newLine();
            writer.write("TIME: " + globalTime);
            writer.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
}
