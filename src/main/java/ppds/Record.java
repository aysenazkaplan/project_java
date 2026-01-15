package ppds;

/**
 * The record information stored in an index
 *
 * @value key: The lookup key for the record.
 * @value payload: The value stored under that key. It will be a
 * string of no more than 128 bytes.
 */
public class Record {
    /**
     * Specifies the maximum size for a payload.
     */
    static int MAX_PAYLOAD_LEN = 100;
    private Key<?> _key;
    private String _payload;

    public Record(Key<?> key) {
        _key = key;
    }

    public Record(Key<?> key, String payload) {
        _key = key;
        _payload = payload;
    }

    public void clearPayload() {
        _payload = null;
    }

    public Key<?> getKey() {
        return _key;
    }

    public String getPayload() {
        return _payload;
    }

    public void setKey(Key<?> key) {
        _key = key;
    }

    public void setPayload(String payload) {
        _payload = payload;
    }
}

