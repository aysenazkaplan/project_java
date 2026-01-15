package ppds;

public class Types {
    /**
     * Status messages for outcomes of API calls.
     */
    public enum ErrCode {
        SUCCESS,
        DB_DNE,
        DB_EXISTS,
        DB_END,
        KEY_NOTFOUND,
        TXN_EXISTS,
        TXN_DNE,
        ENTRY_EXISTS,
        ENTRY_DNE,
        DEADLOCK,
        FAILURE
    }

    /**
     * Three possible key types.
     */
    public enum KeyType {
        INT,
        LONG,
        STRING
    }
}
