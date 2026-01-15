package ppds;

import ppds.Types.KeyType;

/**
 * Stores the key value, whether it is an int, a long or a String.
 *
 * @type defines what kind of key it is
 */
public class Key<T> {
    private KeyType _type;
    private T _keyval;

    public Key(KeyType keytype, T keyval) {
        _type = keytype;
        _keyval = keyval;
    }

    public T getKeyval() {
        return _keyval;
    }

    public KeyType getKey() {
        return _type;
    }
}
