package simpledb.exception;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class DBException extends RuntimeException{

    public DBException(String message) {
        super(message);
    }

    public DBException(String message, Throwable cause) {
        super(message, cause);
    }
}
