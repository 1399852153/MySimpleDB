package simpledb.exception;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class ParseException extends RuntimeException{

    public ParseException(String message) {
        super(message);
    }

    public ParseException(String message,Exception e) {
        super(message,e);
    }
}
