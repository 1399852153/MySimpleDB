package simpledb.iterator.enums;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public enum OperatorEnum {

    EQUALS("="),
    GREATER_THAN(">"),
    LESS_THAN("<"),
    LESS_THAN_OR_EQ("<="),
    GREATER_THAN_OR_EQ(">="),
    LIKE("%LIKE%"),
    LEFT_LIKE("%LIKE"),
    RIGHT_LIKE("LIKE%"),
    NOT_EQUALS("<>");

    OperatorEnum(String view) {
        this.view = view;
    }

    private String view;

    public String getView() {
        return view;
    }

    @Override
    public String toString() {
       return this.view;
    }
}
