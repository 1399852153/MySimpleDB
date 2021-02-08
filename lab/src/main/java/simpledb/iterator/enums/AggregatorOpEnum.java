package simpledb.iterator.enums;

/**
 * @author xiongyx
 * @date 2021/2/7
 */
public enum AggregatorOpEnum {

    MIN("MIN"),
    MAX("MAX"),
    SUM("SUM"),
    AVG("AVG"),
    COUNT("COUNT")
    ;

    private final String view;

    AggregatorOpEnum(String view) {
        this.view = view;
    }

    @Override
    public String toString() {
        return "AggregatorEnum{" +
                "view='" + view + '\'' +
                '}';
    }
}
