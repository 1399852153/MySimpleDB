package simpledb.dbpage.btree;

/**
 * @author xiongyx
 * @date 2021/2/12
 */
public enum BTreePageCategoryEnum {
    ROOT_PTR(0,"根节点指针页"),
    INTERNAL(1,"内部节点页"),
    LEAF(2,"叶子节点页"),
    HEADER(3,"头结点页"),
    ;

    private final int value;
    private final String message;

    BTreePageCategoryEnum(int value, String message) {
        this.value = value;
        this.message = message;
    }

    public int getValue() {
        return value;
    }

    public String getMessage() {
        return message;
    }

}
