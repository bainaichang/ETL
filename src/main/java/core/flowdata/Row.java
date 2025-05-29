package core.flowdata;

import java.util.ArrayList;

public class Row extends ArrayList<Object> {
    public Object get(String field, RowSetTable table) {
        int index = table.getFieldIndex(field);
        if (index == -1) {
            System.err.println("字段不存在！");
            return null;
        }
        return this.get(index);
    }
}
