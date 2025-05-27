package ETLTools.data;

import ETLTools.data.interFace.RowLive;
import ETLTools.data.interFace.RowSelect;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

public class RowSetTable {
    @Getter
    @Setter
    private String tbName;
    @Getter
    @Setter
    private List<Row> rowList = new ArrayList<>();
    private final List<String> field = new LinkedList<>();
    
    public RowSetTable(List<String> field) {
        this.field.addAll(field);
    }
    
    public void addRow(Row row) {
        if (row.size() == field.size()) {
            rowList.add(row);
        } else {
            System.err.println("数据行长度不对！应为" + field.size() + "个数据");
        }
    }
    
    public void addRow(List<Row> table) {
        for (Row row : table) {
            this.addRow(row);
        }
    }
    
    public void addRow(Row[] table) {
        for (Row row : table) {
            this.addRow(row);
        }
    }
    
    public RowSetTable where(RowLive compare) {
        RowSetTable result = new RowSetTable(field);
        for (Row row : rowList) {
            if (compare.reserve(row)) {
                result.addRow(row);
            }
        }
        return result;
    }
    
    public RowSetTable select(RowSelect selectField) {
        String[] newField = selectField.select(this.field);
        RowSetTable result = new RowSetTable(Arrays.asList(newField));
        Set<Integer> liveIndex = new HashSet<>();
        for (String field : newField) {
            if (this.field.contains(field)) {
                liveIndex.add(this.field.indexOf(field));
            }
        }
        for (Row oldRow : this.rowList) {
            Row newRow = new Row();
            for (int i = 0; i < oldRow.size(); i++) {
                if (liveIndex.contains(i)) {
                    newRow.add(oldRow.get(i));
                }
            }
            result.addRow(newRow);
        }
        return result;
    }
    
    public RowSetTable orderBy(Comparator<Row> compare) { // 传true代表升序
        RowSetTable result = new RowSetTable(this.field);
        result.addRow(this.rowList);
        result.getRowList()
              .sort(compare);
        return result;
    }
    
    public RowSetTable[] groupBy(RowSelect selectField) {
        List<RowSetTable> result = new ArrayList<>();
        // 未完成。。。
        return result.toArray(new RowSetTable[0]);
    }
    
    @Override
    public String toString() {
        StringBuffer builder = new StringBuffer();
        builder.append(String.join(", ", field))
          .append("\n");
        for (Row row : rowList) {
            Object[] array = row.toArray();
            for (int i = 0; i < array.length; i++) {
                builder.append(array[i]);
                if (i + 1 == array.length) {
                    builder.append("\n");
                    break;
                }
                builder.append(", ");
            }
        }
        return builder.toString();
    }
}
