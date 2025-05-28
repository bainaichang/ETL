package org.gugu.etl;

import ETLTools.data.Row;
import ETLTools.data.RowSetTable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;

public class RowSetTableTest {
    @Test
    public void test_1() {
        RowSetTable table = new RowSetTable(Arrays.asList("id", "age", "sex"));
        Row row1 = new Row();
        row1.addAll(Arrays.asList("0001", "18", "man"));
        Row row2 = new Row();
        row2.addAll(Arrays.asList("0002", "20", "woman"));
        Row row3 = new Row();
        row3.addAll(Arrays.asList("0003", "30", "man"));
        Row row4 = new Row();
        row4.addAll(Arrays.asList("0004", "27", "woman"));
        Row row5 = new Row();
        row5.addAll(Arrays.asList("0005", "20", "woman"));
        table.addRow(new Row[]{row1, row2, row3, row4, row5});
        table = table.select(header -> new String[]{header.get(0), header.get(1)})
                     .orderBy(Comparator.comparingInt(l -> Integer.parseInt((String) l.get(1))));
        System.out.println(table);
    }
}
