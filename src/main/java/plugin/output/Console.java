package plugin.output;

import anno.Output;
import core.Channel;
import core.flowdata.RowSetTable;
import core.intf.IOutput;

import java.util.Map;

@Output(type = "console")
public class Console implements IOutput {
    @Override
    public void init(Map<String, Object> config) {}

    @Override
    public void consume(Channel input) throws Exception {
        System.out.println();
        RowSetTable header = input.getHeader();
        if (header != null) {
            System.out.println("[Console] 表头: " +input.hashCode()+"|" + header.getField());
        } else {
            System.out.println("[Console] 无表头信息");
        }

        input.subscribe(row -> {
            System.out.println("[Console] 数据: "+input.hashCode()+"|"+ row);
        });
    }
}
