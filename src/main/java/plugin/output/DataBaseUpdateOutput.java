package plugin.output;

import anno.Output;
import cn.hutool.json.JSONUtil;
import core.Channel;
import core.flowdata.RowSetTable;
import core.intf.IOutput;
import tool.Log;
import tool.database.DataBase;
import tool.database.DataBaseTool;

import java.util.HashMap;
import java.util.Map;

@Output(type = "update")
public class DataBaseUpdateOutput implements IOutput {
    private DataBaseTool db = new DataBaseTool();
    private String tableName;
    private RowSetTable table;
    
    @Override
    public void init(Map<String, Object> cfg) {
        String connectionId = (String) cfg.get("connectionId");
        this.tableName = (String) cfg.get("tableName");
        db.setConnectionId(connectionId);
        try {
            this.table = db.select("select * from " + tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void consume(Channel input) throws Exception {
        input.subscribe(rowSetTableObj -> {
            if (! (rowSetTableObj instanceof RowSetTable)) {
                Log.warn("TableOutput", "Received non-RowSetTable object from channel, skipping.");
                return;
            }
            RowSetTable table = (RowSetTable) rowSetTableObj;
            RowSetTable insert = table.except(this.table);
            for (String sql : insert.getInsertSQL(this.tableName)) {
                // 插入新的数据
                try {
                    this.db.exec(sql);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            for (String sql : table.intersect(this.table)
                                   .getUpdate(this.tableName)) {
                try {
                    this.db.exec(sql);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
