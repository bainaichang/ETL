package core.intf;

import ETLTools.data.Row;

import java.util.List;

public interface RowSelect {
    // 返回你需要保留的字段
    String[] select(List<String> field);
}
