package ETLTools.data.interFace;

import ETLTools.data.Row;

public interface RowLive {
    // 返回true表示要这条数据，反之不要
    boolean reserve(Row row);
}
