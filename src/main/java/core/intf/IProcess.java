package core.intf;

import core.Channel;
import core.flowdata.Row;

import java.util.List;
import java.util.Map;

public interface IProcess {
    void init(Map<String,Object> cfg);
    void process(Channel input, List<Channel> outputs) throws Exception;
}