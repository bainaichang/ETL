package plugin.input;

import anno.Input;
import core.Channel;
import core.intf.IInput;
import tool.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Input(type = "access")
public class AccessInput implements IInput {
    @Override
    public void init(Map<String, Object> cfg) {
    
    }
    
    @Override
    public void start(List<Channel> output) throws Exception {
    
    }
}
