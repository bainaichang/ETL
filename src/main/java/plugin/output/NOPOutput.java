package plugin.output;

import anno.Output;
import core.Channel;
import core.intf.IOutput;

import java.util.Map;

@Output(type = "nop")
public class NOPOutput implements IOutput {
    @Override
    public void init(Map<String, Object> cfg) {
    
    }
    
    @Override
    public void consume(Channel input) throws Exception {
    
    }
}
