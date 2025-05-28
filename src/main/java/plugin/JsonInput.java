package plugin;

import anno.Input;
import core.intf.IInput;

@Input(type = "json")
public class JsonInput implements IInput {
    @Override
    public void deal(Object data) {
        System.out.println("读取json文件");
        System.out.println(data);
    }
}
