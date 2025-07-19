package tool.database;
/**
 *
 * 该类是数据库json的类
 * */
public class DataBase {
    private String name;
    private String type;
    private Config config;
    
    public DataBase(String name, String type, Config config) {
        this.name = name;
        this.type = type;
        this.config = config;
    }
    
    public DataBase() {
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public Config getConfig() {
        return config;
    }
    
    public void setConfig(Config config) {
        this.config = config;
    }
}
