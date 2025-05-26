package RunTask;

import java.util.ArrayList;
import java.util.List;

public class StepList {
    List<Step> data = new ArrayList<>();
    
    public List<Step> getData() {
        return data;
    }
    
    public void setData(List<Step> data) {
        this.data = data;
    }
    
    public StepList(List<Step> data) {
        this.data = data;
    }
    
    public StepList() {
    }
}
