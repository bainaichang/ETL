package runtask;

import java.util.ArrayList;
import java.util.List;

public class Step {
    protected Integer stepId;
    protected String description;
    protected String type;//大类-子模式
    protected String domain;
    protected List<String> parentStepId = new ArrayList<>();
    protected List<String> childStepId = new ArrayList<>();
    
    public Step(Integer stepId, String description, String type, String domain, List<String> parentStepId, List<String> childStepId) {
        this.stepId = stepId;
        this.description = description;
        this.type = type;
        this.domain = domain;
        this.parentStepId = parentStepId;
        this.childStepId = childStepId;
    }
    
    public Step() {
    }
    
    public Integer getStepId() {
        return stepId;
    }
    
    public void setStepId(Integer stepId) {
        this.stepId = stepId;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getDomain() {
        return domain;
    }
    
    public void setDomain(String domain) {
        this.domain = domain;
    }
    
    public List<String> getParentStepId() {
        return parentStepId;
    }
    
    public void setParentStepId(List<String> parentStepId) {
        this.parentStepId = parentStepId;
    }
    
    public List<String> getChildStepId() {
        return childStepId;
    }
    
    public void setChildStepId(List<String> childStepId) {
        this.childStepId = childStepId;
    }
}
