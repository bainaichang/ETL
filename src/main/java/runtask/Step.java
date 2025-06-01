package runtask;

import java.util.ArrayList;
import java.util.List;

public class Step {
    protected Integer stepId;
    protected String description;
    protected String subType;//子类型
    protected String config;//具体配置
    protected String domain;//主类型
    protected List<String> parentStepId = new ArrayList<>();//上游ID
    protected List<String> childStepId = new ArrayList<>();//下游ID
    
    public Step(Integer stepId, String description, String subType, String domain, List<String> parentStepId, List<String> childStepId) {
        this.stepId = stepId;
        this.description = description;
        this.subType = subType;
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
    
    public String getSubType() {
        return subType;
    }
    
    public void setSubType(String type) {
        this.subType = type;
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
    public String getConfig() {
        return config;
    }
    public void setConfig(String config) {
        this.config = config;
    }
    
    public List<String> getChildStepId() {
        return childStepId;
    }
    
    public void setChildStepId(List<String> childStepId) {
        this.childStepId = childStepId;
    }
}
