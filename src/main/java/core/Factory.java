package core;

import anno.Input;
import cn.hutool.core.util.ClassUtil;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Factory {
    private static final String PKG= "plugin";
    private final Map<String,Class<?>> reg=new HashMap<>();

    private void init(){
        Class<? extends Annotation>[] annos=new Class[]{
                Input.class
        };
        for(Class<? extends Annotation>anno:annos)
            scan(anno,PKG);
    }

    private void scan(Class<? extends Annotation>anno,String pkg){
        Set<Class<?>> list= ClassUtil.scanPackageByAnnotation(pkg,anno);
        for(Class<?> cls:list){
            Annotation tag=cls.getAnnotation(anno);
            try {
                String type = (String) anno.getMethod("type").invoke(tag);
                reg.put(type, cls);
            }catch (Exception e){
                System.out.println("错,获取注解类方法有误");
            }
        }
    }

    public Factory(){
        init();
    }
    public void newPlugin(String type,Object data){
        Class<?> cls=reg.get(type);
        try {
            Object ins = cls.getDeclaredConstructor().newInstance();
            cls.getMethod("deal",Object.class).invoke(ins,data);
        }catch (Exception e){
            System.out.println("错，反射方法有误");
        }
    }
}