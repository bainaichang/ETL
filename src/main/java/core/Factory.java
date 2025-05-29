package core;

import anno.Input;
import cn.hutool.core.util.ClassUtil;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Factory {
    private static final String PKG = "plugin";
    private final Map<String, Class<?>> reg = new HashMap<>();
    private final Map<String, Object> pluginCache = new ConcurrentHashMap<>();

    public Factory() {
        init();
    }

    private void init() {
        Class<? extends Annotation>[] annos = new Class[]{
                Input.class,
        };
        for (Class<? extends Annotation> anno : annos) {
            scan(anno);
        }
    }

    private void scan(Class<? extends Annotation> anno) {
        Set<Class<?>> classes = ClassUtil.scanPackageByAnnotation(PKG, anno);
        for (Class<?> cls : classes) {
            Annotation tag = cls.getAnnotation(anno);
            try {
                String type = (String) anno.getMethod("type").invoke(tag);
                reg.put(type, cls);
            } catch (Exception e) {
                System.out.println("错,获取注解类方法有误");
            }
        }
    }

    public void newPlugin(String type, Object data) {
        Object plugin = pluginCache.computeIfAbsent(type, t -> {
            Class<?> cls = reg.get(t);
            try {
                return cls.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                System.out.println("错，缓存反射方法有误");
                return null;
            }
        });

        if (plugin == null) return;

        try {
            plugin.getClass().getMethod("deal", Object.class).invoke(plugin, data);
        } catch (Exception e) {
            System.out.println("错，反射方法有误");
        }
    }
}
