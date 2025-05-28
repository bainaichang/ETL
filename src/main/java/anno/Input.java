// anno/Input.java
package anno;

import java.lang.annotation.*;

@MetaAnno(
        value = {"core.IInput"}
)
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Input {
    String type();
}
