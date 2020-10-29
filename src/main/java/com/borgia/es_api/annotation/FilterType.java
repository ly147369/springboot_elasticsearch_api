package com.borgia.es_api.annotation;

import com.borgia.es_api.enums.FType;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FilterType {

    /**
     * 过滤类型
     * @see FType
     * @return
     */
    FType value() default FType.STRING;

    /**
     * 忽略过滤 如果设置此值，为此值则跳过
     * @return
     */
    String ignoreValue() default "";


}
