package com.borgia.es_api.model.vo;

import com.borgia.es_api.annotation.FilterType;
import com.borgia.es_api.enums.FType;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


@Slf4j
public class ESFilterWrapper implements Serializable {


    public <E> List<QueryBuilder> filter(E filterObj) {
        try {
            List<QueryBuilder> filterList = new ArrayList<>();
            Field[] fields = filterObj.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                String value = (String) field.get(filterObj);
                if (StringUtils.isBlank(value)) {
                    continue;
                }
                FilterType filterType = field.getAnnotation(FilterType.class);
                FType current = filterType != null ? filterType.value() : FType.STRING;
                String ignoreValue = filterType == null ? "" : filterType.ignoreValue();
                if (ignoreValue.equals(value)) {
                    continue;
                }
                if (current == FType.STRING) {
                    filterList.add(QueryBuilders.termQuery(field.getName(), value));
                } else if (current == FType.DATE) {
                    String[] dateRange = value.split("#");
                    filterList.add(QueryBuilders.rangeQuery(field.getName()).from(dateRange[0]).to(dateRange[1]));
                } else if (current == FType.ARRAY) {
                    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                    String[] array = value.split(",");
                    Arrays.stream(array).forEach(a -> {
                        filterList.add(boolQueryBuilder.should(QueryBuilders.termQuery(field.getName(), a)));
                    });
                    filterList.add(boolQueryBuilder);
                }
            }
            return filterList;
        } catch (IllegalAccessException i) {
            log.error("filterwrapper exception:{}", i.getLocalizedMessage());
        }
        return Collections.EMPTY_LIST;
    }

}
