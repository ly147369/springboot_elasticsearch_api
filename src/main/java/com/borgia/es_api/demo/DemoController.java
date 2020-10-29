package com.borgia.es_api.demo;

import com.borgia.es_api.common.Constant;
import com.borgia.es_api.model.vo.FilterBean;
import com.borgia.es_api.model.vo.QueryBean;
import com.borgia.es_api.model.vo.ESFilterWrapper;
import com.borgia.es_api.model.vo.ESUpdateWrapper;
import com.borgia.es_api.model.vo.SearchResult;
import com.borgia.es_api.util.ElasticSearchHelper;
import com.borgia.es_api.util.ElasticSearchSynListener;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.Mapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 示例
 */
@RestController
@RequestMapping("/query")
public class DemoController {


    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private ElasticSearchHelper elasticSearchHelper;
    @Autowired
    private ElasticSearchSynListener elasticSearchSynListener;

    /**
     * 检索示例
     * @param queryBean
     * @param filterBean
     * @return
     */
    public SearchResult keyword(QueryBean queryBean, FilterBean filterBean){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        List<QueryBuilder> filterList = boolQueryBuilder.filter();
        filterList.addAll(new ESFilterWrapper().filter(filterBean));

        List<QueryBuilder> boolmustList = boolQueryBuilder.must();
        boolmustList.add(QueryBuilders.matchQuery("title",queryBean.getKeyword()));
        
        searchSourceBuilder.query(boolQueryBuilder);
        
        return requestResult(queryBean,searchSourceBuilder);
    }


    /**
     * 更新示例
     * @param updateWrapper
     * @throws IOException
     */
    public void lambdaUpdateDocument(ESUpdateWrapper<?> updateWrapper) throws IOException {
        if (StringUtils.isBlank(updateWrapper.getIndex())){
            updateWrapper.index("indexName");
        }
        elasticSearchHelper.lambdaUpdateDocument(updateWrapper);
    }


    private SearchResult requestResult(QueryBean queryBean,SearchSourceBuilder searchSourceBuilder) {
        SearchRequest firstSearchRequest = new SearchRequest("indexName");
        searchSourceBuilder.trackTotalHits(true); //设置返回总数
        searchSourceBuilder.trackScores(true);
        searchSourceBuilder.size(queryBean.getSize()).from((queryBean.getPage()-1)*queryBean.getSize());
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        /*返回字段*/
        searchSourceBuilder.fetchSource(new String[]{"title"}, Strings.EMPTY_ARRAY);
        /*排序*/
        searchSourceBuilder.sort(new FieldSortBuilder(queryBean.getSortField()).order(queryBean.getSortOrder()));
        /*学科聚合*/
        searchSourceBuilder.aggregation(AggregationBuilders.terms("suject_count").field("suject_code").size(100000).order(BucketOrder.key(true)));
        firstSearchRequest.source(searchSourceBuilder);
        SearchResult searchResult = elasticSearchHelper.searchDocument(firstSearchRequest);
        return searchResult;
    }



    @GetMapping("/s")
    public void  s (){
//        XContentBuilder builder = null;
//        try {
//            builder = XContentFactory.jsonBuilder();
//            builder.startObject();
//            {
//                builder.field("user", "kimchy");
//                builder.timeField("postDate", new Date());
//                builder.field("message", "trying out Elasticsearch");
//            }
//            builder.endObject();
//            // 1、创建索引请求
//            CreateIndexRequest request = new CreateIndexRequest(Constant.ES_LIU_DOCUMENT_INDEX);
//            request.setTimeout(TimeValue.timeValueSeconds(5));
//            restHighLevelClient.indices().createAsync(request,RequestOptions.DEFAULT,elasticSearchSynListener.createIndexListener(Constant.ES_LIU_DOCUMENT_INDEX));
//
////    request.opType(DocWriteRequest.OpType.INDEX);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        // 1、创建索引请求
        CreateIndexRequest request = new CreateIndexRequest(Constant.ES_LIU_DOCUMENT_INDEX);
        Map<String, Object> message = new HashMap<>();
        message.put("type", "keyword");

        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);

        //2、执行创建请求
        restHighLevelClient.indices().createAsync(request,RequestOptions.DEFAULT,elasticSearchSynListener.createIndexListener(Constant.ES_LIU_DOCUMENT_INDEX));

    }


}
