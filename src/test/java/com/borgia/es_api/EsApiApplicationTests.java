package com.borgia.es_api;

import com.alibaba.fastjson.JSON;
import com.borgia.es_api.common.Constant;
import com.borgia.es_api.model.po.User;
import com.borgia.es_api.model.vo.SearchResult;
import com.borgia.es_api.util.ElasticSearchHelper;
import com.borgia.es_api.util.ElasticSearchSynListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * 7.7.x api
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class EsApiApplicationTests {

  @Autowired
  private RestHighLevelClient restHighLevelClient;

  @Autowired
  private ElasticSearchHelper elasticSearchHelper;
  @Autowired
  private ElasticSearchSynListener elasticSearchSynListener;


  //创建索引 Request
  @Test
 public void testCreateIndex() throws IOException {
    // 1、创建索引请求
    CreateIndexRequest request = new CreateIndexRequest(Constant.ES_LIU_DOCUMENT_INDEX);
    //2、执行创建请求
    CreateIndexResponse createIndexResponse = restHighLevelClient.indices()
        .create(request, RequestOptions.DEFAULT);
  }

  // 异步->创建索引 并设置字段名和类型 Request
  @Test
 public void testSynCreateIndexMapping()  {

    try {
      // 1、创建索引请求
      CreateIndexRequest request = new CreateIndexRequest(Constant.ES_LIU_DOCUMENT_INDEX);
      //2、执行创建异步请求
      restHighLevelClient.indices().createAsync(request,RequestOptions.DEFAULT,elasticSearchSynListener.createIndexListener(Constant.ES_LIU_DOCUMENT_INDEX));
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


  // 异步-> 创建文档数据
  @Test
  public void testSynAddDocument(){
    try {
      String id = "2";
      //1.实例化索引请求 后面链式写法 追加文档数据
      User user = new User("小2", 2);

      IndexRequest indexRequest = new IndexRequest(Constant.ES_LIU_DOCUMENT_INDEX).id(id)
          //将我们的数据放入请求 json
          .source(JSON.toJSONString(user), XContentType.JSON);
      //等待主分片的时间,这里超过1秒代表超时 若超时则使用副分片
      indexRequest.timeout(TimeValue.timeValueSeconds(1));
      restHighLevelClient.indexAsync(indexRequest,RequestOptions.DEFAULT,elasticSearchSynListener.createDocumentListener(id));

      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }














  //检查索引 是否存在
  @Test
  public void testExistsIndex() throws IOException {
    GetIndexRequest request = new GetIndexRequest(Constant.ES_LIU_DOCUMENT_INDEX);
    boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
    System.out.println(exists);
  }

  // 删除索引
  @Test
  public void testDeleteIndex() throws IOException {
    DeleteIndexRequest request = new DeleteIndexRequest("kuang_index");
    AcknowledgedResponse delete = restHighLevelClient.indices()
        .delete(request, RequestOptions.DEFAULT);
    System.out.println(delete.isAcknowledged());
  }

  //添加文档
  @Test
  public void testAddDocument() throws IOException {
    //创建对象
    User user = new User("小1", 1);
//创建请求
    IndexRequest request = new IndexRequest(Constant.ES_LIU_DOCUMENT_INDEX);

    //规则 put /kuang_index/_doc/1
    request.id("1"); //文档id

    request.timeout(TimeValue.timeValueSeconds(1));//时间 1s

    //将我们的数据放入请求 json
    request.source(JSON.toJSONString(user), XContentType.JSON);

    //客户端发送请求 ， 获取响应的结果
    IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
    System.out.println(indexResponse.toString()); //返回文档信息
    System.out.println(indexResponse.status()); //操作文档类型
  }

  //判断文档是否存在
  @Test
  public void testExistsDocument() throws IOException {
    GetRequest getRequest = new GetRequest("kuang_index", "1");
    //不获取返回的 _source 的上下文了（只获取文档状态等 不获取具体内容）
    getRequest.fetchSourceContext(new FetchSourceContext(false));
    getRequest.storedFields("_none_");
    boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
    System.out.println(exists);
  }

  //获取文档信息
  @Test
  public void testGetDocument() throws IOException {
    GetRequest getRequest = new GetRequest("kuang_index", "1");

    GetResponse documentFields = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
    System.out.println(documentFields.getSourceAsString()); //打印文档内容
    System.out.println(documentFields);
  }

  //批量新增文档
  @Test
  public  void testBatchAddDocument() throws IOException {
    List<User> list = new ArrayList<>();

    for (int i = 0; i < 1000; i++) {
      list.add(new User("a1" + i + 2, i));
    }
    List<User> list2 = Collections.synchronizedList(new ArrayList<User>());
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.timeout("5s");
//		list.parallelStream().forEach(u->	System.out.println(u));
    list.parallelStream().forEach(list2::add);
    list2.stream().forEach(u -> System.out.println(u));

//
//		BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
//		System.out.println(bulkResponse.hasFailures());
  }

  /** 查询
   *  SearchRequest 搜索请求
   *  SearchSourceBuilder 条件构造
   *  HighlightBuilder 构建高亮
   *  TermQueryBuilder 精确查询
   *  MatchAllQueryBuilder
   *  xxx QueryBuilder 对应我们之前操作kibana看到的命令！
   */

  @Test
  public  void testSearch()  {
    SearchRequest searchRequest = new SearchRequest("liuy_doc");
// 构建搜索条件
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.highlighter();
// 查询条件，我们可以使用 QueryBuilders 工具来实现
// QueryBuilders.termQuery 精确
// QueryBuilders.matchAllQuery() 匹配所有
    MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name",
        "刘洋牛逼");
    QueryBuilders.matchAllQuery();
    sourceBuilder.query(matchQueryBuilder);
    sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
    searchRequest.source(sourceBuilder);
    SearchResult result = elasticSearchHelper.searchDocument(searchRequest);
    System.out.println(result);
    List<Map<String, Object>> list = result.getDocuments();
    list.forEach(m -> {
      System.out.println(m.get("name") + "," + m.get("age"));
    });
  }
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest,
//				RequestOptions.DEFAULT);
//		System.out.println(JSON.toJSONString(searchResponse.getHits()));
//		System.out.println("=================================");
//		for (SearchHit documentFields : searchResponse.getHits().getHits()) {
//			System.out.println(documentFields.getSourceAsMap());
//		}



}
