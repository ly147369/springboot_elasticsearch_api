package com.borgia.es_api.util;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ElasticSearchSynListener {
  /**
   * 异步创建index
   * @return
   */
  public static ActionListener<CreateIndexResponse> createIndexListener(String name) {
    ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
      @Override
      public void onResponse(CreateIndexResponse createIndexResponse) {
        System.out.println("创建索引 "+name+" 成功");
      }

      @Override
      public void onFailure(Exception e) {
        log.error("Asynchronous batch increases data exceptions：{}", e.getLocalizedMessage());
      }
    };
    return listener;
  }


  /**
   * 异步添加文档Document
   * @return
   */
  public static ActionListener<IndexResponse> createDocumentListener(String id) {
    ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
      @Override
      public void onResponse(IndexResponse createIndexResponse) {
        System.out.println("创建文档成功,id: "+id);
      }

      @Override
      public void onFailure(Exception e) {
        log.error("Asynchronous batch increases data exceptions：{}", e.getLocalizedMessage());
      }
    };
    return listener;
  }
}
