package com.gerry.elasticsearchTest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.gerry.elasticsearchTest.struct.scrollStruct;


public class Application {
    public static void main(String[] args){
        try {
            TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(
                            new TransportAddress(InetAddress.getByName("121.43.169.16"),49300));

//            deleteIndex(client,"test");
//            createIndex(client,"test");
//            createMapping(client,"test");
//
//
//            create(
//                    client,
//                    "test",
//                    "美国留给伊拉克的是个烂摊子吗",
//                    "新华社",
//                    "新华社讯，美国留给伊拉克的是个烂摊子吗",
//                    "1"
//            );
//            create(
//                    client,
//                    "test",
//                    "公安部：各地校车将享最高路权",
//                    "CNN",
//                    "公安部：各地校车将享最高路权",
//                    "2"
//            );
//
//            create(
//                    client,
//                    "test",
//                    "中韩渔警冲突调查：韩警平均每天扣1艘中国渔船",
//                    "BBC",
//                    "中韩渔警冲突调查：韩警平均每天扣1艘中国渔船",
//                    "3"
//            );
//
//            create(
//                    client,
//                    "test",
//                    "中国驻洛杉矶领事馆遭亚裔男子枪击 嫌犯已自首",
//                    "美联社",
//                    "中国驻洛杉矶领事馆遭亚裔男子枪击 嫌犯已自首",
//                    "4"
//            );
//
//            System.out.println(get(client,"test","1"));
//            System.out.println(get(client,"test","2"));
//            System.out.println(get(client,"test","3"));
//            System.out.println(get(client,"test","4"));

//            create(
//                    client,
//                    "test",
//                    "中国国歌",
//                    "知乎",
//                    "中国的国歌是义勇军进行曲",
//                    "5"
//            );
//            create(
//                    client,
//                    "test",
//                    "中国国庆节",
//                    "知乎",
//                    "中国的国庆节是十月一日",
//                    "6"
//            );
//            search(client,"test","中国");

//            scrollStruct stru=getScrollSearch(
//                    client,
//                    "test",
//                    "中国",
//                    1,
//                    5
//            );
//
//            for(String s:stru.list){
//                System.out.println(s);
//            }



            client.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void createIndex(TransportClient client,String index){
        client.admin().indices().create(new CreateIndexRequest(index)).actionGet();
        // waitForYellow
        client.admin().cluster()
                .health(new ClusterHealthRequest(index)
                .waitForYellowStatus())
                .actionGet();
    }

    public static void createMapping(TransportClient client,String index) throws IOException{
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")

                .startObject("title")
                .field("type", "text")
                .field("analyzer", "ik_max_word")
                .field("search_analyzer", "ik_max_word")
                .field("boost", 3)
                .endObject()

                .startObject("author")
                .field("type", "keyword")
                .field("boost", 1)
                .endObject()

                .startObject("content")
                .field("type", "text")
                .field("analyzer", "ik_max_word")
                .field("search_analyzer", "ik_max_word")
                .field("boost", 2)
                .endObject()

                .endObject()
                .endObject();
        PutMappingRequest mapping = Requests.putMappingRequest(index)
                .type("fulltext")
                .source(builder);

//        System.out.println(
//                mapping.source().toString()
//        );

        client.admin().indices().putMapping(mapping).actionGet();
    }

    public static void deleteIndex(TransportClient client,String index) {
        DeleteIndexResponse deleteIndexResponse =
                client
                        .admin()
                        .indices()
                        .prepareDelete(index)
                        .get();
      //  System.out.println(deleteIndexResponse.isAcknowledged()); // true表示成功
    }

    public static void create(
            TransportClient client,
            String index,
            String title,
            String author,
            String content,
            String id
    )
            throws IOException, InterruptedException, ExecutionException
    {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("title", title)
                .field("author", author)
                .field("content", content)
                .endObject();
        IndexResponse indexResponse = client
                .prepareIndex(
                        index,
                        "fulltext",
                        id
                )
                .setSource(builder)
                .execute().get();
        //.execute().get();和get()效果一样
       // System.out.println(indexResponse.getVersion());
    }

    public static void update(
            TransportClient client,
            String index,
            String id,
            String title,
            String author,
            String content
    ) {
        try {
            XContentBuilder builder =
                    XContentFactory.jsonBuilder()
                            .startObject()
                            .field("title", title)
                            .field("author", author)
                            .field("content", content)
                            .endObject();

            UpdateResponse updateResponse =
                    client.prepareUpdate()
                            .setIndex(index)
                            .setType("fulltext")
                            .setId(id)
                            .setDoc(builder.string())
                            .get();
            System.out.println(updateResponse.getIndex()); // true表示成功
        } catch (ElasticsearchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String get(
            TransportClient client,
            String index,
            String id
    ) {
        GetResponse getResponse = client
                .prepareGet()   // 准备进行get操作，此时还有真正地执行get操作。（与直接get的区别）
                .setIndex(index)  // 要查询的
                .setType("fulltext")
                .setId(id)
                .get();
        return getResponse.getSourceAsString();
    }

    public static void delete(
            TransportClient client,
            String index,
            String id
    )
    {
        DeleteResponse deleteResponse =
                client.prepareDelete(
                        index,
                        "fulltext",
                        id
                ).get();

        System.out.println(deleteResponse.getVersion());

        //删除所有记录
        client.prepareDelete(index,"fulltext",id).get();
    }

    /**
     * 简易分页搜索
     * @param client
     * @param index
     * @param keyword
     */
    public static void search(
            TransportClient client,
            String index,
            String keyword
    )
    {
        SearchResponse response = client.prepareSearch(index)
                .setTypes("fulltext")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(
                        QueryBuilders.boolQuery().should(
                                QueryBuilders.matchQuery("title", keyword)
                        )
                        .should(
                                QueryBuilders.matchQuery("author", keyword)
                        )
                        .should(
                                QueryBuilders.matchQuery("content", keyword)
                        )
                )
                .setFrom(0).setSize(60).setExplain(true)
                .get();

        SearchHits hits = response.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            System.out.println(hits.getHits()[i].getSourceAsString());
            System.out.println(hits.getHits()[i].getId());
        }
    }


    /**
     * 得到总数
     * @param client
     * @param index
     * @param keyword
     * @return
     */
    public static long searchCount(
            TransportClient client,
            String index,
            String keyword
    )
    {
        SearchResponse response = client.prepareSearch(index)
                .setTypes("fulltext")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(
                        QueryBuilders.boolQuery().should(
                                QueryBuilders.matchQuery("title", keyword)
                        )
                                .should(
                                        QueryBuilders.matchQuery("author", keyword)
                                )
                                .should(
                                        QueryBuilders.matchQuery("content", keyword)
                                )
                )
                .get();

        return response.getHits().getTotalHits();
    }

    /**
     * 得到scrollid
     * @param client
     * @param index
     * @param keyword
     * @param size
     * @param showdetail
     * @return
     */
    public static scrollStruct getScrollSearchId(
            TransportClient client,
            String index,
            String keyword,
            int size,
            boolean showdetail
    )
    {
        scrollStruct stru=new scrollStruct();

        SearchResponse response = client.prepareSearch(index)
                .setTypes("fulltext")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setScroll(TimeValue.timeValueMinutes(1))
                .setQuery(
                        QueryBuilders.boolQuery().should(
                                QueryBuilders.matchQuery("title", keyword)
                        )
                                .should(
                                        QueryBuilders.matchQuery("author", keyword)
                                )
                                .should(
                                        QueryBuilders.matchQuery("content", keyword)
                                )
                )
                .setSize(size)
                .get();
        stru.total=response.getHits().getTotalHits();
        stru.scrollid=response.getScrollId();
        stru.page=1;

        if(showdetail){
            SearchHits hits = response.getHits();
            for (int i = 0; i < hits.getHits().length; i++) {
                stru.list.add(
                        hits.getHits()[i].getSourceAsString()
                );
            }
        }

        return stru;
    }

    /**
     * scrollSearch分页
     * @param client
     * @param scrollid
     */
    public static scrollStruct getScrollSearch(
            TransportClient client,
            String scrollid,
            int page,
            boolean showdetail
    )
    {
        scrollStruct stru=new scrollStruct();

        SearchResponse response = client.prepareSearchScroll(scrollid)
                .setScroll(TimeValue.timeValueMinutes(1))
                .get();

        stru.total=response.getHits().getTotalHits();
        stru.scrollid=response.getScrollId();
        stru.page=page;

        if(showdetail){
            SearchHits hits = response.getHits();
            for (int i = 0; i < hits.getHits().length; i++) {
                stru.list.add(
                        hits.getHits()[i].getSourceAsString()
                );
            }
        }

        return stru;
    }


    /**
     * 分页搜索
     * @param client
     * @param index
     * @param keyword
     * @param size
     * @param page
     * @return
     */
    public static scrollStruct getScrollSearch(
            TransportClient client,
            String index,
            String keyword,
            int size,
            int page
    )
    {
        scrollStruct stru=getScrollSearchId(
                client,
                index,
                keyword,
                size,
                page==1
        );

        while (stru.page!=page){
            stru=getScrollSearch(
                    client,
                    stru.scrollid,
                    stru.page+1,
                    (stru.page+1)==page
            );
        }

        return stru;
    }
}
