package elasticsearch7

import (
	"context"
	"strconv"

	"github.com/olivere/elastic/v7"
)

// 保存的数据

// 定义字段的关系映射
// 指定分词器 和分词规则

var (
	esClient *elastic.Client
	ctx      = context.Background()
	err      error
)

func init() {
	//实例化es客户端
	//SetSniff 检查集群中是否有其它可用节点
	esClient, err = elastic.NewClient(elastic.SetURL("http://127.0.0.1:9222"), elastic.SetSniff(false))
	if err != nil {
		//  no Elasticsearch node available
		// es没开或者没有指定连接地址
		panic(err)
	}
}

// 索引是否存在
// indexName 索引的名字
func ExistIndex(indexName string) (bool, error) {
	exists, err := esClient.IndexExists(indexName).Do(ctx)
	return exists, err
}

// 创建索引
// indexName 索引的名字
func CreateIndex(indexName string, mapping string) (*elastic.IndicesCreateResult, error) {

	createIndex, err := esClient.CreateIndex(indexName).BodyString(mapping).Do(ctx)

	return createIndex, err
}

// 向索引写入单条数据
func AddDocToIndex(indexName string, doc interface{}) (*elastic.IndexResponse, error) {
	put1, err := esClient.Index().
		Index(indexName).
		//Id(strconv.Itoa(doc.Id)).
		BodyJson(doc).
		Do(ctx)

	return put1, err
}

// 根据文档id查询数据
func SearchDocByDocID(indexName string, id int) (*elastic.GetResult, error) {
	// Get tweet with specified ID
	get1, err := esClient.Get().
		Index(indexName).
		Id(strconv.Itoa(id)).
		Do(ctx)
	return get1, err
}

// 词项精确查询,term是精确查询，字段类型keyword 不能是text
func TermQuery(indexName, field, value string, offset, limit int) (*elastic.SearchResult, error) {
	termQuery := elastic.NewTermQuery(field, value)
	searchResult, err := esClient.Search().
		Index(indexName).         // search in index "twitter"
		Query(termQuery).         // specify the query
		From(offset).Size(limit). // take documents 0-9
		Pretty(true).             // pretty print request and response JSON
		Do(ctx)                   // execute

	return searchResult, err
}

// 通过文档ID更改信息
func UpdateByDocId(indexName string, id int, doc interface{}) (*elastic.UpdateResponse, error) {
	do, err := esClient.Update().Index(indexName).Id(strconv.Itoa(id)).
		Doc(doc).
		Do(context.Background())

	return do, err
}
