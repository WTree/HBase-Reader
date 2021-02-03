使用这个库，读取HBase时可以像JPB的方式一样去获取数据，使用api相关的工程

使用教程

1、使用前的准备要配置好HBase 集群的连接

2、配置仓库地址

[![](https://jitpack.io/v/WTree/HBase-Reader.svg)](https://jitpack.io/#WTree/HBase-Reader)

```
	<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>
	
	
	
	<dependency>
	    <groupId>com.github.WTree</groupId>
	    <artifactId>HBase-Reader</artifactId>
	    <version>v0.1</version>
	</dependency>
	 
``` 

3、配置要读取的实体

``` 
@HBaseInfo(tableName = "product", tableNameSpace = "ay")
public class ProductInfo implements IFieldCheck, Serializable {

    //family 默认值是r 如果是这个可以不配置
    @HBaseFieldName(PackType = EnumPack.PACK_BYTES, HBaseKey = "i", family = "e")
    private String id;
    
    @HBaseFieldName(PackType = EnumPack.PACK_BYTES, HBaseKey = "t", family = "e")
    private String title;
    
    @HBaseFieldName(PackType = EnumPack.PACK_BYTES, HBaseKey = "p", family = "e")
    private String price;
    
    //这个为了允许实体里面的部分字段为空
    @Override
    public boolean isEmptyValue() {
        return StringUtils.isEmpty(id);
    }

```

4、读取

```
  byte[] rowKey = DigestUtils.sha1Hex("34514").getBytes();
  //这个callBack是为了只读取部分字段而设置的
  IGetFieldCallBack callBack = () -> new String[]{"id", "price"};
  //getConnection() 自己集群的连接池
  ProductInfo info = HBaseReadHelper.read(getConnection(), ProductInfo.class, callBack, rowKey);

  //如果想把几个想读的字段直接转成JSON 对象，可以使用这个
  BeanFactoryInterceptorHelper.intercept(info, callBack.fieldValue())
  
```