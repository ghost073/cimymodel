# cimymodel
ci3.X版本对数据库crud操作封装，加入redis缓存操作，mysql,redis使用长链接，减少重新链接造成的性能消耗
只需简单配置就可以对数据库单条数据进行缓存使用，不用写set,get方法

# 文件目录说明
third_party/mymodel  是封装的文件目录，
其中MY_Model 使用时放在ci application/core/ 目录下，注意修改xredis.php的引入地址
redis.ini为redis服务器配置
rediskey.php 是项目中所有用到的rediskey值配置

my_model.php 中用到的自定义函数可以在 application_func.php 中找到，自行放置在合适位置


ad_model , 为示例model文件使用方法
