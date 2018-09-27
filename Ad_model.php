<?php defined('BASEPATH') OR exit('No direct script access allowed');
/**
 * 广告Model
 * @author 
 * @date   2017/05/11
 */
class Ad_model extends MY_Model {

    protected $pk               = 'ad_id';  // 主键ID
    protected $table_name       = 'ad'; // 表名
    protected $active_group     = 'default'; // 使用的mysql实例
    protected $redis_detail_key = 'git:ad:detail:(id)';     // rediskey 名
    protected $redis_group      = 'redis127';   // redis服务器

    /**
     * 构造函数
     */
    public function __construct() {
        parent::__construct();
    }
}