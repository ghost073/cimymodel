<?php defined('BASEPATH') OR exit('No direct script access allowed');
/**
 * 广告Model
 * @author 
 * @date   2017/05/11
 */
class Ad_model extends MY_Model {

    protected $pk               = 'ad_id';
    protected $table_name       = 'ad';
    protected $active_group     = 'default';
    protected $redis_detail_key = 'git:ad:detail:(id)';    
    protected $redis_group      = 'redis127';

    /**
     * 构造函数
     */
    public function __construct() {
        parent::__construct();
    }
}