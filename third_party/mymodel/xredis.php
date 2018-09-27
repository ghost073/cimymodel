<?php defined('BASEPATH') OR exit('No direct script access allowed');
/**
 * 封装Redis
 * 1
 *
 */

include_once 'redisconf.php';
class XRedis {
    //是否已连接redis标记
    public $connect_flag = NULL;
    // 当前选择的数据库
    private $db_id = 0;

    /**
     * 初始化连接redis
     * 
     */
    public function __construct($cache=true) {
        //是否取缓存标记 默认取缓存
        $this->is_cache = $cache;
    }
    /**
     * 自动关闭redis
     */
    public function __destruct() {
        //$this->getRedis()->close();
        return;
    }

    #########################
    # redis自身操作  #               
    #########################
    /**
     * 关闭redis
     */
    public function close() {
        $this->redis->close();
    }

    /**
     * 通用连接方法
     *
     * @param string $confkey redis配置key
     *
     * @param int    $master  1|0 1代表主 0代表从
     *
     * @return 
     *
     */
    public function mconnect($confkey, $master=1) {
        $this->redis = new Redis();
        $redisconf = $this->getConfig($confkey);
        $host      = $master ? $redisconf['HOST'] : $redisconf['RHOST'];
        $port      = $master ? $redisconf['PORT'] : $redisconf['RPORT'];
        $timeout   = $master ? $redisconf['TIMEOUT'] : $redisconf['RTIMEOUT'];
        $selectdb  = $master ? $redisconf['SELECTDB'] : $redisconf['RSELECTDB'];
        $auth      = $master ? $redisconf['AUTH'] : $redisconf['RAUTH'];

        try {
            $ok   = $this->redis->connect($host, $port, $timeout);
            $this->redis->auth($auth);
            $this->redis->select($selectdb);   
            // 当前数据库ID
            $this->db_id = $selectdb;   
        }
        catch (Exception $e)
        {
            $log_str = date('Y-m-d H:i:s').'| 错误：'.$e->getMessage(). "\n";
            // 操作写入日志
            force_write_file(APPPATH.'logs/redis.log', $log_str, 'a+');
            die('can not connect redis '.$confkey);
        }

        //已连接标记
        $this->connect_flag = true;
        return $this->redis;
    }

    /**
     * pconncet
     * @param string $confkey redis配置key
     *
     * @param int    $master  1|0 1代表主 0代表从
     *
     * @return 
     *
     */
    public function mpconnect($confkey, $master=1) {
        $this->redis = new Redis();
        $redisconf = $this->getConfig($confkey);
        $host      = $master ? $redisconf['HOST'] : $redisconf['RHOST'];
        $port      = $master ? $redisconf['PORT'] : $redisconf['RPORT'];
        $timeout   = $master ? $redisconf['TIMEOUT'] : $redisconf['RTIMEOUT'];
        $selectdb  = $master ? $redisconf['SELECTDB'] : $redisconf['RSELECTDB'];
        $auth      = $master ? $redisconf['AUTH'] : $redisconf['RAUTH'];

        try {
            $ok = $this->redis->pconnect($host, $port, $timeout);

            if (!empty($auth)) {
                $this->redis->auth($auth);
            }
        }
        catch (Exception $e)
        {
            $log_str = date('Y-m-d H:i:s').'| 错误：'.$e->getMessage(). "\n";
            // 操作写入日志
            force_write_file(APPPATH.'logs/redis.log', $log_str, 'a+');
            die('can not connect redis '.$confkey);
        }
        
        // 当前数据库ID
        $this->db_id = $selectdb;

        //已连接标记
        $this->connect_flag = true;
        return $this->redis;
    } 

    /**
     * 获取当前数据库ID
     * 
     * @return [type] [description]
     */
    public function getDbId() 
    {
        return $this->db_id;
    }

    /**
     * 选择数据库
     * 
     * @return [type] [description]
     */
    public function selectDbNo($selectdb = null)
    {
        $dbno = $this->db_id;
        if (!is_null($selectdb)) {
            $dbno = $selectdb;
        }
        $this->redis->select($dbno);
    }

    /**
     * 简化版本写入redis队列
     *
     * @param string $key     队列key名
     *
     * @param array  $value   队列名对应数组 
     *
     * @param int    $master  连接主从 
     *
     * @param int    $rand    干扰变量 用于多个reids写入分担使用
     *
     */
    public function writeRedisList($key, array $value, $rand=0, $master=1) {
        if (!isset($GLOBALS['DEAL_CONF'][$key])) {
            exit('no key, please check DEAL_CONF');
        }
        if ($this->connect_flag === NULL) {
            $_servers = explode(':', trim($GLOBALS['DEAL_CONF'][$key]));
            $count = count($_servers);
            $index = $rand % $count;
            $this->mconnect($_servers[$index], $master);
        }
        $res = $this->setListArray($key, $value);
        return $res;
    } 

    /**
     * 返回redis对象
     *
     */
    public function getRedis() {
        return $this->redis;
    }

    /**
     * 获得reids配置
     * 
     * @param string redis配置前缀
     * 
     * @return array
     */
    private function getConfig($confkey) {
        if(!isset($GLOBALS['REDIS_CONF'][$confkey])) {
            exit('not found redis conf');
        } 
        return $GLOBALS['REDIS_CONF'][$confkey];
    }

    // 删除key 
    public function delKey($key) {
        if ($key === '') {
            return false;
        }
        return $this->redis->del($key);
    }

    /**
     * 获得key生存周期
     *
     * @param string $key 缓存key
     *
     */
    public function getKeyTTL($key) {
        return $this->redis->TTL($key);
    }
    
    /**
     * 设置key生存周期
     *
     * @param string $key     缓存key
     *
     * @parma int    $exprie  生存周期 多少秒
     */
    public function setKeyExp($key, $expire=60) { 
        return $this->redis->EXPIRE($key, $expire);
    }

    public function setKesArray($keys,$values) {

    }
    public function getKeysArray($keys) {

    }

    // 重连操作,利用ping操作断线重连
    public function reconnect()
    {
        $errno = 100;
        $errstr = '';
        try {
            $this->redis->ping();
        } catch (Exception $e) {
            $errno = $e->getCode();
            $errstr = $e->getMessage();
        }

        $res = array(
            'errno' => $errno,
            'errstr'=> $errstr,
            );
        return $res;
    }

    // 重命名
    public function rename($oldkey, $newkey)
    {
        if (empty($oldkey) || empty($newkey))
        {
            return false;
        }
        $res = $this->redis->RENAME($oldkey, $newkey);
        return $res;
    }

    #########################
    # string(字符串)   #               
    #########################
    /**
     * k => v redis set 
     *
     * @param string           $key     缓存key
     *
     * @param string|int|array $value   缓存值
     *
     * @param int              $expire  多少秒过后期,默认60秒
     *
     *
     */
    public function setValue($key, $value, $expire=60 ) {
        if ($key == '') {
            return false;
        }
        return $this->redis->SETEX($key, $expire, $value);
    }

    /**
     * k => redis get
     *
     * @param string $key 缓存key
     *
     */
    public function getValue($key) {
        if (!$this->is_cache) {
            return false;
        }
        if ($key == '') {
            return false;
        }
        $value = $this->redis->GET($key);
        return $value;
    }

    /**
     * 增加计数
     * 
     * @param  [type] $key    缓存KEY
     * @param  [type] $num    增加的值
     * @param  [type] $expire 过期时间
     * 
     * @return [type]         [description]
     */
    public function incrValue($key, $num, $expire) {
        if ('' === $key) {
            return false;
        }

        $num = intval($num);
        $res = $this->redis->incrBy($key, $num);
        $this->setKeyExp($key, $expire);
        return $res;
    }

    /**  
     * 直接缓存 数组 
     *
     * @param  string $key    缓存key
     *
     * @paranm array  $value  值(数组)
     *
     * @param  int    $expire 缓存时间 
     *
     */
    public function setArray($key, $value, $expire=60) {
        if ($key == '') {
            return false;
        }
        if (empty($value)) {
            $value = array();
        }
        $value = serialize($value);
        return $this->redis->SETEX($key, $expire, $value);
    }

    /**
     * 取缓存 (数组) 
     *
     * @param string $key 缓存key
     *
     */
    public function getArray($key) {
        if (!$this->is_cache) {
            return false;
        }
        $value = $this->redis->GET($key);
        $value = unserialize($value);
        return $value;
    }

    /**
     * 批量设置缓存
     *
     * @param array  $data   缓存数据 缓存格式为 array(key => data)
     *
     * @param string $keyfix 缓存key前缀
     * 
     * @param  int   $expire 缓存时间 
     *
     *
     */
    public function setArrayBatch(array $data, $keyfix, $expire=60) {
        if (!$data || !$keyfix) {
            return false;
        }
        $result  = array();
        $keylist = array();
        foreach ($data as $kk => $dd) {
            $key          = sprintf($keyfix, $kk);
            $this->setValue($key, serialize($dd), $expire);
        }
        return true;

        /*
        foreach ($data as $kk => $dd) {
            $key          = sprintf($keyfix, $kk);

            $this->setValue($key, serialize($dd), $expire);
            $result[$key] = serialize($dd);
            $keylist[]    = $key;
        }
        //批量缓存
        $res = $this->redis->MSET($result);
        //设置key生存周期
        foreach ($keylist as $key) {
           $this->setKeyExp($key, $expire);
        }
        return $res;*/
    }

    /**
     * 批量获得缓存中数据
     *
     * @param array  缓存的key
     *
     * @param string 缓存key前缀  
     *
     */
    public function getArrayBatch(array &$keys, $keyfix) {
        if (!$keys || !$keyfix) {
            return false;
        }
        $keylist = array();
        foreach ($keys as $key) {
            $tmp_key   = sprintf($keyfix, $key);
            $keylist[] = $tmp_key;
            $keylist2[$tmp_key] = $key;
        }
        $data    = $this->redis->MGET($keylist);
        $lostkey = array();
        $result  = array();
        foreach ($data as $kk => $val) {
            $key = $keylist[$kk];
            $key = $keylist2[$key];
            //没有该key
            if ($val === false) {
                $lostkey[] = $key;
                continue;
            }
            $val = unserialize($val);
            $result[$key] = $val; 
        }
        $keys = $lostkey;
        return $result;
    }

    #########################
    # list(列表操作)   #               
    #########################
    /**
     * 向redis队列中写入值 
     *
     * @param string $key   队列key名
     *
     * @param string $value 要写入队列的值 
     */
    public function lSetValue($key, $value) {
        if ($key == '') {
            return false;
        }
        return $this->redis->LPUSH($key, $value);
    }
    /**
     * 从redis队列中读取值
     *
     */
    public function lGetValue($key) {
        if ($key == '') {
            return false;
        }
        return $this->redis->RPOP($key);
    }

    /**
     * 向redis队列写入一个值 类型是数组 
     *
     * @param string $key   队列key名
     *
     * @param array  $value 值 
     */
    public function lSetArray($key, array $value) {
        if ($key == '') {
            return false;
        }
        $value = serialize($value);
        $res = $this->redis->LPUSH($key, $value);
        return $res;
    }
    
    /**
     * 从redis队列读出一个值 类型为数组
     *
     * @param string $key
     *
     */
    public function lGetArray($key) {
        if ($key == '') {
            return false;
        }
        $value = $this->redis->RPOP($key);
        if ($value === false) {
            return $value;
        } 
        $value = unserialize($value);
        return $value;
    } 

    /**
     * 返回redis队列中值个数
     *
     * @param string $key redis key名
     *
     */
    public function lLen($key) {
        if ($key == '') {
            return false;
        }
        return $this->redis->LLEN($key);
    }
    
    
    #########################
    # SortedSet(有序集合)   #               
    #########################
    /**
    * 有序集合中插入数据
    * 
    * @param    string  $key    reids zset 名
    * @param    mix $member 加入的元素
    * @param    int $score  元素的分数
    */
    public function zAddValue($key, $member, $score = 0) {
        $key = trim($key);
        if ($key === '') {
            return FALSE;
        }
        $res = $this->redis->zAdd($key, $score, $member);
        return $res;
    }
    
    /**
    * 有序集合中批量插入数据
    * 
    * @param    string  $key    reids zset 名
    * @param    array   $arr    插入的数据数组 array('a'=>2,2=>1.1);
    */
    public function zAddBatchValue($key, array $arr) {
        $key = trim($key);
        if (($key === '') || empty($arr)) {
            return FALSE;
        }
        
        
        foreach ($arr AS $k => $v) {
            $this->redis->zAdd($key, $v, $k);
        }
        return TRUE;
    }
    
    /**
    * 根据偏移量获取有序集合中的数据
    *
    * @param    string  $key    redis zset 名
    * @param    int $offset 偏移量
    * @param    int $limit  取多少条数据
    * @param    bool    $asc    正序：TRUE 倒序FALSE
    * @param    bool    $withscores 是否返回分数
    *
    * @param    array   排序的值
    */
    public function zGetListOffset($key, $offset, $limit, $asc = FALSE, $withscores = FALSE) {
        $key = trim($key);
        if ('' === $key) {
            return FALSE;
        }
        
        // 取回的数据条数
        $limit = abs($limit);
        $limit = max(1, $limit);
        // 开始下标
        $start = max($offset, 0);
        // 结束下标
        $end = $start + $limit - 1;
        $end = max($end, 0);
        
        if (TRUE === $asc) {
            $res = $this->redis->zRange($key, $start, $end, $withscores);
        } else {
            $res = $this->redis->zRevRange($key, $start, $end, $withscores);
        }
        return $res;
    }
    
    /**
    * 根据页码获取有序集合中的数据
    *
    * @param    string  $key    redis zset 名
    * @param    int $page   当前页码
    * @param    int $page_size  取多少条数据
    * @param    bool    $asc    正序：TRUE 倒序FALSE
    * @param    bool    $withscores 是否返回分数
    *
    * @param    array   排序的值
    */
    public function zGetListPage($key, $page, $page_size, $asc = FALSE, $withscores = FALSE) {
        $key = trim($key);
        if ('' === $key) {
            return FALSE;
        }
        
        // 取回的数据条数
        $limit = abs($page_size);
        $limit = max(1, $limit);
        // 开始下标
        $start = ($page - 1) * $limit;
        $start = max($start, 0);
        // 结束下标
        $end = $start + $limit - 1;
        $end = max($end, 0);

        if (TRUE === $asc) {
            $res = $this->redis->zRange($key, $start, $end, $withscores);
        } else {
            $res = $this->redis->zRevRange($key, $start, $end, $withscores);
        }
        return $res;
    }
    
    /**
    * 获取有序集合中的所有数据个数
    *
    * @param    string  $key    redis zset 名
    *
    * @return   int 数据个数
    */
    public function zGetCount($key) {
        $key = trim($key);
        if ('' === $key) {
            return FALSE;
        }
        
        $res = $this->redis->zSize($key);
        return $res;
    }
    
    /**
    * 获取有序集合中元素的位置
    * 
    * @param    string  $key    redis zset 名
    * @param    mix $member 元素
    * @param    bool    $asc    正序位置:true 倒序位置:false
    *
    * @return   int 元素位置
    */
    public function zGetPos($key, $member, $asc = FALSE) {
        $key = trim($key);
        $member = trim($member);
        if (('' === $key) || ('' === $member)) {
            return FALSE;
        }
        
        if (TRUE === $asc) {
            $res = $this->redis->zRank($key, $member);
        } else {
            $res = $this->redis->zRevRank($key, $member);
        }
        
        $res += 1;
        return $res;
    }
    
    /**
    * 返回名称为key的zset中元素member的score
    * 
    * @param    string  $key    redis zset 名
    * @param    mix $member 元素
    *
    * @return   int 分数
    */  
    public function zMemberScore($key, $member) {
        $key = trim($key);
        $member = trim($member);
        if (('' === $key) || ('' === $member)) {
            return FALSE;
        }
        
        $res = $this->redis->zScore($key, $member);
        return $res;
    }
    
    /**
    * 删除名称为key的zset中的元素member
    * 
    * @param    string  $key    redis zset 名
    * @param    mix $member 元素
    *
    * @return   int 元素位置
    */
    public function zRemMember($key, $member) {
        $key = trim($key);
        $member = trim($member);
        if (('' === $key) || ('' === $member)) {
            return FALSE;
        }
        
        $res = $this->redis->zRem($key, $member);
        return $res;
    }
    
    /**
    * 返回名称为key的zset中score >= star且score <= end的所有元素
    * 
    * @param    string  $key    redis zset 名
    * @param    int $start  最小分数
    * @param    int $end    最大分数
    * @param    bool    $asc    true:正序排列 false:倒序排列
    * @param    bool    $withscores true:返回分数 false:不返回
    *
    * @return   int 元素位置
    */  
    public function zRangeByScore($key, $start, $end, $asc = FALSE, $withscores = FALSE) {
        $key = trim($key);
        if ('' === $key) {
            return FALSE;
        }
        
        $param = array(TRUE);
        if (TRUE === $asc) {
            
            $res = $this->redis->zRangeByScore($key, $start, $end, $param);
        } else {
            $res = $this->redis->zRevRangeByScore($key, $end, $start, $param);
        }
        return $res;
    }
    
    /**
    * 返回名称为key的zset中score >= star且score <= end的所有元素的个数
    * 
    * @param    string  $key    redis zset 名
    * @param    int $start  最小分数
    * @param    int $end    最大分数
    *
    * @return   int 元素位置
    */  
    public function zRangeCount($key, $start, $end) {
        $key = trim($key);
        if ('' === $key) {
            return FALSE;
        }
        
        $res = $this->redis->zCount($key, $start, $end);
        return $res;
    }

    #########################
    # hash(hash表)   #               
    #########################
    /**
     * 将哈希表key中的域field的值设为value。 
     *
     * @param string           $key     缓存key
     * @param string           $field   键
     * @param string|int|array $value   缓存值
     */
    public function hSet($key, $field, $value) {
        if ($key == '' || $field == '') {
            return false;
        }
        return $this->redis->HSET($key, $field, $value);
    }

    /**
     * 取得整个HASH表的信息，返回一个以KEY为索引VALUE为内容的数组。 
     *
     * @param string           $key     缓存key
     */
    public function hGetAll($key) {
        if ($key == '') {
            return false;
        }
        return $this->redis->hGetAll($key);
    }

    /**
     * 取得哈希表key中的域field的值。 
     *
     * @param string           $key     缓存key
     * @param string           $field   键
     */
    public function hGet($key, $field) {
        if ($key == '' || $field == '') {
            return false;
        }
        return $this->redis->HGET($key, $field);
    }

    /**
     * 删除哈希表指定的元素。 
     *
     * @param string           $key     缓存key
     * @param string           $field   键
     */
    public function hDel($key, $field) {
        if ($key == '' || $field == '') {
            return false;
        }
        return $this->redis->HDEL($key, $field);
    }

    /**
     * 验证哈希表中是否存在指定的KEY-VALUE。 
     *
     * @param string           $key     缓存key
     * @param string           $field   键
     */
    public function hExists($key, $field) {
        if ($key == '' || $field == '') {
            return false;
        }
        return $this->redis->HEXISTS($key, $field);
    }

    #########################
    # set(set集合)   #               
    #########################
    
    /**
     * 添加一个VALUE到SET容器中，如果这个VALUE已经存在于SET中，那么返回FLASE。
     *
     * @param string           $key     缓存key
     * @param string           $value   值
     */
    public function sAdd($key, $value) {
        if ($key == '' || $value == '') {
            return false;
        }
        return $this->redis->SADD($key, $value);
    }

    /**
     * 检查VALUE是否是SET容器中的成员。
     *
     * @param string           $key     缓存key
     * @param string           $value   值
     */
    public function sIsMember($key, $value) {
        if ($key == '' || $value == '') {
            return false;
        }
        return $this->redis->SISMEMBER($key, $value);
    }

    /**
     * 返回集合key中的所有成员。
     *
     * @param string           $key     缓存key
     */
    public function sMembers($key) {
        if ($key == '') {
            return false;
        }
        return $this->redis->SMEMBERS($key);
    }

    /**
     * 返回SET容器的成员数
     *
     * @param string           $key     缓存key
     */
    public function sCard($key) {
        if ($key == '') {
            return false;
        }
        return $this->redis->SCARD($key);
    }

    /**
     * 随机返回一个元素，并且在SET容器中移除该元素。
     *
     * @param string           $key     缓存key
     */
    public function sPop($key) {
        if ($key == '') {
            return false;
        }
        return $this->redis->SPOP($key);
    }

    /**
     * 移除指定的VALUE从SET容器中
     *
     * @param string           $key     缓存key
     * @param string           $value   值
     */
    public function sRem($key, $value) {
        if ($key == '' || $value == '') {
            return false;
        }
        return $this->redis->SREM($key, $value);
    }

    /**
     * 返回集合中的一个随机元素
     *
     * @param string  $key 缓存key
     */
    public function sRandMember($key) {
        if ($key == '') {
            return false;
        }
        return $this->redis->SRANDMEMBER($key);
    }


}