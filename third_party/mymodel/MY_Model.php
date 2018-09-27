<?php defined('BASEPATH') OR exit('No direct script access allowed');
/**
 * model层扩展
 *
 * 实现基本的curd操作，
 * 复杂的需要在各单表的model中自行完成例如join操作
 *
 * 子类方法中使用 parent::mysqlInstances($this->active_group) 获得连接
 * 
 * redis连接和mysql连接使用单例模式，减少mysql连接建立的通信请求
 *
 * http://blog.csdn.net/qmhball/article/details/46988111
 */
include_once 'xredis.php';
class MY_Model extends CI_Model 
{
    protected $pk = '';
    protected $table_name = '';
    protected $active_group = 'default';
    protected $redis_detail_key = '';    
    protected $redis_group = '';

    /**
     * 实例数组
     *
     * @var array
     */
    protected static $mysql_instance = array();

    /**
     * reids实例数组
     *
     * @var array
     */
    protected static $redis_instance = array();

    public function __construct() 
    {
        parent::__construct();
    }

    public static function mysqlInstances($active_group)
    {
        if (!isset(self::$mysql_instance[$active_group]) || empty(self::$mysql_instance[$active_group]))
        {
            self::$mysql_instance[$active_group] = get_instance()->load->database($active_group, TRUE); 
        }
        return self::$mysql_instance[$active_group];
    }

    public static function redisInstances($redis_group, $reselect_db_no = true)
    {
        if (!isset(self::$redis_instance[$redis_group]) || empty(self::$redis_instance[$redis_group]))
        {
            $redis = new Xredis();
            $redis->mpconnect($redis_group, 1);
            self::$redis_instance[$redis_group] = $redis;
        }
        
        if ($reselect_db_no === true) {
            // 重新选择数据库
            self::$redis_instance[$redis_group]->selectDbNo();    
        }

        return self::$redis_instance[$redis_group];
    }

    /**
     * redis断线重连
     * 
     * @param  [type] $redis_group reids组
     * @return [type]              [description]
     */
    private function redisReconnect($redis_group)
    {
        // 检查数据库是否正常，检查前不选择数据库，在reconenect里重新选择
        $redis = self::redisInstances($redis_group, false);
        // 连接正常
        $res = $redis->reconnect();
        if ($res['errno'] == 100) {
            $redis->selectDbNo();
            return $redis;
            // return true;
        }

        // 关闭删除掉原来的连接资源
        unset(self::$redis_instance[$redis_group]);
        // 只重连一次
        self::redisInstances($redis_group);

        $log_str = date('Y-m-d H:i:s').'| 错误：'.$res['errstr']. ' | redis断线重连 '.$redis_group."\n";
        // 操作写入日志
       // force_write_file(APPPATH.'logs/redis.log', $log_str, 'a+');
        force_write_file('/tmp/redis.log', $log_str, 'a+');
    }

    /**
    * 插入单条数据
    *
    * @param    array   $data       要插入的数据
    * @return   int                 最后插入的ID
    */
    public function addData($data) 
    {
        if (empty($data)) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        
        // 参数过滤
        foreach( $data as $key => $val )
        {
            $key = $db->escape_str($key);
            $val = $db->escape_str($val);
            $data[$key] = $val;
        }
        $key_str = "`" . implode("`,`", array_keys($data)) . "`";
        $val_str = "'" . implode("','", $data) . "'";
        $sql = "INSERT INTO `{$this->table_name}` ({$key_str}) VALUES ({$val_str})";
        $query = $db->query($sql);
        $id = $db->insert_id();

        if (($id>0) && ($this->redis_group) && ($this->redis_detail_key))
        {
            // 更新缓存
            $this->getDataListByIds(array($id), false);   
        }
        return $id;
    }

    /**
    * 忽略插入单条数据
    *
    * @param    array   $data       要插入的数据
    * @return   int                 最后插入的ID
    */
    public function addDataIgnore($data) 
    {
        if (empty($data)) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        
        // 参数过滤
        foreach( $data as $key => $val )
        {
            $key = $db->escape_str($key);
            $val = $db->escape_str($val);
            $data[$key] = $val;
        }
        $key_str = "`" . implode("`,`", array_keys($data)) . "`";
        $val_str = "'" . implode("','", $data) . "'";
        $sql = "INSERT IGNORE INTO `{$this->table_name}` ({$key_str}) VALUES ({$val_str})";
        $query = $db->query($sql);
        return true;
    }

    /**
    * 批量插入数据
    * @param    array   $data       要插入的数据
    * @return   int                 最后插入的ID
    */
    public function addDataBatch($data) 
    {
        if (empty($data)) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        

        //过滤字符防sql注入
        $values = array();
        $keys   = '';

        foreach($data as $val) {
            $data2 = array();
            if (!$keys) {
                $field_arr = array();
                foreach (array_keys($val) as $field_val)
                {

                    $field_arr[] = $db->escape_str($field_val);
                }
                $keys   = "`".implode("`,`", $field_arr)."`";
            }
            foreach($val as $kk => $vv) {
                $data2[] = $db->escape_str($vv);
            }            
            $values[] = "('".implode("','", $data2)."')";
        }
        
        $value2 = implode(',', $values);
        $sql = "INSERT IGNORE INTO `{$this->table_name}` ({$keys}) values {$value2}";
        $query  = $db->query($sql);

        // 影响行数
        $db->affected_rows();
        return TRUE;
    }

    /**
    * 批量更新数据
    * @param    array   $data       要插入的数据
    * @return   int                 最后插入的ID
    */
    public function repDataBatch($data) 
    {
        if (empty($data)) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        

        //过滤字符防sql注入
        $values = array();
        $keys   = '';

        foreach($data as $val) {
            $data2 = array();
            if (!$keys) {
                $field_arr = array();
                foreach (array_keys($val) as $field_val)
                {

                    $field_arr[] = $db->escape_str($field_val);
                }
                $keys   = "`".implode("`,`", $field_arr)."`";
            }
            foreach($val as $kk => $vv) {
                $data2[] = $db->escape_str($vv);
            }            
            $values[] = "('".implode("','", $data2)."')";
        }
        
        $value2 = implode(',', $values);
        $sql = "REPLACE INTO `{$this->table_name}` ({$keys}) values {$value2}";
        $query  = $db->query($sql);

        // 影响行数
        $db->affected_rows();
        return TRUE;
    }
    
    /**
    * replace单条数据
    * @param    array   $data       要插入的数据
    * @return   int                 最后插入的ID
    */
    public function replaceData($data) 
    {
        if (empty($data)) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        

        // 参数过滤
        foreach ($data AS $key=>$val) {
            $key = $db->escape_str($key);
            $val = $db->escape_str($val);

            $data[$key] = $val;
        }
        $key_str = "`" . implode("`,`", array_keys($data)) . "`";
        $val_str = "'" . implode("','", $data) . "'";
        $sql = "REPLACE INTO `{$this->table_name}` ({$key_str}) VALUES ({$val_str})";
        $query = $db->query($sql);

        // 影响行数
        $db->affected_rows();

        if (isset($data[$this->pk]) && ($data[$this->pk]>0) && ($this->redis_group) && ($this->redis_detail_key))
        {
            // 更新缓存
            $this->getDataListByIds(array($data[$this->pk]), false);   
        }

        return true;
    }

    /**
    * 根据ID更新数据
    *
    * @param    array   $data       要更新的数据
    * @param    int     $id         要更新的ID
    *
    * @return   bool
    */
    public function updDataById($data, $id) 
    {
        $id = intval($id);
        if (empty($data) || ($id < 1)) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        

        // 参数过滤
        $set_arr = array();
        foreach( $data as $key => $val )
        {
            $key = $db->escape_str($key);
            $val = $db->escape_str($val);
            $set_arr[] = "`{$key}`='{$val}'";
        }
        $set_str = implode(',', $set_arr);
        $sql = "UPDATE `{$this->table_name}` SET {$set_str} WHERE `{$this->pk}`='{$id}'";
        $query = $db->query($sql);

        // 影响行数
        $db->affected_rows();

        if (($this->redis_group) && ($this->redis_detail_key))
        {
            // 更新缓存
            $this->getDataListByIds(array($id), false);   
        }

        return true;
    }

    /**
     * 根据条件更新数据
     *
     * @param    array   $data       要更新的数据
     * @param    int     $where         要更新的条件 
     *
     * @return   bool
     */
    public function updDataByWhere($data, $where) 
    {
        $db = self::mysqlInstances($this->active_group);

        // 参数过滤
        $set_arr = array();
        foreach ($data as $key => $val) {
            $key = $db->escape_str($key);
            $val = $db->escape_str($val);
            $set_arr[] = "`{$key}`='{$val}'";
        }
        $set_str = implode(',', $set_arr);
        $where_sql = $this->whereSql($where);

        $sql = "UPDATE `{$this->table_name}` SET {$set_str}";
        $sql.= $where_sql;
        $query = $db->query($sql);

        // 影响行数
        return $db->affected_rows();
    }
    
    /**
    * 根据ID批量更新数据
    *
    * @param    array   $data       要更新的数据
    * @param    string  $upd_key    要更新的KEY
    *
    * @return   bool
    */
    public function updDataBatchByIds($data, $upd_key = '') 
    {
        $data = filter_keyforval($data);
        if (empty($data)) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        

        $upd_key = $db->escape_str($upd_key);
        $id_arr = array_keys($data);
        $ids_str = "'" . implode("','", $id_arr ) . "'";
        $sql = "UPDATE `{$this->table_name}` SET `{$upd_key}`=CASE `{$this->pk}`";
        foreach($data as $key => $val )
        {
            $sql .= sprintf(" WHEN %d THEN %d ", $key, $val);

        }
        $sql .= " END WHERE `{$this->pk}` IN ({$ids_str})";
        $query = $db->query($sql);

        // 影响行数
        $db->affected_rows();

        if (($this->redis_group) && ($this->redis_detail_key))
        {
            $this->getDataListByIds($id_arr, false);   
        }
        return true;
    }

    /**
    * 更改+1数据
    *
    * @param    array   $chg_where     要更新的数据
    * @param    array   $where         要更新的ID
    *
    * @return   bool
    */
    public function chgDataNum($where, $chg_where) 
    {
        if (empty($chg_where)) {
            return false;
        }
        
        $db = self::mysqlInstances($this->active_group);
        

        $set_arr = array();

        foreach ($chg_where as $key=>$val)
        {
            $key = $db->escape_str($key);
            $num = $db->escape_str($val);
            $set_arr[] = "`{$key}`=`{$key}`+{$num}";
        }


        if (count($set_arr) < 1) {
            return false;
        }
        
        $where_sql = $this->whereSql($where);
        if (empty($where_sql))
        {
            return false;
        }

        $set_sql = implode(",", $set_arr);
        $sql = "UPDATE `{$this->table_name}` SET {$set_sql}";
        $sql.= $where_sql;
                
        $query = $db->query($sql);

        // 影响行数
        $db->affected_rows();

        if (($this->redis_group) && ($this->redis_detail_key))
        {
            // 获得自增ID
            $id_arr = $this->getDataIdsBy($where,1, 500);
            // 更新缓存
            $this->getDataListByIds($id_arr, false);   
        }
        return true;
    }

    /**
    * 更改+1数据
    *
    * @param    array   $chg_where      要更新的数据
    * @param    int     $id             主键ID
    *
    * @return   bool
    */
    public function chgDataNumById($id, $chg_where) 
    {
        $id = intval($id);
        if (empty($chg_where) || ($id<1)) {
            return false;
        }
        
        $db = self::mysqlInstances($this->active_group);
        

        $set_arr = array();

        foreach ($chg_where as $key=>$val)
        {
            $key =  $db->escape_str($key);
            $num = intval($val);
            $set_arr[] = "`{$key}`=`{$key}`+{$num}";
        }


        if (count($set_arr) < 1) {
            return false;
        }
        
        $set_sql = implode(",", $set_arr);
        $sql = "UPDATE `{$this->table_name}` SET {$set_sql} WHERE `{$this->pk}`='{$id}'";
        $query = $db->query($sql);

        // 影响行数
        $db->affected_rows();

        if (($this->redis_group) && ($this->redis_detail_key))
        {
            // 更新缓存
            $this->getDataListByIds(array($id), false);   
        }
        return true;
    }

    /**
    * 根据条件获得总数
    *
    * @param    array       $where      条件数组
    * @return   array
    */
    public function getDataSumBy($where)
    {
        if (!isset($where['sum_field'])) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        
        $sum_field = $db->escape_str($where['sum_field']);

        $where_sql = $this->whereSql($where);
        $sql = "SELECT SUM(`{$sum_field}`) AS `new_sum` FROM `{$this->table_name}`";
        $sql .= $where_sql.' LIMIT 1';
        $query = $db->query($sql);
        $row = $query->row_array();
        return $row['new_sum'];
    }

    /**
     * 根据条件获得条数
     *
     * @param    array    $where         条件数组
     * @param    array   $select_field   读取字段数组
     * @param    int      $limit         读取记录数
     * @return   array
     */
    public function getDataGroupbyTotalBy($where, $select_field = array(), $limit = 1000)
    {
        $db = self::mysqlInstances($this->active_group);

        $where_sql   = $this->whereSql($where);
        $groupby_sql   = $this->groupbySql($where);

        $where['orderby'] = ['new_total' => 'desc'];
        $orderby_sql = $this->sortSql($where);
        if( !empty($select_field) && is_string( $select_field)) {
            $select_field = (array) $select_field;
        }

        if( !empty( $select_field) && is_array( $select_field)) {
            $select_field_str = '`'.implode('`,`', $select_field).'`';
            $select_field = $select_field_str . ',';
        }

        $sql = "SELECT {$select_field} COUNT(`{$this->pk}`) AS `new_total` FROM `{$this->table_name}`";
        $sql .= $where_sql.$groupby_sql.' LIMIT ' . $limit;
        $query = $db->query($sql);
        $res = $query->result_array();
        return $res;
    }

    /**
     * 按组根据条件获得总数
     *
     * @param    array    $where         条件数组
     * @param    array    $select_field  读取字段数组
     * @param    int      $limit         读取记录数
     * @return   array
     */
    public function getDataGroupbySumBy($where, $select_field_arr = array(), $limit = 1000)
    {
        if (!isset($where['sum_field']))
        {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);

        $sum_field_sql = '';
        if (is_string($where['sum_field'])) {
            $sum_field = $db->escape_str($where['sum_field']);
            $sum_field_sql = "SUM(`{$sum_field}`) AS `new_sum`";
        } else if (is_array($where['sum_field'])) {
            // 数组
            $tmp = [];
            foreach ($where['sum_field'] as $key => $val) {
                $key = $db->escape_str($key);
                $val = $db->escape_str($val);

                $tmp[] = "SUM(`{$key}`) AS `{$val}`";
            }
            $sum_field_sql = implode(',', $tmp);
        }

        $select_field = '';
        if(!empty($select_field_arr) && is_string($select_field_arr)) {
            $select_field_arr = (array) $select_field_arr;
        }
        if(!empty($select_field_arr) && is_array($select_field_arr)) {
            $select_field_str = implode($select_field_arr, ',');
            $select_field = $select_field_str . ',';
        }

        $where_sql   = $this->whereSql($where);
        $groupby_sql = $this->groupbySql($where);
        $sql = "SELECT {$select_field} {$sum_field_sql} FROM `{$this->table_name}`";
        $sql .= $where_sql.$groupby_sql.' LIMIT ' . $limit;

        $query = $db->query($sql);
        $res = $query->result_array();
        return $res;
    }

    /**
    * 更改+1数据
    *
    * @param    array   $chg_where      要更新的数据
    * @param    int     $id             主键ID
    *
    * @return   bool
    */
    public function chgDataById($id, $chg_where) 
    {
        $id = intval($id);
        if (empty($chg_where) || ($id<1)) {
            return false;
        }
        
        $db = self::mysqlInstances($this->active_group);
        

        $set_arr = [];

        foreach ($chg_where as $key => $val) {
            switch ($key) {
                case 'eq': // 等于
                    foreach ($val as $k=>$v) {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $set_arr[] = "`{$k}`='{$v}'";
                    }
                    break;
                case 'increase': // 增加
                    foreach ($val as $k=>$v) {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $set_arr[] = "`{$k}`=`{$k}`+'{$v}'";
                    }
                    break;
                case 'reduce': // 减少
                    foreach ($val as $k=>$v) {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $set_arr[] = "`{$k}`=`{$k}`-'{$v}'";
                    }
                    break;
                default:
                    break;
            }
        }

        if (count($set_arr) < 1) {
            return false;
        }
        
        $set_sql = implode(",", $set_arr);
        $sql = "UPDATE `{$this->table_name}` SET {$set_sql} WHERE `{$this->pk}`='{$id}'";

        $query = $db->query($sql);

        // 影响行数
        $db->affected_rows();

        if (($this->redis_group) && ($this->redis_detail_key)) {
            // 更新缓存
            $this->getDataListByIds(array($id), false);   
        }
        return true;
    }

    /**
     * 根据ID 删除数据
     * 
     * @param  int     $id      要删除的ID
     * @return bool             删除是否成功
     */
    public function delDataById($id) 
    {
        $id = intval($id);
        if ($id < 1) {
            return false;
        }

        $db = self::mysqlInstances($this->active_group);
        

        $sql = "DELETE FROM `{$this->table_name}` WHERE `{$this->pk}`='{$id}'";
        $query = $db->query($sql);

        // 影响行数
        $db->affected_rows();

        if (($id>0) && ($this->redis_group) && ($this->redis_detail_key))
        {
            // 更新缓存
            $this->getDataListByIds(array($id), false);   
        }
        return true;
    }


    /**
     * 根据条件 删除数据
     * 
     * @param  int     $where      要删除的条件
     * @return bool             删除是否成功
     */
    public function delDataByWhere($where, $limit = 1000) 
    {
        $db = self::mysqlInstances($this->active_group);
        
        $sql = "DELETE FROM `{$this->table_name}`";
        $where_sql   = $this->whereSql($where);
        // 必须有条件
        if (empty($where_sql)) {
            return false;
        }
        
        $limit = intval($limit);
        $order_sql = $this->sortSql($where);
        $limit_sql = " LIMIT {$limit}";

        $sql .= $where_sql.$order_sql.$limit_sql;

        $query = $db->query($sql);

        return true;
    }
    

    /**
    * 根据条件获得条数
    *
    * @param    array       $where      条件数组
    * @return   array   
    */
    public function getDataTotalBy($where) 
    {
        $db = self::mysqlInstances($this->active_group);
        
        $where_sql = $this->whereSql($where);
        $group_sql = $this->groupbySql($where);
        
        $sql = "SELECT COUNT(*) AS `new_total` FROM `{$this->table_name}`";
        $sql .= $where_sql . $group_sql;        
        $query = $db->query($sql);
        $res = 0;
        $row = $query->row_array();
        if (! empty($row))
        {
            if ($group_sql != '') {
                $res = $query->num_rows();
            } else {
                $res = intval($row['new_total']);
            }
        }
        return $res;
    }

    /**
     * 根据条件批量取得信息
     * @param array $where 查询数组
     * @param int $page 当前页数
     * @param int $page_size 每页条数
     *          
     * @return array
     */
    public function getDataListByF($where, $page = 1, $page_size = 10)
    {       
        $db = self::mysqlInstances($this->active_group);

        $field = '*';
        if (isset($where['field']) && !empty($where['field']))
        {
            $field_arr = $where['field'];
            if (!in_array($this->pk, $field_arr))
            {
                $field_arr[] = $this->pk;
            }

            // 过滤字符串
            foreach ($field_arr as $key=>$val)
            {
                $field_arr[$key] = $db->escape_str($val);  
            }
            $field = '`'.implode('`,`', $field_arr).'`';
        }

        $where_sql = $this->whereSql($where);
        $order_sql = $this->sortSql($where);
        $group_sql = $this->groupbySql($where);
        $limit_sql = $this->limitSql($page, $page_size);
        
        $sql = "SELECT {$field} FROM `{$this->table_name}`";
        $sql .= $where_sql . $group_sql . $order_sql . $limit_sql;
 
        $query  = $db->query($sql);
        $result = array();
        foreach($query->result_array() as $key => $val)
        {
            $result[$val[$this->pk]] = $val;
        }
        return $result;
    }

    /**
     * 根据条件批量取得信息
     * @param array $where 查询数组
     * @param int $pk_min_id 最小主键值    
     * @param int $page_size 每页条数
     *          
     * @return array
     */
    public function getDataListByPkPageF($where, $pk_min_id = 0, $page_size = 10)
    {       
        $db = self::mysqlInstances($this->active_group);
        
        $field = '*';
        if (isset($where['field']) && !empty($where['field']))
        {
            $field_arr = $where['field'];
            if (!in_array($this->pk, $field_arr))
            {
                $field_arr[] = $this->pk;
            }
            // 过滤字符串
            foreach ($field_arr as $key=>$val)
            {
                $field_arr[$key] = $db->escape_str($val);  
            }
            $field = '`'.implode('`,`', $field_arr).'`';
        }

        $where['gt']["{$this->pk}"] = $pk_min_id;
        $where['orderby'] = array(
            "{$this->pk}"   => 'asc',
            );
        $where_sql = $this->whereSql($where);
        $order_sql = $this->sortSql($where);
        $limit_sql = $this->limitSql(1, $page_size);
        $sql = "SELECT {$field} FROM `{$this->table_name}`";
        $sql .= $where_sql . $order_sql . $limit_sql;
        $query  = $db->query($sql);
        $result = array();
        foreach($query->result_array() as $key => $val)
        {
            $result[$val[$this->pk]] = $val;
        }
        return $result;
    }

    /**
     * 根据IDS批量取得信息
     *
     * @param array $id_arr 查询IDs
     * @param array $field_arr 查询出的字段
     * 
     * @return array
     */
    public function getDataListByIdsF(array $id_arr, $field_arr = array())
    {       
        $id_arr = filter_ids($id_arr);
        if (empty($id_arr)) {
            return array();
        }

        $db = self::mysqlInstances($this->active_group);
        
        $field = '*';
        if (!empty($field_arr))
        {
            if (!in_array($this->pk, $field_arr))
            {
                $field_arr[] = $this->pk;
            }
            // 过滤字符串
            foreach ($field_arr as $key=>$val)
            {
                $field_arr[$key] = $db->escape_str($val);  
            }
            $field = '`'.implode('`,`', $field_arr).'`';
        }

        $where = array(
            'in'    => array(
                "{$this->pk}"   => $id_arr,
                ),
            );
        $where_sql = $this->whereSql($where);
        $sql = "SELECT {$field} FROM `{$this->table_name}`";
        $sql.= $where_sql;

        $query = $db->query($sql);
        $res1 = array();
        foreach ($query->result_array() as $key=>$val)
        {
            $res1[$val[$this->pk]] = $val;
        }
        
        // 按ID查询顺序排序
        $res2 = array();
        foreach($id_arr as $id)
        {
            if (isset($res1[$id]))
            {
                $res2[$id] = $res1[$id];
            }
        }
        return $res2;
    }

    /**
     * 根据ID 获得数据信息
     * @param int $id 主键ID
     * @return array
     */
    public function getDataByIdF($id)
    {       
        $res = array();
        // 数据信息
        $arr = $this->getDataListByIdsF(array($id));
        if (isset($arr[$id]))
        {
            $res = $arr[$id];
        }
        return $res;
    }

    /**
     * 根据条件批量取得id
     *
     * @param array $where 需要查询的条件数组
     * @param int $page 当前页数
     * @param int $page_size 每页条数
     * @param string $id_fields 要取得的字段，默认是主键         
     * @return array
     */
    public function getDataIdsBy($where, $page = 1, $page_size = 10, $id_fields='')
    {
        $db = self::mysqlInstances($this->active_group);
        

        $where_sql = $this->whereSql($where);
        $order_sql = $this->sortSql($where);
        $group_sql = $this->groupbySql($where);
        $limit_sql = $this->limitSql($page, $page_size);

        $fields = $this->pk;
        
        if ($id_fields != '') {
            $fields = $id_fields;    
        }
        $sql = "SELECT `{$fields}` FROM `{$this->table_name}`";
        $sql .= $where_sql . $group_sql . $order_sql . $limit_sql;
        $query = $db->query($sql);
        $id_arr = array();
        foreach($query->result_array() as $key => $val)
        {
            $id_arr[] = $val[$fields];
        }
        return $id_arr;
    }

    /**
     * 根据id批量取得信息
     *
     * @param array $id_arr id数组
     * @param bool $is_cache  是否使用缓存
     * @return array
     */
    public function getDataListByIds(array $id_arr, $is_cache = true)
    {
        $id_arr = filter_ids($id_arr);
        if (empty($id_arr)) {
            return array();
        }
        $ids = $id_arr;
        
        // 缓存对象ID
        $use_cache = null;
        // 缓存KEY
        $cache_key_conf = array();
        if ($this->redis_group && $this->redis_detail_key)
        {
            // redis断线重连
            $use_cache = $this->redisReconnect($this->redis_group);
            // 存在缓存结构才使用缓存
            $cache_key_conf = get_redis_key($this->redis_detail_key);
        }

        $result1 = array();
        // 缓存中取数据
        if (!is_null($use_cache) && ($is_cache === TRUE))
        {
            $result1 = $use_cache->getArrayBatch($ids, $cache_key_conf['key']);
            if (($result1 !== false) && (empty($ids)))
            {
                return $result1;
            } 
        }

        // 数据库中根据ID获得数据
        $result2 = $this->getDataListByIdsF($ids);
        
        if (!is_null($use_cache)) 
        {
            // 存在缓存结构才使用缓存
            // 缓存中设置数据
            $use_cache->setArrayBatch($result2, $cache_key_conf['key'], $cache_key_conf['time']);    
        }
        
        $result = (array)$result1 + (array)$result2;
        
        // 按参数中id顺序返回
        $res = array();
        foreach($id_arr as $id)
        {
            if (isset($result[$id]))
            {
                $res[$id] = $result[$id];
            }
        }
        return $res;
    }

    /**
     * 根据id取得信息
     *
     * @param int $id 主键ID
     * @param bool $is_cache 是否使用缓存
     *          
     * @return array
     */
    public function getDataById($id, $is_cache = TRUE)
    {
        $id = intval($id);
        if ($id < 1)
        {
            return array();
        }
        $result = array();
        $arr = $this->getDataListByIds(array($id), $is_cache);
        if (isset($arr[$id]))
        {
            $result = $arr[$id];
        }
        return $result;
    }

    /**
     * 通过条件取得信息一条数据
     * @param array $where 条件
     * 
     * @return array
     */
    public function getOneDataByF($where)
    {
        $arr = $this->getDataListByF($where, 1, 1);
        if (empty($arr))
        {
            return array();
        }

        $data = current($arr);
        return $data;
    }

    /**
     * 通过条件取得信息,通过缓存取
     * @param array $where 条件
     * @param bool $is_cache 是否启用缓存
     * @return array
     */
    public function getOneDataBy($where, $is_cache = TRUE)
    {
        $ids = $this->getDataIdsBy($where, 1,1);
        if( !isset($ids[0]) || empty($ids[0])) {
            return array();
        }

        $res = $this->getDataById($ids[0], $is_cache);
        return $res;
    }


    /**
     * 根据条件获得去重数据
     * @param array $where 需要查询的条件数组
     * @param int $page  页码
     * @param int $page_size 每页记录数
     * @return array
     */
    public function getDistinctDataBy($where, $page=1, $page_size=0)
    {
        if (empty($where))
        {
            return false;
        }
        $db = self::mysqlInstances($this->active_group);
        

        // 去重字段
        $distinct_field = $db->escape_str($where['distinct_field']);
        unset($where['distinct_field']);

        $where_sql = $this->whereSql($where);
        $order_sql = $this->sortSql($where);
        $limit_sql = $this->limitSql($page, $page_size);

        $sql = "SELECT DISTINCT {$distinct_field} FROM `{$this->table_name}`";
        $sql .= $where_sql . $order_sql . $limit_sql;
        $query = $db->query($sql);
        $result = array();
        foreach($query->result_array() as $key => $val)
        {
            $result[] = $val;
        }
        return $result;
    }

    public function __destruct()
    {  
        if (isset(self::$mysql_instance[$this->active_group])) {
            self::$mysql_instance[$this->active_group]->close();
        }

        if (isset(self::$redis_instance[$this->redis_group])) {
            self::$redis_instance[$this->redis_group]->close();
        }
    }

    /**
     * 组装SQL and
     *
     * @param array $where 要组装的数据
     * @return string 组装完成的SQL
     *
     *
     * $where = array(
     *     'eq' => array(
     *         'status' => 1,
     *     ),
     *     'neq' => array(
     *         'status' => 1,
     *     ),
     *     'bothlike' => array(
     *         'title' => '你好',
     *     ),
     *     'in' => array(
     *         'id' => array(1,2,3),
     *     ),
     *     'lte' => array(
     *         'start_time' => '2017-10-10 10:10:10',
     *     ),
     *     'gte' => array(
     *         'start_time' => '2017-10-10 10:10:10',
     *     ),
     *     'gt' => array(
     *         'start_time' => '2017-10-10 10:10:10',
     *     ),
     *     'lt' => array(
     *         'start_time' => '2017-10-10 10:10:10',
     *     ),
     *     'beforelike' => array(
     *         'start_time' => '2017-10-10 10:10:10',
     *     ),
     *     'afterlike' => array(
     *         'start_time' => '2017-10-10 10:10:10',
     *     ),
     * );
     */
    protected function whereSql($where)
    {
        if (empty($where))
        {
            return '';
        }

        // 组装成的SQL
        $sql_arr = array();

        $db = self::mysqlInstances($this->active_group);

        foreach ($where as $key=>$val)
        {
            switch ($key) 
            {
                case 'eq':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}`='{$v}'";
                    }
                    break;
                case 'neq':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}`!='{$v}'";
                    }
                    break;
                case 'bothlike':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}` LIKE '%{$v}%'";
                    }
                    break;
                case 'beforelike':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}` LIKE '%{$v}'";
                    }
                    break;
                case 'afterlike':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}` LIKE '{$v}%'";
                    }
                    break;
                case 'in':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);

                        $tmp_in = array();
                        if (!is_array($v))
                        {
                            $tmp_in = array($v);
                        }
                        else
                        {
                            foreach ($v as $vv)
                            {
                                $vv = $db->escape_str($vv);
                                $tmp_in[] = $vv;
                            }
                        }
                        if (!empty($tmp_in))
                        {
                            $sql_arr[] = "`{$k}` IN ('".implode("','", $tmp_in)."')";
                        }
                    }
                    break;
                case 'lte':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}`<='{$v}'";
                    }
                    break;
                case 'gte':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}`>='{$v}'";
                    }
                    break;
                case 'lt':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}`<'{$v}'";
                    }
                    break;
                case 'gt':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}`>'{$v}'";
                    }
                    break;
                case 'xand':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}` = {$k} & {$v}";
                    }
                    break;
                case 'xor':
                    foreach ($val as $k=>$v)
                    {
                        $k = $db->escape_str($k);
                        $v = $db->escape_str($v);
                        $sql_arr[] = "`{$k}` = {$k} | {$v}";
                    }
                    break;
                default:
                    break;
            }
        }

        // 组装的sql
        $sql = '';
        if (count($sql_arr) > 0)
        {
            $sql = ' WHERE ' . implode(' AND ', $sql_arr);
        }
        return $sql;
    }

    /**
     * 生成order by 语句
     *
     * @param array $where 传进来的参数条件
     * @return string sql语句
     */
    protected function sortSql($where)
    {
        if (!isset($where['orderby']) || empty($where['orderby']))
        {
            return '';
        }
        
        $sql_arr = array();
        $db = self::mysqlInstances($this->active_group);

        foreach($where['orderby'] as $key => $val)
        {
            // 参数过滤
            $key = trim($db->escape_str($key));
            $val = trim($db->escape_str($val));
            $sql_arr[] = "`{$key}` {$val}";
        }
        
        $sql = '';
        if (count($sql_arr) > 0)
        {
            $sql = ' ORDER BY ' . implode(',', $sql_arr);
        }
        return $sql;
    }
        
    /**
     * 组装LIMIT sql
     * @param int $page 当前页
     * @param int $page_size 每页条数
     * @return string limit语句
     */
    protected function limitSql($page = 0, $page_size = 0)
    {
        $sql = '';
        // 分页
        $page = intval($page);
        $page_size = intval($page_size);
        // 分页返回数据
        if (($page > 0) && ($page_size > 0))
        {
            // 偏移量
            $start = ($page - 1) * $page_size;
            $start = max($start, 0);
            $sql .= " LIMIT {$start},{$page_size}";
        }
        
        return $sql;
    }

    /**
     * 生成GROUP by 语句
     *
     * @param array $where 传进来的参数条件
     * @return string sql语句
     */
    protected function groupbySql($where, $table = '')
    {
        if (!isset($where['groupby']) || empty($where['groupby']))
        {
            return '';
        }

        $sql_arr = array();
        $db = self::mysqlInstances($this->active_group);

        foreach($where['groupby'] as $val)
        {
            // 参数过滤
            $val = trim($db->escape_str($val));
            $sql_arr[] = $val;
        }

        $sql = '';

        if (count($sql_arr) > 0)
        {
            if ($table == '') {
                $sql = ' GROUP BY ' . '`' . implode('`,`', $sql_arr).'`';
            } else {
                $sql = "`{$table}`.`".implode("`,`{$table}`.`", $sql_arr).'`';
            }
        }
        return $sql;
    }
}