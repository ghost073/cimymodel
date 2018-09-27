<?php defined('BASEPATH') OR exit('No direct script access allowed');
/**
 * redis使用的key值
 *
 * 说明： 项目前缀:存储类型(4种):操作应用名:扩展其他
 * 关键字用() 包裹，前台可读性更高 
 */
return array(
    'git:ad:detail:(id)'  => [ // 广告
        'key'   => 'git:s:ad:d:%s',
        'time'  => 7200,
        'type'  => 'string',
    ],
);
