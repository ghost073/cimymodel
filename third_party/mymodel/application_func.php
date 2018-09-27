<?php
/**
* 项目公共方法
* 
* @date     2014/08/09
*/
/**
* 自动加载 apppath下的类文件
* 
* @param	string	$class	类文件名	
*/
if (! function_exists('apppath_autoload')) {

	function apppath_autoload($class) {
		// 如果是系统的类不做判断，使用系统原方法判断
		if ((0 === strpos($class, config_item('subclass_prefix'))) || (0 === strpos($class, 'CI_')))
		{
			return	FALSE;
		}
		
		// 判断是否在app下models和controllers中
		// 要判断的文件目录
		$apppath_arr = array('models', 'controllers');
		foreach ($apppath_arr AS $val)
		{
			$file_name = APPPATH . $val . '/' . ucfirst(strtolower($class)) . '.php';
			if (file_exists($file_name))
			{
				include_once($file_name);
				break;
			}
		}
	}
}

/*
* 输出json格式化的数据
* errno 为100 代表操作正常
* 
* @param    int     $errno      状态码
* @param    string  $errstr     状态码说明
* @param    mix     $errinfo    返回的数据 array/string
* @param    array   $extinfo    额外附加信息
* 
* @return   unknow
*/
if (!function_exists('json_msg')) {
	function json_msg($errno, $errstr, $errinfo = '', $extinfo = array()) {
	    $err_arr = array(
            'errno'     => $errno, 
            'errstr'    => $errstr,
            'info'      => $errinfo,  
        );
        
        // 有附加信息补充
        if (!empty($extinfo)) {
            $err_arr = array_merge($err_arr, $extinfo);
        }

	    echo json_encode($err_arr);
	    exit;
	}
}

/*
* 输出json格式化的数据
* errno 为100 代表操作正常
* 
* @param    int     $errno      状态码
* @param    string  $errstr     状态码说明
* @param    mix     $errinfo    返回的数据 array/string
* @param    array   $extinfo    额外附加信息
* 
* @return   unknow
*/
if (!function_exists('json_msg_client')) {
    function json_msg_client($errno, $errstr, $errinfo = '', $extinfo = array()) {
        $err_arr = array(
            'errno'     => $errno, 
            'errstr'    => $errstr, 
        );

        if (!empty($errinfo)) {
            $err_arr['info'] = $errinfo;
        }
        
        // 有附加信息补充
        if (!empty($extinfo)) {
            $err_arr = array_merge($err_arr, $extinfo);
        }

        echo json_encode($err_arr);
        exit;
    }
}

/**
* 输出jsonp格式化的数据
* errno 为100 代表操作正常
*
* @param    string  $callback   jsonp回调函数名
* @param    int     $errno      状态码
* @param    string  $errstr     状态码说明
* @param    mix     $errinfo    返回的数据 array/string
* @param    array   $extinfo    额外附加信息
* 
* @return   unknow
*/
if (!function_exists('jsonp_msg')) {
	function jsonp_msg($callback, $errno, $errstr, $errinfo = '', $extinfo = array()) {
	    $err_arr = array(
            'errno'     => $errno, 
            'errstr'    => $errstr,
            'info'      => $errinfo,  
        );

        // 有附加信息补充
        if (!empty($extinfo)) {
            $err_arr = array_merge($err_arr, $extinfo);
        }

	    echo $callback . '(' . json_encode($err_arr) . ')';
	    exit;
	}
}

/**
 * uc加密解密方法
 * 
 * @param  string $string    要加密的字符串
 * @param  string $operation 加密解密操作
 * @param  string $key       加密解密KEY
 * @param  int    $expiry    过期时间
 *
 *  @return string            加密串
 */
if (!function_exists('uc_authcode')) {
    
    function uc_authcode($string, $operation = 'DECODE', $key = '', $expiry = 0) {

        $ckey_length = 4;

        $key = md5($key);
        $keya = md5(substr($key, 0, 16));
        $keyb = md5(substr($key, 16, 16));
        $keyc = $ckey_length ? ($operation == 'DECODE' ? substr($string, 0, $ckey_length): substr(md5(microtime()), -$ckey_length)) : '';

        $cryptkey = $keya.md5($keya.$keyc);
        $key_length = strlen($cryptkey);

        $string = $operation == 'DECODE' ? base64_decode(substr($string, $ckey_length)) : sprintf('%010d', $expiry ? $expiry + time() : 0).substr(md5($string.$keyb), 0, 16).$string;
        $string_length = strlen($string);

        $result = '';
        $box = range(0, 255);

        $rndkey = array();
        for($i = 0; $i <= 255; $i++) {
            $rndkey[$i] = ord($cryptkey[$i % $key_length]);
        }

        for($j = $i = 0; $i < 256; $i++) {
            $j = ($j + $box[$i] + $rndkey[$i]) % 256;
            $tmp = $box[$i];
            $box[$i] = $box[$j];
            $box[$j] = $tmp;
        }

        for($a = $j = $i = 0; $i < $string_length; $i++) {
            $a = ($a + 1) % 256;
            $j = ($j + $box[$a]) % 256;
            $tmp = $box[$a];
            $box[$a] = $box[$j];
            $box[$j] = $tmp;
            $result .= chr(ord($string[$i]) ^ ($box[($box[$a] + $box[$j]) % 256]));
        }

        if($operation == 'DECODE') {
            if((substr($result, 0, 10) == 0 || substr($result, 0, 10) - time() > 0) && substr($result, 10, 16) == substr(md5(substr($result, 26).$keyb), 0, 16)) {
                return substr($result, 26);
            } else {
                return '';
            }
        } else {
            return $keyc.str_replace('=', '', base64_encode($result));
        }
    }
}

/**
 * 格式化数字
 * 大于1万，显示 1 + 万
 * 
 * @param  int    $num 要格式化的数字
 * 
 * @return string       格式化显示的数字
 */
if (!function_exists('format_num')) {
    
    function format_num($num = 0) {
        $num = intval($num);
        if ($num < 10000) {
            return $num;
        }

        $num_str = floor($num);
        $num_str .= '万';
        return $num_str;
    }
}

/**
* 版本号转成10进制数字 
*
* @param    string  $version    版本号：2.5.5.11
*
* @return   int 10进制数字 33883403
*/
if (!function_exists('version2dec')) {

	function version2dec($version)
	{
	    if (empty($version))
	    {
	        return 0;
	    }
	    
	    $new_arr = array();
	    $ver_arr = explode('.', $version);
	    foreach ($ver_arr AS $key => $val)
	    {
	        $new_arr[] = str_pad(dechex($val), 2, '0', STR_PAD_LEFT);;
	    }
	    $hex_str = '0x' . implode('', $new_arr);
	    $dec_str = hexdec($hex_str);
	    return $dec_str;
	}
}

/**
* 生成随机值
*
* @param    int     $num        生成的长度
*
* @return   string              随机值
*/
if (!function_exists('random_str')) {
    function random_str($num = 6) {
        // 参数过滤
        $num = intval($num);
        if ($num < 1) {
            $num = 6;
        }
        // 生成密码使用的初始值
        $init_str = '0123456789abcdefghijklmnopqrstuvwxyz';
        // 返回的值
        $new_str = '';

        for ($i = 1; $i <= $num; $i ++) {
            // 字符串顺序随机
            $str = str_shuffle($init_str);
            // 截取指定长度
            $str = substr($str, $i, 1);

            $new_str .= $str;
        }

        return $new_str;
    }
}

/**
* 生成随机值
*
* @param    int     $num        生成的长度
*
* @return   string              随机值
*/
if (!function_exists('random_num')) {
    function random_num($num = 5) {
        // 参数过滤
        $num = intval($num);
        if ($num < 1) {
            $num = 6;
        }
        // 生成密码使用的初始值
        $init_str = '0123456789';
        // 返回的值
        $new_str = '';

        for ($i = 1; $i <= $num; $i ++) {
            // 字符串顺序随机
            $str = str_shuffle($init_str);
            // 截取指定长度
            $str = substr($str, $i, 1);

            $new_str .= $str;
        }

        return $new_str;
    }
}

/**
 * 过滤ID只返回ID>0的非重复值
 */
if (!function_exists('filter_ids')) {
function filter_ids(array $id_arr) {
    if (empty($id_arr)) {
        return array();
    }

    //过滤
    foreach ($id_arr as $key => $value) {
        $value = intval($value);
        if ($value < 1)
        {
            unset($id_arr[$key]);
            continue;
        }

        $id_arr[$key] = $value;
    }

    $id_arr = array_values(array_unique($id_arr));

    return $id_arr;
}
}

/**
 * 过滤id数组 返回key->val
 * example:
 * Array(9=>1, 5=>5, 4=>6)
 */
if (! function_exists('filter_keyforval'))
{
function filter_keyforval(array $data)
{
	if (empty($data)) {
		return false;
	}

	// 过滤数据
	foreach ($data as $key=>$val) {
		$key = intval($key);
		if ($key < 1) {
			unset($data[$key]);
			continue;
		}
		$val = intval($val);
		$id_arr[$key] = $val;
	}

	return $id_arr;
}
}

if (!function_exists('cut_pic_url')) {
/**
* 获得nginx图片截取数据
*  
* @param    string  $url    图片url地址
* @Param    int $width  截取后的宽
* @param    int $height 截取后的高
*
* @return   string  拼接后的地址
*/
function cut_pic_url($url, $width = 0, $height = 0)
{
    // 如果宽或者高小于1返回原图
    if (($width < 1) || ($height < 1))
    {
        return $url;
    }

    // 图片pathinfo
    $pic_pathinfo = pathinfo($url);
    $new_url = $pic_pathinfo['dirname'] . '/' . $pic_pathinfo['filename'];
    $new_url .= '@' . $width . 'x' . $height;
    if (isset($pic_pathinfo['extension'])) {
        $new_url .= '.' . $pic_pathinfo['extension'];
    }
    return $new_url;
}
}
if (!function_exists('cut_pic_url2')) {
/**
* 获得nginx图片截取数据2 按某一个尺寸切图
*  
* @param    string  $url 图片url地址
*
* @return   string  拼接后的地址
*/
function cut_pic_url2($url, $length=0)
{
    // 图片pathinfo
    $pic_pathinfo = pathinfo($url);
    $new_url = $pic_pathinfo['dirname'] . '/' . $pic_pathinfo['filename'];
    $new_url .= ":m{$length}";
    if (isset($pic_pathinfo['extension'])) {
        $new_url .= '.' . $pic_pathinfo['extension'];
    }
    return $new_url;
}
}


if (!function_exists('get_first_char')){
    //获取首字母
    function get_first_char($str){
    $num = intval($str);
    if($num > 0){ return substr($str,0,1);}
    
    if(empty($str)){return '';}
    $fchar=ord($str{0});
    if($fchar>=ord('A')&&$fchar<=ord('z')) return strtoupper($str{0});
    $s1=iconv('UTF-8','gb2312',$str);
    $s2=iconv('gb2312','UTF-8',$s1);
    $s=$s2==$str?$s1:$str;
    $asc=ord($s{0})*256+ord($s{1})-65536;

    if($asc>=-20319&&$asc<=-20284) return 'A';
    if($asc>=-20283&&$asc<=-19776) return 'B';
    if($asc>=-19775&&$asc<=-19219) return 'C';
    if($asc>=-19218&&$asc<=-18711) return 'D';
    if($asc>=-18710&&$asc<=-18527) return 'E';
    if($asc>=-18526&&$asc<=-18240) return 'F';
    if($asc>=-18239&&$asc<=-17923) return 'G';
    if($asc>=-17922&&$asc<=-17418) return 'H';
    if($asc>=-17417&&$asc<=-16475) return 'J';
    if($asc>=-16474&&$asc<=-16213) return 'K';
    if($asc>=-16212&&$asc<=-15641) return 'L';
    if($asc>=-15640&&$asc<=-15166) return 'M';
    if($asc>=-15165&&$asc<=-14923) return 'N';
    if($asc>=-14922&&$asc<=-14915) return 'O';
    if($asc>=-14914&&$asc<=-14631) return 'P';
    if($asc>=-14630&&$asc<=-14150) return 'Q';
    if($asc>=-14149&&$asc<=-14091) return 'R';
    if($asc>=-14090&&$asc<=-13319) return 'S';
    if($asc>=-13318&&$asc<=-12839) return 'T';
    if($asc>=-12838&&$asc<=-12557) return 'W';
    if($asc>=-12556&&$asc<=-11848) return 'X';
    if($asc>=-11847&&$asc<=-11056) return 'Y';
    if($asc>=-11055&&$asc<=-10247) return 'Z';
    return null;
}
}


if (!function_exists('get_redis_key'))
{
/**
 * 获得redis key的键名
 * 
 * @param  string $string    键名KEY
 * 
 * @return [type]            [description]
 */
function get_redis_key($key) {
    if (empty($key)) {
        return false;
    }

    $redis_key_conf = include(APPPATH.'config/rediskey.php');
    if (!isset($redis_key_conf[$key])) {
        return false;
    }

    $value = $redis_key_conf[$key];
    return $value;
}
}