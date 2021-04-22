<?php

//路徑設定
include_once('config.inc');
include_once(PATH_LIB . "db.inc");


//$invoice_id = 100080240; //要開始清除的invoice_id
$invoice_time = date("Y-m-04 00:00:00", strtotime(date("y-m-d h:i:s")."-6 month"));
$month = date("m04", strtotime(date("y-m-d h:i:s")."-6 month"));
//$invoice_time = '2020-09-01 00:00:00'; //要開始清除的日期

echo 'create table start'."\r\n";

//檢查是否已經建立
$create_sql = db_read("SHOW TABLES LIKE '__del_game_".$month."';", null, true);

if($create_sql['count']>0){
    echo 'create table err __del_game'."\r\n";
    exit;
}
db_read("CREATE TABLE `__del_game_".$month."` (
	`type` INT NULL,
	`issue` CHAR(50) NULL,
    `open_at` INT NULL,
	INDEX `type` (`type`),
	INDEX `issue` (`issue`)
)
ENGINE=MyISAM
COLLATE='utf8_general_ci';", null, true);

$create_sql = db_read("SHOW TABLES LIKE '__del_bet_".$month."';", null, true);

if($create_sql['count']>0){
    echo 'create table err __del_bet'."\r\n";
    exit;
}
db_read("CREATE TABLE `__del_bet_".$month."` (
	`id` INT ,PRIMARY KEY (  id )
);", null, true);

$create_sql = db_read("SHOW TABLES LIKE '__del_change_".$month."';", null, true);
if($create_sql['count']>0){
    echo 'create table err __del_change'."\r\n";
    exit;
}
db_read("CREATE TABLE `__del_change_".$month."` (
	`id` INT ,PRIMARY KEY (  id )
);", null, true);


echo 'create table end'."\r\n";
echo 'insert __del start'."\r\n";
db_read("insert into __del_game_".$month." select type,issue,open_at from Lottery_result where open_at < unix_timestamp('".$invoice_time."');", null, true);

$base_time = db_read("select max(open_at) from __del_game_".$month.";", null, true);
$base_time = $base_time['result'][0]["max(open_at)"];

$base_id = db_read("select max(id)  from Bet_invoice where created_at < ".$base_time.";", null, true);
$base_id = $base_id['result'][0] ["max(id)"];

db_read("insert into __del_bet_".$month." select Bet_invoice.id from __del_game_".$month." , Bet_invoice where Bet_invoice.created_at < ".$base_time ." and Bet_invoice.id < ".$base_id." and  Bet_invoice.type_id = __del_game_".$month.".type and Bet_invoice.issue = __del_game_".$month.".issue;", null, true);
db_read("insert into __del_change_".$month."  select distinct user_id from Deposit_change where created_at < ".$base_time, null, true);
echo 'insert __del end'."\r\n";
$invoice_offset = 10000;//間隔多少筆執行一次
$temp = 0;

do {
    $sql_del = "SELECT `id` FROM `__del_bet_".$month."` where `id` > {$temp} ORDER BY `id` ASC LIMIT {$invoice_offset}";
    $result = db_read($sql_del , null, true);
    $counts = $result['count']; # 取出資料的數量
    $id = [];
    $chase = [];

    if($counts<1){
        echo 'clear_end'."\r\n";
        break;
    }

    foreach ($result['result'] as $value) {
        $id[] = $value['id'];
        
    }
    $temp = $id[count($id)-1] ;
 
    $ids = implode(',', $id);
    

    $sql = db_read("SELECT `chase_id` FROM `Bet_invoice` where `id` in (".$ids.")", null, true);
    foreach ($sql['result'] as $v) {
        $chase[] = $v['chase_id'];
    }

    

    $chases = implode(',', array_unique($chase));
    
    if(!$chases){
        $chases = 0;
    }

    //執行刪除
    $incvoice_sql = "delete from `Bet_invoice` where `id` in (".$ids.")";
    $report_sql = "delete from `Bet_report` where `invoice_id` in (".$ids.")";
    $chase_sql = "delete from `Bet_chase` where `id` in (".$chases.")";
    $agent_sql = "delete from `Bet_report_agent_data` where `invoice_id` in (".$ids.")";

    $invoice_res = db_write($incvoice_sql);
    $report_res = db_write($report_sql);
    $chase_res = db_write($chase_sql);
    $agent_res = db_write($agent_sql);
    //$star_id += $invoice_offset;
    echo $temp."\r\n";
    //var_dump($agent_sql);

} while($counts>0);
//exit;
echo 'insert_faker_start'."\r\n";

//-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

$star = intval(0); //使用者ID; 要開始寫假資料的起始點
$offset = 1000;//間格多少筆執行一次寫假資料

do {

    # 取出會員資料

    $s = db_read("SELECT `id` FROM `Users` where `id` in (select `id` from __del_change_".$month." ) and `role` not in('guest') LIMIT {$star},{$offset}", null, true);;
    //var_dump($s);
    # 如果沒資料結束運行

    $count = $s['count']; # 取出資料的數量
    $user_list = &$s['result'];  # 會員資料
    //var_dump($user_list);
    if($count<1){
        echo 'insert_faker_end'."\r\n";
        break;
    }

    # 會員資料整理
    $value = array(); # 暫存資料陣列 存異動清單 ( Deposit_change ) 的假資料內容
    $sql_insert_set = '';

    foreach($user_list as $user ){

        # 取出會員資料整理成陣列
        $temp_users = array(
            'user_id' => $user['id'],
            'memo' => "'手動重置存款點'",
            'action' => 31,
            'created_at' => 'unix_timestamp()',
            'water_change' => 0,
            'percentage_change' => 0,
            'surplus_credit' => 0,
            'surplus_change' => "(select surplus_credit from Users where id=".$user['id'].")"
        );

        # 複製五筆 
        for ($i=0; $i < 5; $i++) { 
            $value[] = sprintf("(%s)",join(",",$temp_users)); 
        }
        
    }
    unset($user_list);
    
    # 新增 存異動清單 ( Deposit_change ) Sql 串接
    $values = join(",",$value);
    unset($value);
    
    $sql_insert_set = sprintf(
        "INSERT INTO 
            Deposit_change (`user_id`,`memo`,`action`,`created_at`,`water_change`,`percentage_change`,`surplus_credit`,`surplus_change`) 
        VALUES %s;"
            ,$values
    );
    
    unset($values);
    
    # 執行
    $result = db_write($sql_insert_set);
    unset($sql_insert_set);
    //exit();
    if ($result["error"] != 0)
	{
        echo sprintf("id [ %s ] err",$star);
		exit;
	}

    $star += $offset;
    echo $star."\n";
} while($count>0);


//刪除獎期
echo 'delete Lottery_result start'."\r\n";;
$info_sql = "delete from `Lottery_result` where `created_at` < unix_timestamp('".$invoice_time."')";
$info_res = db_write($info_sql);
echo 'delete Lottery_result end'."\r\n";

//刪除獎期鎖
echo 'delete Lottery_result_lock start'."\r\n";
$info_sql = "delete from `Lottery_result_lock` where `create_at` < unix_timestamp('".$invoice_time."')";
$info_res = db_write($info_sql);
echo 'delete Lottery_result_lock end'."\r\n";

//刪除異動紀錄
echo 'delete Deposit_change start'."\r\n";
$info_sql = "delete from `Deposit_change` where `created_at` < unix_timestamp('".$invoice_time."')";
$info_res = db_write($info_sql);
//var_dump($info_sql);
echo 'delete Deposit_change end'."\r\n";

//刪除debug_log
echo 'delete debug_log start'."\r\n";
$info_sql = "delete from `debug_log` where `time` < '".$invoice_time."'";
$info_res = db_write($info_sql);
//var_dump($info_sql);
echo 'delete debug_log end'."\r\n";

echo 'optimize table start'."\r\n";
db_read("OPTIMIZE TABLE `shishi`.`Lottery_result`", null, true);
db_read("OPTIMIZE TABLE `shishi`.`Lottery_result_lock`", null, true);
db_read("OPTIMIZE TABLE `shishi`.`Deposit_change`", null, true);
db_read("OPTIMIZE TABLE `shishi`.`Bet_invoice`", null, true);
db_read("OPTIMIZE TABLE `shishi`.`Bet_report`", null, true);
db_read("OPTIMIZE TABLE `shishi`.`Bet_chase`", null, true);
db_read("OPTIMIZE TABLE `shishi`.`Bet_report_agent_data`", null, true);
db_read("OPTIMIZE TABLE `shishi`.`debug_log`", null, true);
echo 'optimize table end'."\r\n";
exit;