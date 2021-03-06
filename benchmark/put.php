<?php
/**
 * Created by PhpStorm.
 * User: xfachen
 * Date: 2017/8/25
 * Time: 15:39
 */

require_once __DIR__.'/../src/autoload.php';

$process_count = 10;

$pidArray = [];
$files = glob('/data/ext/udid/x???');
$s = microtime(1);
$pid = 0;
$f = '';
while($files) {
    while (count($pidArray) >= $process_count) {
        foreach($pidArray as $pid) {
            if (pcntl_waitpid($pid, $status, WNOHANG) != 0) {

                unset($pidArray[$pid]);
            }else{
                usleep(10000);
            }
        }
    }
    $f = array_pop($files);
    $pid = pcntl_fork();
    if ($pid == 0)break;
    $pidArray[$pid] = $pid;
}

if ($pid > 0) {
    foreach($pidArray as $pid){
        pcntl_waitpid($pid, $status);
    }
    echo "\n\n","total used:",(microtime(1)-$s)*1000,"\n\n";
    exit;
}


$client = new ThriftHbase\Client('192.168.20.222', 9090);
$client->connect();
$total = 0;
$count = 0;
foreach (file($f) as $udid){
    $udid = trim($udid);
    $s = microtime(1);
    $result = $client->put('testhbase', $udid.'_ef67aef2be557d56d80ac71c8e7fbb04', ['info:pt'=>rand(100000,999999)]);
    $total += microtime(1)-$s;
    $count++;
}

echo "use:",(round($total*1000,2)),"\t\t","count:",$count,"\t\t","avg:",(round($total*1000/$count,2)),"\n";