<?php

require_once __DIR__.'/src/autoload.php';





$pidArray = [];
$files = glob('/data/ext/udid/x???');
$s = microtime(1);
$pid = 0;
$f = '';
while($files) {
    while (count($pidArray) >= 2) {
        foreach($pidArray as $pid) {
            if (pcntl_waitpid($pid, $status, WNOHANG) != 0) {

                unset($pidArray[$pid]);
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




//$client = new ThriftHbase\Client('192.168.1.15', 9090);
$client = new ThriftHbase\Client('192.168.234.236', 9091);
$client->connect();
$total = 0;
$count = 0;
foreach (file($f) as $udid){
    $udid = trim($udid);
    $s = microtime(1);
    $result = $client->get('testhbase', $udid.'_ef67aef2be557d56d80ac71c8e7fbb04', 'info', []);
    $total += microtime(1)-$s;
    $count++;
}

echo "use:",(round($total*1000,2)),"\t\t","count:",$count,"\t\t","avg:",(round($total*1000/$count,2)),"\n";