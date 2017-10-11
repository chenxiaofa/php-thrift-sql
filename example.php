<?php

use Hbase\Thrift\TScan;

require_once __DIR__ . '/src/autoload.php';
//require_once __DIR__ . '/ThriftSQL.phar.bak';


$c = new ThriftHbase\Client('192.168.20.222', 9090, 10);
$c->connect();

while(1){
    try{
        $a = $c->get('strategy_engine', '00011768453b87b9b95f79e949550b6dace45650_ef67aef2be557d56d80ac71c8e7fbb04_6');
        var_dump($a);
        echo date('YmdHis'),"\n";
    }catch (\Exception $e)
    {
        print_r($e);
    }
}

