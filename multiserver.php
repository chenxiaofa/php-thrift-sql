<?php

use Hbase\Thrift\TScan;

require_once __DIR__ . '/src/autoload.php';
//require_once __DIR__ . '/ThriftSQL.phar.bak';
//

$c = new ThriftHbase\Client('192.168.20.222', 9090);
$c = new ThriftHbase\Client([
    ['host'=>'192.168.20.201','port'=>'9090','weight'=>100],
    ['host'=>'192.168.20.222','port'=>'9090','weight'=>100],
    ['host'=>'192.168.20.202','port'=>'9090','weight'=>100],
]);


while(1)
{
    $c->connect();
    echo $c->host, "\n";
    print_r($c->get('xxx', '123123'));

}

$c->connect();

$c->newScanner('xxx')
    ->addSingleColumnValueFilter('result', 'output', 'true', '=')
    ->setScanBatchSize(1)
    ->setMaxReTry(10000000)
    ->setScanCallback(function ($list)use($c){
        echo $c->host,"\n";
        print_r($list);
        $c->close();
    })
    ->startScanner();

