<?php

use Hbase\Thrift\TScan;

//require_once __DIR__ . '/src/autoload.php';
require_once __DIR__ . '/ThriftSQL.phar';


$c = new ThriftHbase\Client('192.168.20.222', 9090);
$c->connect();
//
//print_r($c->get('strategy_cache', '05940f5c-8688-3661-8d76-7dd38b48a660_ef67aef2be557d56d80ac71c8e7fbb04'));exit;

$c->newScanner('strategy_cache')
    ->setScanBatchSize(1024)
//    ->setColumns(['base:app_key', 'base:udid'])
    ->addRowFilterRegexp('^0.*_ef67aef2be557d56d80ac71c8e7fbb04')
    ->addSingleColumnValueFilter('base', 'udid', '00005748-a0b9-3859-b7f6-df02487843b0')
    ->addSingleColumnValueFilter('result', 'output', '0')
    ->setScanCallback(function ($list)use(&$count){
        foreach($list as $item)
        {
            print_r($item);
        }
    })
    ->startScanner();

echo $count;

//$client = $c->getClient();
//
//
//
//$scan = new TScan(
//    [
//        'columns'=>["result:output"],
////        'filterString'=>"ValueFilter(=, 'binary:test')",
//        'filterString'=>"SingleColumnValueFilter('result', 'output', =, 'binary:1')",
//    ]);
//$scanid = $client->scannerOpenWithScan('strategy_cache', $scan, []);
//do {
//    $rowList = $client->scannerGetList($scanid,2);
//    if (!$rowList)break;
//    print_r($rowList);
//    echo count($rowList),"\n";
//
//
//}while($rowList);
//
//$client->scannerClose($scanid);
//

