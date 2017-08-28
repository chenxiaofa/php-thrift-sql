<?php

use Hbase\Thrift\TScan;

require_once __DIR__ . '/src/autoload.php';


$client = new ThriftHbase\Client('192.168.20.222', 9090);
$client->connect();


$client = $client->getClient();


//$filter = "QualifierFilter(>=, 'binary:1500')"; // greater than 1500
$filter = null; // greater than 1500
$scan = new TScan(array('sortColumns' => true, 'batchSize'=>10));
$scanid = $client->scannerOpenWithScan('testhbase', $scan, []);
while($scanid){
    $rowresult = $client->scannerGet($scanid);

    echo("\nrow: {$rowresult[0]->row}, cols: \n\n");

    $values = $rowresult[0]->sortedColumns;

    foreach ($values as $k => $v) {
        echo("  {$v->columnName} => {$v->cell->value}\n");
    }
}

$client->scannerClose($scanid);

