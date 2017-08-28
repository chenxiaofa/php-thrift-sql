<?php
/**
 * Created by PhpStorm.
 * User: xfachen
 * Date: 2017/8/25
 * Time: 15:53
 */
include __DIR__.'/../src/autoload.php';

$client = new ThriftHbase\Client('192.168.234.236', 9091);
$client->connect();

$result = $client->get('testhbase', 'fc523bb7-2146-3841-888a-419ae6ebe522_ef67aef2be557d56d80ac71c8e7fbb04', 'info:pt');

print_r($result);

$client->put('testhbase', 'fc523bb7-2146-3841-888a-419ae6ebe522_ef67aef2be557d56d80ac71c8e7fbb04', ['info:pt'=>rand(100000,999999)]);

$result = $client->get('testhbase', 'fc523bb7-2146-3841-888a-419ae6ebe522_ef67aef2be557d56d80ac71c8e7fbb04');

print_r($result);