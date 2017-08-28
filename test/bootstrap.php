<?php
/**
 * Created by PhpStorm.
 * User: xfachen
 * Date: 2017/8/25
 * Time: 16:38
 */
include __DIR__.'/../src/autoload.php';

$client = new ThriftHbase\Client('192.168.234.236', 9091);
$client->connect();