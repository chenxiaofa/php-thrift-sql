<?php

use Hbase\Thrift\TScan;

require_once __DIR__ . '/src/autoload.php';
//require_once __DIR__ . '/ThriftSQL.phar.bak';

//
//$c = new ThriftHbase\Client('192.168.20.222', 9090);
//$c->connect();
//
//$c->newScanner('strategy_engine')
//    ->addSingleColumnValueFilter('result', 'output', 'true', '=')
//    ->setScanCallback(function ($list){print_r($list);})
//    ->startScanner();


$impala = new \ThriftSQL\Impala('192.168.1.104', '8080', 'impala');

$impala->connect();

$result = $impala->queryAndFetchAll(<<<TEXT

select 
    t.ymd,
    '' app,
    '' version,
    '' channel,
    
    count(distinct(t.udid)) as new_user,
        sum(if(after_day=1,1,0))  after_1_day,
    sum(if(after_day=2,1,0))  after_2_day,
    sum(if(after_day=3,1,0))  after_3_day,
    sum(if(after_day=4,1,0))  after_4_day,
    sum(if(after_day=5,1,0))  after_5_day,
    sum(if(after_day=6,1,0))  after_6_day,
    sum(if(after_day=7,1,0))  after_7_day
from
    (
        select 
            n.ymd,
            n.app,
            n.version,
            n.channel,
            n.udid,
            (unix_timestamp(cast(e.ymd as string),'yyyyMMdd')-unix_timestamp(cast(n.ymd as string),'yyyyMMdd'))/86400 after_day
        from 
            ( 
                select 
    udid,
    app_key app,
    app_ver version,
    channel,
    firstymd ymd 
from 
    cl_report.app_udevent_new_user_log 
where 
app_key in ('958d187fe094e4a5015d77d1778005e1','ef67aef2be557d56d80ac71c8e7fbb04')
    and firstymd >= 20170902
    and firstymd <= 20170909
    and ((app_key = 'ef67aef2be557d56d80ac71c8e7fbb04' and event_id = 'hmds-index' and event_param_key in ('首页点击')) or (app_key = '958d187fe094e4a5015d77d1778005e1' and event_id = 'hmds-index' and event_param_key in ('首页点击')))
    and ymd = 20170910 
            ) n
            left join (
                select 
                    ymd,udid
                from 
                    cl_report.app_udevent_day_log 
                where 
                    app_key in ('958d187fe094e4a5015d77d1778005e1','ef67aef2be557d56d80ac71c8e7fbb04')
                    and ymd > 20170902
                    and ymd <= 20170916
                    and ((app_key = '958d187fe094e4a5015d77d1778005e1' and event_id = 'hmds-index' and event_param_key in ('首页点击')) or (app_key = 'ef67aef2be557d56d80ac71c8e7fbb04' and event_id = 'hmds-index' and event_param_key in ('首页点击')))
                group by ymd,udid
            ) e on  e.udid=n.udid 
    ) t
group by t.ymd,app,version,channel
order by t.ymd desc

TEXT
);

print_r($result);
