<?php
/**
 * Created by PhpStorm.
 * User: xfachen
 * Date: 2017/8/31
 * Time: 14:21
 */

namespace ThriftHbase;


use Exception;
use Hbase\Thrift\TScan;
use Thrift\Exception\TTransportException;

class Scanner
{
    /** @var Client */
    public $client = null;

    protected $filterColumns = [];
    protected $filters = [];
    protected $scanTable = '';
    /** @var TScan */
    protected $scanObject = null;
    protected $scanCallback = null;
    protected $scanBatchSize = 1024;
    protected $timeout = 30000;

    public function __construct($client, $table)
    {
        $this->client = $client;
        $this->scanTable = $table;
        $this->scanObject = new TScan();
    }

    public function setColumns($columns)
    {
        $this->scanObject->columns = $columns;
        return $this;
    }



    /**
     * @param $callback
     * @return $this
     * @throws Exception
     */
    public function setScanCallback($callback)
    {
        if (!is_callable($callback)){
            throw new Exception('invalid callbck');
        }
        $this->scanCallback = $callback;
        return $this;
    }

    public function addSingleColumnValueFilter($family, $qualifier, $comparator, $operator = '=')
    {
        if (in_array($operator, ['=', '>', '>=', '<', '<=', '!=']))
        {
            $column = $family.':'.$qualifier;
            $this->filterColumns[] = $column;
            $this->filters[] = "SingleColumnValueExcludeFilter ('$family', '$qualifier', $operator, 'binary:$comparator')";
        }

        return $this;
    }

    /**
     * Regexp row filter
     * @param $pattern
     * @param bool $not
     * @return $this
     */
    public function addRowFilterRegexp($pattern, $not=false)
    {
        $operator = '=';
        if ($not)$operator = '!=';
        $pattern = addslashes($pattern);
        $this->filters[] = "RowFilter ({$operator}, 'regexstring:{$pattern}')";
        return $this;
    }

    /**
     * @param $size
     * @return $this
     */
    public function setScanBatchSize($size)
    {
        $this->scanBatchSize = max([0, intval($size)]);
        return $this;
    }

    public function setTimeout($t = 30000)
    {
        $this->timeout = $t;
        return $this;
    }

    public function startScanner()
    {
        $this->client->socket->setRecvTimeout($this->timeout);
        $this->client->socket->setSendTimeout($this->timeout);
        $try = 0;
        $scanId = 0;
        $count = 0;
        if ($this->filters)
        {
            $this->scanObject->filterString = implode(' AND ', $this->filters);
        }

        if ($this->scanObject->columns && $this->filterColumns)
        {
            $this->scanObject->columns = array_merge($this->scanObject->columns, $this->filterColumns);
        }

        do{

            try{
                if (!$scanId)
                {
                    $scanId = $this->client->getClient()->scannerOpenWithScan($this->scanTable, $this->scanObject, []);
                }
                else{
                    $rowList = $this->client->getClient()->scannerGetList($scanId, $this->scanBatchSize);
                    if (!$rowList)break;
                    $count += count($rowList);

                    $output = [];
                    foreach($rowList as $row)
                    {
                        $tmp = ['row_key'=>$row->row];
                        foreach($row->columns as $name=>$column)
                        {
                            $tmp[$name] = $column->value;
                        }
                        $output[] = $tmp;
                    }

                    $stop = call_user_func_array($this->scanCallback, [$output]);

                    if ($stop === false)
                    {
                        break;
                    }

                }

            }catch (TTransportException $e){
                $try++;
                if ($try > 5){
                    throw $e;
                }
                $this->client->connect();
            }

        }while(1);

        if ($scanId)
        {
            $this->client->getClient()->scannerClose($scanId);
        }

    }



}