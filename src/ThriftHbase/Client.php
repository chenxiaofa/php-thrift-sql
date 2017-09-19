<?php
namespace ThriftHbase;

use Hbase\Thrift\HbaseClient;
use Hbase\Thrift\Mutation;
use Hbase\Thrift\TScan;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;
use Hbase\Thrift\IOError;
use ThriftSQL\Exception;

/**
 * Created by PhpStorm.
 * User: xfachen
 * Date: 2017/8/24
 * Time: 19:29
 * @method get(String $table, String $row, Array $column = [])
 * @method put(String $table, String $row, Array $modifies)
 * @method exists(String $table, String $row, Array $column = [])
 * @method deleteRow(String $table, String $row)
 */
class Client
{
    public $socket = null;
    public $transport = null;
    public $protocol = null;
    public $client = null;

    /**
     * Client constructor.
     * @param string $host
     * @param string $port
     * @param int $timeout
     */
    public function __construct($host = '127.0.0.1', $port = '9090', $timeout = 1000)
    {
        $this->socket = new TSocket($host, $port);
        $this->socket->setSendTimeout($timeout); // Ten seconds (too long for production, but this is just a demo ;)
        $this->socket->setRecvTimeout($timeout); // Twenty seconds
        $this->transport = new TBufferedTransport($this->socket);
        $this->protocol = new TBinaryProtocol($this->transport);
        $this->client = new HbaseClient($this->protocol);
    }

    /**
     * return HbaseClient instance
     * @return HbaseClient
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * close
     */
    public function close()
    {
        if ($this->transport->isOpen()) {
            $this->transport->close();
        }
    }

    /**
     * @return $this
     */
    public function connect()
    {
        if ($this->transport->isOpen()) {
            $this->transport->close();
        }
        $this->transport->open();
        return $this;
    }

    /**
     * Return true if row or column exists, otherwise return false
     * @param $table
     * @param $row
     * @param array $column
     * @return bool
     */
    protected function _exists($table, $row, $column = [])
    {
        return !is_null($this->_get($table, $row, $column));
    }

    /**
     * Get data from row
     * @param $table
     * @param $row
     * @param array $column
     * @return array|null
     * @throws IOError
     */
    protected function _get($table, $row, $column = [])
    {
        if (!is_array($column)) {
            $column = [(string)$column];
        }
        try {
            if ($column) {
                $return = $this->client->getRowWithColumns($table, $row, $column, []);
            } else {
                $return = $this->client->getRow($table, $row, []);
            }
        } catch (IOError $error) {
            $message = $error->getMessage();
            if (strpos($message, 'org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException') !== false ||
                strpos($message, 'org.apache.hadoop.hbase.TableNotFoundException') !== false
            ) {
                //table or column not found, return null
                return null;
            }
            throw $error;
        }

        $output = [];
        if (empty($return)) return null;
        foreach ($return[0]->columns as $name => $cell) {
            $output[$name] = $cell->value;
        }
        return $output;
    }

    /**
     * Put data to row
     * @param $table
     * @param $row
     * @param $modifies
     */
    protected function _put($table, $row, $modifies)
    {
        $mutations = [];
        foreach ($modifies as $column => $value) {
            $mutations[] = new Mutation([
                'column' => $column,
                'value' => $value
            ]);
        }
        $this->client->mutateRow($table, $row, $mutations, []);
    }

    /**
     * delete a row
     * @param $table
     * @param $row
     */
    public function _deleteRow($table, $row)
    {
        $this->client->deleteAllRow($table, $row, []);
    }

    /**
     * @param $name
     * @param $arguments
     * @return bool|mixed
     * @throws TTransportException
     * @throws \Exception
     */
    public function __call($name, $arguments)
    {
        $try = 0;
        do {
            try {
                switch ($name) {
                    case 'get':
                        return call_user_func_array([$this, '_get'], $arguments);
                    case 'put':
                        return call_user_func_array([$this, '_put'], $arguments);
                    case 'exists':
                        return call_user_func_array([$this, '_exists'], $arguments);
                    case 'deleteRow':
                        return call_user_func_array([$this, '_deleteRow'], $arguments);
                    default:
                        throw new \Exception('unknown method:' . $name);
                }
            } catch (TTransportException $e) {
                if ($try++ >= 3) {
                    throw $e;
                }
                $this->connect();
            }
        } while (1);
        //never arrive there
        return false;
    }



    /**
     * @param $table
     * @return Scanner
     */
    public function newScanner($table)
    {
        return new \ThriftHbase\Scanner($this, $table);
    }


    public function getTableRegions($table)
    {
        return $this->client->getTableRegions($table);
    }


}