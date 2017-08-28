<?php
namespace ThriftHbase;

use Hbase\Thrift\HbaseClient;
use Thrift\Exception\TTransportException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;
use Hbase\Thrift\IOError;

/**
 * Created by PhpStorm.
 * User: xfachen
 * Date: 2017/8/24
 * Time: 19:29
 * @method get(String $table, String $row, Array $column=[])
 * @method put(String $table, String $row, Array $modifies)
 */
class Client
{
    protected $socket = null;
    protected $transport = null;
    protected $protocol = null;
    protected $client = null;

    public function __construct($host = '127.0.0.1', $port = '9090', $timeout = 1000)
    {
        $this->socket = new TSocket($host, $port);
        $this->socket->setSendTimeout($timeout); // Ten seconds (too long for production, but this is just a demo ;)
        $this->socket->setRecvTimeout($timeout); // Twenty seconds
        $this->transport = new TBufferedTransport($this->socket);
        $this->protocol = new TBinaryProtocol($this->transport);
        $this->client = new HbaseClient($this->protocol);
    }

    public function connect()
    {
        if ($this->transport->isOpen()) {
            $this->transport->close();
        }
        $this->transport->open();
        return $this;
    }

    public function _exists($table, $row)
    {
        $return = $this->client->getRow($table, $row, []);
        return !empty($return);
    }

    public function _get($table, $row, $column = [])
    {
        if ($column) {
            $return = $this->client->getRowWithColumns($table, $row, [$column], []);
        } else {
            $return = $this->client->getRow($table, $row, []);
        }

        $output = [];
        if (empty($return)) return NULL;
        foreach ($return[0]->columns as $name => $cell) {
            $output[$name] = $cell->value;
        }
        return $output;
    }

    public function _put($table, $row, $modifies)
    {
        $mutations = [];
        foreach ($modifies as $column => $value) {
            $mutations[] = new \Hbase\Thrift\Mutation([
                'column' => $column,
                'value' => $value
            ]);
        }
        $this->client->mutateRow($table, $row, $mutations, []);
    }


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
                }
            } catch (TTransportException $e) {
                if ($try++ >= 3) {
                    throw $e;
                }
                $this->connect();
            } catch (IOError $e) {
                return false;
            }
        } while (1);

    }

}