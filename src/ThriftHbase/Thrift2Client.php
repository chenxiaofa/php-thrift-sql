<?php
namespace ThriftHbase;

use Hbase\Thrift2\TGet;
use Hbase\Thrift2\THBaseServiceClient;
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
 * @method get(String $table, String $row, String $column, Array $attribute)
 */
class Thrift2Client
{
    protected $socket = null;
    protected $transport = null;
    protected $protocol = null;
    protected $client = null;

    public function __construct($host='127.0.0.1', $port='9090', $timeout=1000)
    {
        $this->socket = new TSocket( $host, $port );
        $this->socket->setSendTimeout( $timeout ); // Ten seconds (too long for production, but this is just a demo ;)
        $this->socket->setRecvTimeout( $timeout ); // Twenty seconds
        $this->transport = new TBufferedTransport( $this->socket );
        $this->protocol = new TBinaryProtocol( $this->transport );
        $this->client = new THBaseServiceClient( $this->protocol );
    }

    public function connect()
    {
        if ($this->transport->isOpen())
        {
            $this->transport->close();
        }
        $this->transport->open();
        return $this;
    }

    public function _get($table, $row, $column='', $attribute='')
    {
        $get = new TGet();
        $get->row = $row;
        $values = $this->client->get($table, $get);
        return $values;
    }

    public function __call($name, $arguments)
    {
        $try = 0;
        do{
            try{
                switch($name)
                {
                    case 'get':
                        return call_user_func_array([$this, '_get'], $arguments);
                }
            }catch (TTransportException $e)
            {
                if ($try++ >= 3)
                {
                    throw $e;
                }
                $this->connect();
            }
            catch (IOError $e)
            {
                return false;
            }
        }while(1);

    }

}