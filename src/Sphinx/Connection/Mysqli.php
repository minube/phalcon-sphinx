<?php

namespace Adapters\Sphinx\Connection;

use Adapters\Sphinx\Exception\ConnectionException;

/**
 * Class Mysqli provides connection features for SphinxQL library using Mysqli driver
 *
 * @property \mysqli $resource Mysqli resource
 * @method \mysqli getResource() Return resource
 */
class Mysqli extends AbstractConnection
{
    /**
     * Driver name
     */
    const DRIVER_NAME = 'mysqli';

    /**
     * Connection parameters
     *
     * @var array
     */
    protected $connectionParameters = array();

    /**
     * Constructor
     *
     * $connectionParameters can be either a an array of parameters or a \mysqli object. If $connectionParameters is NULL
     * then default {@see Mysqli::$defaultConnectionParameters} connection parameters are used.
     *
     * Connection parameters:
     *
     * - 'host': hostname to connect to, default is '127.0.0.1'
     * - 'port: port to connect to, default is 9306
     * - 'unix_socket': path to the UNIX socket to connect to
     * - 'options': \mysqli driver options
     *
     * @link http://us2.php.net/manual/en/mysqli.options.php
     *
     * @param null|array|\mysqli $connectionParameters Connection parameters
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    public function __construct($connectionParameters = null)
    {
        if (!extension_loaded(static::DRIVER_NAME)) {
            throw new ConnectionException('"mysqli" extension is not installed');
        }

        if (is_array($connectionParameters)) {
            $this->setConnectionParameters($connectionParameters);
        } else if ($connectionParameters instanceof \mysqli) {
            $this->resource = $connectionParameters;
        } else if ($connectionParameters === null) {
            $this->setConnectionParameters(self::$defaultConnectionParameters);
        } else {
            throw new \InvalidArgumentException('Argument $connectionParameters must be either an array, a \\mysqli object or NULL');
        }
    }

    /**
     * Return driver name
     *
     * @return string
     */
    public function getDriverName()
    {
        return static::DRIVER_NAME;
    }

    /**
     * Establish database connection
     *
     * @return Mysqli
     * @throws ConnectionException
     */
    public function connect()
    {
        if ($this->isConnected()) {
            return $this;
        }

        /**
         * @var string $host
         * @var int $port
         * @var string $unix_socket
         */

        extract($this->connectionParameters);

        $this->resource = new \mysqli();
        $this->resource->init();

        foreach ($this->driverOptions as $option => $value) {
            if (is_string($option)) {
                $option = strtoupper($option);

                if (!defined($option)) {
                    continue;
                }

                $option = constant($option);
            }

            $this->resource->options($option, $value);
        }

        if ($this->resource->real_connect($host, null, null, null, $port, $unix_socket) === false) {
            throw new ConnectionException('Error opening connection to the Sphinx server', 0,
                new \ErrorException($this->resource->connect_error, $this->resource->connect_errno)
            );
        }

        return $this;
    }

    /**
     * Check whether database connection has been established
     *
     * @return bool
     */
    public function isConnected()
    {
        return ($this->resource instanceof \mysqli);
    }

    /**
     * Disconnect
     *
     * @return Mysqli
     */
    public function disconnect()
    {
        if ($this->isConnected()) {
            $this->resource->close();
        }

        $this->resource = null;
        return $this;
    }

    /**
     * Begin transaction
     *
     * @return Mysqli
     */
    public function beginTransaction()
    {
        $this->connect();

        $this->resource->begin_transaction();
        $this->resource->autocommit(false);

        $this->inTransaction = true;
        return $this;
    }

    /**
     * Commit transaction
     *
     * @return Mysqli
     */
    public function commit()
    {
        $this->connect();

        $this->resource->commit();
        $this->resource->autocommit(true);

        $this->inTransaction = false;
        return $this;
    }

    /**
     * Rollback transaction
     *
     * @return Mysqli
     * @throws ConnectionException
     */
    public function rollback()
    {
        if (!$this->isConnected()) {
            throw new ConnectionException('Cannot rollback transaction: connection is not established');
        } else if (!$this->inTransaction) {
            throw new ConnectionException('Cannot rollback transaction: transaction is not started');
        }

        $this->resource->rollback();
        $this->resource->autocommit(true);

        return $this;
    }

    /**
     * Place quotes around the $string and escape special characters
     *
     * @param mixed $value String to quote
     * @return string
     */
    public function quoteString($value)
    {
        $this->connect();

        $value = $this->resource->real_escape_string((string)$value);
        return "'" . $value . "'";
    }

    /**
     * Execute SQL query and return fetched data
     *
     * @param string $query SQL query to execute
     * @return array[]
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    public function fetchAll($query)
    {
        if (!is_string($query) || ($query = trim($query)) == '') {
            throw new \InvalidArgumentException('Argument $sql must be a string');
        }

        $this->connect();

        if (($mysqli_result = $this->resource->query($query)) === false) {
            throw new ConnectionException('Error executing SQL', 0,
                new \ErrorException($this->resource->error, $this->resource->errno));
        } else if (!$mysqli_result instanceof \mysqli_result) {
            throw new ConnectionException('Error fetching data: no \\mysqli_result returned for the SQL');
        }

        $result = array();

        /**
         * iterator_to_array($result) may produce unexpected result, because there is no way to set default
         * fetch mode for \mysqli_result. Normally it returns associative array.
         */

        while ($row = $mysqli_result->fetch_assoc()) {
            $result[] = $row;
        }

        $mysqli_result->free_result();
        return $result;
    }

    /**
     * Execute SQL query and return a single row
     *
     * @param string $query SQL query to execute
     * @return array|null
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    public function fetchOne($query)
    {
        if (!is_string($query) || ($query = trim($query)) == '') {
            throw new \InvalidArgumentException('Argument $sql must be a string');
        }

        $this->connect();

        if (($mysqli_result = $this->resource->query($query)) === false) {
            throw new ConnectionException('Error executing SQL', 0,
                new \ErrorException($this->resource->error, $this->resource->errno));
        } else if (!$mysqli_result instanceof \mysqli_result) {
            throw new ConnectionException('Error fetching data: no \\mysqli_result returned for the SQL');
        }

        $result = $mysqli_result->fetch_assoc();

        $mysqli_result->free_result();
        return (is_array($result) ? $result : null);
    }

    /**
     * Execute SQL query and return number of affected rows
     *
     * @param string $query SQL query to execute
     * @return int
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    public function execute($query)
    {
        if (!is_string($query) || ($query = trim($query)) == '') {
            throw new \InvalidArgumentException('Argument $sql must be a string');
        }

        $this->connect();

        if ($this->resource->query($query) === false) {
            throw new ConnectionException('Error executing SQL', 0,
                new \ErrorException($this->resource->error, $this->resource->errno));
        }

        return $this->resource->affected_rows;
    }

    /**
     * Set connection parameters
     *
     * @param array $connectionParameters Connection parameters
     * @return Mysqli
     * @throws \InvalidArgumentException
     */
    protected function setConnectionParameters(array $connectionParameters)
    {
        $unixSocket = $host = $port = null;
        $driverOptions = array();

        foreach ($connectionParameters as $key => $value) {
            $key = strtolower($key);

            switch ($key) {
                case 'host':
                    $host = trim($value);
                    break;

                case 'port':
                    $port = (int)$value;
                    break;

                case 'unix_socket':
                    $unixSocket = trim($value);
                    break;

                case 'options':
                    $driverOptions = $value;
                    break;

                default:
                    throw new \InvalidArgumentException(sprintf('Configuration option $connectionParameters[\'%s\'] is not supported', $key));
            }
        }

        if (!is_array($driverOptions)) {
            throw new \InvalidArgumentException('Configuration option $connectionParameters[\'options\'] must be an array');
        }

        /**
         * If 'unix_socket' is set, but 'host' isn't, then the default '127.0.0.1' must not be used
         */

        $this->connectionParameters = array(
            'host' => (empty($host) && empty($unixSocket) ? self::$defaultConnectionParameters['host'] : $host),
            'port' => (!empty($port) ? $port : self::$defaultConnectionParameters['port']),
            'unix_socket' => (empty($unixSocket) ? null : $unixSocket)
        );

        $this->driverOptions = $driverOptions;
    }
}
