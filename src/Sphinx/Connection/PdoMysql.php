<?php

namespace Adapters\Sphinx\Connection;

use Adapters\Sphinx\Exception\ConnectionException;

/**
 * Class PdoMysql provides connection features for SphinxQL library using PDO Mysql driver
 *
 * @property \PDO $resource Mysqli resource
 * @method \PDO getResource() Return resource
 */
class PdoMysql extends AbstractConnection
{
    /**
     * Driver name
     */
    const DRIVER_NAME = 'pdo_mysql';

    /**
     * Connection string (DSN)
     *
     * @var string
     */
    protected $dsn = null;

    /**
     * Constructor
     *
     * $connectionParameters can be either a DSN string, an array of parameters or a \PDO object. If $connectionParameters is NULL
     * then default {@see PdoMysql::$defaultConnectionParameters} connection parameters will be used.
     *
     * Connection parameters:
     *
     * - 'dsn': connection string and the preferred connection method
     *   -- OR --
     * - 'unix_socket': path to the UNIX socket to connect to, it is used if DSN is not set in the $connectionParameters
     *   -- OR --
     * - 'host': hostname to connect to, default is '127.0.0.1'
     * - 'port: port to connect to, default is 9306
     *   -- AND / OR --
     * - 'options': \PDO driver options
     *
     * 'host:port' is used if both DSN and 'unix_socket' are not present in the $connectionParameters
     *
     * @link http://www.php.net/manual/en/pdo.constants.php
     * @link http://www.php.net/manual/en/ref.pdo-mysql.php
     *
     * @param null|string|array|\PDO $connectionParameters Connection parameters
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    public function __construct($connectionParameters = null)
    {
        if (!extension_loaded(static::DRIVER_NAME)) {
            throw new ConnectionException('"pdo_mysql" extension is not installed');
        }

        if (is_string($connectionParameters)) {
            if (strncasecmp($connectionParameters, 'mysql:', 6) == 0) {
                $this->dsn = $connectionParameters; // $connectionParameters is a DSN string
            } else {
                throw new ConnectionException(sprintf('Malformed PDO MySQL DSN string "%s"', $connectionParameters));
            }
        } else if ($connectionParameters instanceof \PDO) {
            $this->resource = $connectionParameters;
        } else if (is_array($connectionParameters)) {
            $this->setConnectionParameters($connectionParameters);
        } else if ($connectionParameters === null) {
            $this->setConnectionParameters(self::$defaultConnectionParameters);
        } else {
            throw new \InvalidArgumentException('Argument $connectionParameters must be either a string, an array, a \\PDO object or NULL');
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
     * @return PdoMysql
     * @throws ConnectionException
     */
    public function connect()
    {
        if ($this->isConnected()) {
            return $this;
        }

        try {
            $this->resource = new \PDO($this->dsn, null, null, $this->driverOptions);
            $this->resource->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            /**
             * Sphinx does not support prepared statements, however, they can be emulated
             * by the \PDO driver.
             */

            $this->resource->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
        } catch (\PDOException $exception) {
            throw new ConnectionException('Error opening connection to the Sphinx server', 0, $exception);
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
        return ($this->resource instanceof \PDO);
    }

    /**
     * Disconnect
     *
     * @return PdoMysql
     */
    public function disconnect()
    {
        $this->resource = null;
        return $this;
    }

    /**
     * Begin transaction
     *
     * @return PdoMysql
     */
    public function beginTransaction()
    {
        $this->connect();
        $this->resource->beginTransaction();
        $this->inTransaction = true;

        return $this;
    }

    /**
     * Commit transaction
     *
     * @return PdoMysql
     */
    public function commit()
    {
        $this->connect();
        $this->resource->commit();
        $this->inTransaction = false;

        return $this;
    }

    /**
     * Rollback transaction
     *
     * @return PdoMysql
     * @throws ConnectionException
     */
    public function rollback()
    {
        if (!$this->isConnected()) {
            throw new ConnectionException('Cannot rollback transaction: connection is not established');
        } else if (!$this->inTransaction) {
            throw new ConnectionException('Cannot rollback transaction: transaction is not started');
        }

        $this->resource->rollBack();
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
        return $this->resource->quote((string)$value, \PDO::PARAM_STR);
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

        try {
            $stmt = $this->resource->query($query, \PDO::FETCH_ASSOC);
        } catch (\PDOException $exception) {
            throw new ConnectionException('Error executing SQL', 0, $exception);
        }

        try {
            $resultSet = iterator_to_array($stmt);
        } catch (\PDOException $exception) {
            $stmt->closeCursor();

            /**
             * Normally happens if PdoMysql::query() called for DML queries, e.g., INSERT, UPDATE, DELETE
             */

            throw new ConnectionException('Error fetching data', 0, $exception);
        }

        $stmt->closeCursor();
        return $resultSet;
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

        try {
            $stmt = $this->resource->query($query, \PDO::FETCH_ASSOC);
        } catch (\PDOException $exception) {
            throw new ConnectionException('Error executing SQL', 0, $exception);
        }

        try {
            $result = $stmt->fetch();
        } catch (\PDOException $exception) {
            $stmt->closeCursor();

            /**
             * Normally happens if PdoMysql::query() called for DML queries, e.g., INSERT, UPDATE, DELETE
             */

            throw new ConnectionException('Error fetching data', 0, $exception);
        }

        $stmt->closeCursor();
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

        try {
            $result = $this->resource->exec($query);
        } catch (\PDOException $exception) {
            throw new ConnectionException('Error executing SQL', 0, $exception);
        }

        return $result;
    }

    /**
     * Set connection parameters
     *
     * @param array $connectionParameters Connection parameters
     * @return PdoMysql
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    protected function setConnectionParameters(array $connectionParameters)
    {
        $dsn = $unixSocket = $host = $port = null;
        $driverOptions = array();

        foreach ($connectionParameters as $key => $value) {
            $key = strtolower($key);

            switch ($key) {
                case 'dsn':
                    $dsn = trim($value);
                    break;

                case 'unix_socket':
                    $unixSocket = trim($value);
                    break;

                case 'host':
                    $host = trim($value);
                    break;

                case 'port':
                    $port = (int)$value;
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

        if (isset($dsn)) {
            if (strncasecmp($dsn, 'mysql:', 6) == 0) {
                $this->dsn = $dsn;
            } else {
                throw new ConnectionException(sprintf('Malformed PDO MySQL DSN string "%s"', $dsn));
            }
        } else if (!empty($unixSocket)) {
            $this->dsn = sprintf('mysql:unix_socket=%s', $unixSocket);
        } else {
            $host = (!empty($host) ? $host : self::$defaultConnectionParameters['host']);
            $port = (!empty($port) ? $port : self::$defaultConnectionParameters['port']);

            $this->dsn = sprintf('mysql:host=%s;port=%d', $host, $port);
        }

        $this->driverOptions = $driverOptions;
    }
}
