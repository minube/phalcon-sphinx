<?php

namespace Adapters\Sphinx\Connection;

use Adapters\Sphinx;
use Adapters\Sphinx\Exception\ConnectionException;

/**
 * Class AbstractConnection is used for building connection classes for SphinxQL
 */
abstract class AbstractConnection
{
    /**
     * Database driver options
     *
     * @var array
     */
    protected $driverOptions = array();

    /**
     * Database resource
     *
     * @var mixed
     */
    protected $resource = null;

    /**
     * Flag indicating that transaction has been started if TRUE
     *
     * @var bool
     */
    protected $inTransaction = false;

    /**
     * Default connection parameters: '127.0.0.1:9306'
     *
     * @var array
     */
    protected static $defaultConnectionParameters = array(
        'host' => '127.0.0.1',
        'port' => 9312
    );

    /**
     * Constructor
     *
     * @param mixed $connectionParameters Connection parameters
     */
    abstract public function __construct($connectionParameters = null);

    /**
     * Return resource
     *
     * @return mixed
     */
    public function getResource()
    {
        $this->connect();
        return $this->resource;
    }

    /**
     * Establish database connection
     *
     * @return AbstractConnection
     */
    abstract public function connect();

    /**
     * Check whether database connection has been established
     *
     * @return bool
     */
    abstract public function isConnected();

    /**
     * Disconnect
     *
     * @return AbstractConnection
     */
    abstract public function disconnect();

    /**
     * Begin transaction
     *
     * @return AbstractConnection
     */
    abstract public function beginTransaction();

    /**
     * Commit transaction
     *
     * @return AbstractConnection
     */
    abstract public function commit();

    /**
     * Rollback transaction
     *
     * @return AbstractConnection
     */
    abstract public function rollback();

    /**
     * Quote identifier (index name, column etc)
     *
     * Sphinx identifier has the same format as C identifier. However, in earlier Sphinx versions dash in index name
     * was allowed. This method does not check for identifier format, it simply wraps it in '`', if needed,
     * and let Sphinx deal with invalid identifiers.
     *
     * '*' symbol and Sphinx and user variables (starts with '@') are not quoted.
     *
     * @param string $value Value to quote
     * @return string
     */
    public function quoteIdentifier($value)
    {
        $value = trim($value, '` ');
        return ($value == '*' || substr($value, 0, 1) == '@' ? $value : '`' . $value . '`');
    }

    /**
     * Quote array or identifiers (index name, column etc) and return them as a comma-separated string
     *
     * @see AbstractConnection::quote(quoteValue
     *
     * @param array $values Values to quote
     * @return string
     */
    public function quoteIdentifierArray(array $values)
    {
        $result = array();

        foreach ($values as $value) {
            $result[] = $this->quoteIdentifier($value);
        }

        return implode(', ', $result);
    }

    /**
     * Quote value if necessary
     *
     * $type must be one of SphinxQL::TYPE_* constants. If $type is NULL, then the target type is detected from $value
     * and mapped to the corresponding SphinxQL::TYPE_* constant.
     *
     * Note: type auto-detection is limited and works only for integer, float, boolean, string, array (represented as MVA)
     * and object (represented as JSON string).
     *
     * Special detection is done for integers and MVA:
     * - on 32 bit platforms integer $value is casted to Sphinx unsigned 32 bit integer and so array elements
     * - on 64 bit platforms integer $value is casted to Sphinx signed 64 bit integer and so array elements
     *
     * @param mixed $value Value to quote
     * @param null|string $type Type to cast $value to
     * @return string
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    public function quoteValue($value, $type = null)
    {
        if ($type === null) {
            if (is_int($value)) {
                $type = (PHP_INT_SIZE == 4 ? SphinxQL::TYPE_UINT : SphinxQL::TYPE_BIGINT);
            } else if (is_float($value)) {
                $type = SphinxQL::TYPE_FLOAT;
            } else if (is_bool($value)) {
                $type = SphinxQL::TYPE_BOOL;
            } else if (is_string($value)) {
                $type = SphinxQL::TYPE_STRING;
            } else if (is_array($value)) {
                $type = (PHP_INT_SIZE == 4 ? SphinxQL::TYPE_MULTI : SphinxQL::TYPE_MULTI_64);
            } else if (is_object($value)) {
                $type = SphinxQL::TYPE_JSON;
            } else {
                throw new ConnectionException(sprintf(
                    'Cannot cast $value of the type "%s" to a suitable SphinxQL type',
                    gettype($value)
                ));
            }
        }

        switch ($type) {
            case SphinxQL::TYPE_UINT:
            case SphinxQL::TYPE_TIMESTAMP:
                return sprintf('%u', $value); // convert PHP signed integer to unsigned integer

            case SphinxQL::TYPE_BOOL:
                return ((int)$value == 0 ? '0' : '1'); // unsigned integer, 0 or 1

            case SphinxQL::TYPE_BIGINT:
                return sprintf('%d', $value);

            case SphinxQL::TYPE_FLOAT:
                return sprintf('%F', $value); // convert float to non-locale aware float

            case SphinxQL::TYPE_STRING:
                return $this->quoteString($value);

            case SphinxQL::TYPE_JSON:
                if (is_array($value) || is_object($value)) {
                    $value = json_encode($value); // encode to JSON string
                }

                return $this->quoteString($value);

            case SphinxQL::TYPE_MULTI:
                return $this->quoteArray($value, SphinxQL::TYPE_UINT);

            case SphinxQL::TYPE_MULTI_64:
                return $this->quoteArray($value, SphinxQL::TYPE_BIGINT);

            default:
                throw new \InvalidArgumentException(sprintf('Argument $type [%s] is not a valid SphinxQL type', $type));
        }
    }

    /**
     * Quote values inside array if necessary and return them as a comma-separated string inside parentheses
     *
     * If $type is specified is applies to all array elements. See {@see AbstractConnection::quote()} for details.
     *
     * @param array $values Values to quote
     * @param null|string $type Type to cast $value to
     * @return string
     */
    public function quoteArray(array $values, $type = null)
    {
        $result = array();

        foreach ($values as $value) {
            $result[] = $this->quoteValue($value, $type);
        }

        return '(' . implode(', ', $result) . ')';
    }

    /**
     * Place quotes around the $string and escape special characters
     *
     * @param mixed $value String to quote
     * @return string
     */
    abstract public function quoteString($value);

    /**
     * Execute SQL query and return fetched data
     *
     * @param string $query SQL query to execute
     * @return array
     */
    abstract public function fetchAll($query);

    /**
     * Execute SQL query and return a single row
     *
     * @param string $query SQL query to execute
     * @return array
     */
    abstract public function fetchOne($query);

    /**
     * Execute SQL query and return number of affected rows
     *
     * @param string $query SQL query to execute
     * @return int
     */
    abstract public function execute($query);

    /**
     * Set connection parameters
     *
     * @param array $connectionParameters Connection parameters
     * @return AbstractConnection
     */
    abstract protected function setConnectionParameters(array $connectionParameters);
}
