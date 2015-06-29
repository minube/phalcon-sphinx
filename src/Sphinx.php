<?php

namespace Adapters;

use Adapters\Sphinx\Exception\ConnectionException;
use Adapters\Sphinx\Connection\AbstractConnection;
use Adapters\Sphinx\Connection\PdoMysql;
use Adapters\Sphinx\Connection\Mysqli;
use Adapters\Sphinx\Query\AbstractQuery;
use Adapters\Sphinx\Query\Expression\Literal;
use Adapters\Sphinx\Query\Expression\Expression;
use Adapters\Sphinx\Query\Select;
use Adapters\Sphinx\Query\Insert;
use Adapters\Sphinx\Query\Replace;
use Adapters\Sphinx\Query\Update;
use Adapters\Sphinx\Query\Delete;

/**
 * Class SphinxQL allows to build and perform queries on SphinxQL dialect
 *
 * SphinxQL class supports both Mysqli and PDO MySQL drivers.
 *
 * This library implements SphinxQL dialect for Sphinx and supports new features introduced in 2.2.1-beta.
 * Some queries are not supported by earlier Sphinx versions. Refer to the documentation for installed Sphinx
 * version if not sure {@link http://sphinxsearch.com/docs/}.
 *
 * Note for 32 bit platforms: Sphinx BIGINT is not supported and attempt to use it may lead to unexpected results.
 * When passing value to {@see AbstractConnection::quoteValue()} without specifying type, integers and MVA are quoted
 * differently on 32 bit and 64 bit platforms. On 32 bit platforms integers are treated as 32 bit unsigned integers and
 * on 64 bit platforms they are treated as 64 bit signed integers.
 */
class Sphinx
{
    /**
     * Defines global scope, when setting and / or retrieving variables
     */
    const SCOPE_GLOBAL = 'GLOBAL';

    /**
     * Defines session scope, when setting and / or retrieving variables
     */
    const SCOPE_SESSION = 'SESSION';

    /**
     * Sphinx unsigned 32 bit integer
     */
    const TYPE_UINT = 'uint';

    /**
     * Sphinx boolean (represented with 8 bit unsigned integer)
     */
    const TYPE_BOOL = 'bool';

    /**
     * Sphinx timestamp
     */
    const TYPE_TIMESTAMP = 'timestamp';

    /**
     * Sphinx BIGINT, 64 bit signed integer
     */
    const TYPE_BIGINT = 'bigint';

    /**
     * Sphinx float
     */
    const TYPE_FLOAT = 'float';

    /**
     * Sphinx string
     */
    const TYPE_STRING = 'string';

    /**
     * Sphinx JSON
     */
    const TYPE_JSON = 'json';

    /**
     * Sphinx MVA (array of unsigned 32 bit integers)
     */
    const TYPE_MULTI = 'mva';

    /**
     * Sphinx MVA 64 (array of signed 64 bit integers)
     */
    const TYPE_MULTI_64 = 'mva64';

    /**
     * Connection object
     *
     * @var AbstractConnection
     */
    protected $connection = null;

    /**
     * Constructor
     *
     * @param AbstractConnection $connection Connection object
     */
    public function __construct(AbstractConnection $connection)
    {
        $this->setConnection($connection);
    }

    /**
     * Set connection object
     *
     * @param AbstractConnection $connection Connection object
     * @return SphinxQL
     */
    public function setConnection(AbstractConnection $connection)
    {
        $this->connection = $connection;
        return $this;
    }

    /**
     * Return connection object
     *
     * @return AbstractConnection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * SphinxQL factory helper method
     *
     * This method creates connection object using specified $driver and $connectionParameters, and then it creates
     * and returns SphinxQL instance.
     *
     * $driver can be either NULL, {@see PdoMysql::DRIVER_NAME 'pdo_mysql'} or {@see Mysqli::DRIVER_NAME 'mysqli'}.
     * For the list of $connectionParameters see {@see PdoMysql::setConnectionParameters()} and / or
     * {@see Mysqli::setConnectionParameters()}.
     *
     * If $driver is NULL then this method will attempt to create {@see PdoMysql} connection object first, if 'pdo_mysql'
     * extension is available. Otherwise, it will attempt to create {@see Mysqli} connection object.
     *
     * If $driver is NULL then $connectionParameters is stripped to the safest list of parameters compatible with both drivers.
     * They are: 'host', 'port', 'unix_socket' and 'dsn' (if detected driver is {@see PdoMysql} only). 'options' parameter
     * is stripped explicitly.
     *
     * @param string|null $driver Driver name
     * @param array $connectionParameters Connection parameters
     * @return SphinxQL
     * @throws ConnectionException
     * @throws \InvalidArgumentException
     */
    public static function factory($driver = null, array $connectionParameters = array())
    {
        if ($driver === null) {
            $filter = array('host', 'port', 'unix_socket');

            /**
             * 'pdo_mysql' is preferred driver and picked up first if available. 'mysqli' is the "second chance" driver.
             */

            if ( extension_loaded(PdoMysql::DRIVER_NAME) ) {
                $driver = PdoMysql::DRIVER_NAME;
                $filter[] = 'dsn'; // keep 'dsn' string
            } else if ( extension_loaded(Mysqli::DRIVER_NAME) ) {
                $driver = Mysqli::DRIVER_NAME;
            } else {
                throw new ConnectionException(sprintf(
                    'Cannot find suitable database driver: neither "%s" nor "%s" is available',
                    PdoMysql::DRIVER_NAME,
                    Mysqli::DRIVER_NAME
                ));
            }

            /**
             * 'dsn' is not compatible with \mysqli and driver 'options' are different for each driver.
             * Only 'host', 'port' and 'unix_socket' are accepted by all drivers in $connectionParameters.
             */

            $options = array();

            foreach($connectionParameters as $key => $value) {
                $key = strtolower($key);

                if ( in_array($key, $filter) ) {
                    $options[$key] = $value;
                }
            }

            $connectionParameters = $options;
            unset($options, $filter);
        } else if ( !is_string($driver) ) {
            throw new \InvalidArgumentException('Argument $driver must be a string or NULL');
        }

        switch ( strtolower($driver) ) {
            case PdoMysql::DRIVER_NAME:
                $connection = new PdoMysql($connectionParameters);
                break;

            case Mysqli::DRIVER_NAME:
                $connection = new Mysqli($connectionParameters);
                break;

            default:
                throw new ConnectionException( sprintf('Unsupported driver "%s"', $driver) );
        }

        return new static($connection);
    }

    /**
     * ==========================================================
     *     Transaction management
     * ==========================================================
     */

    /**
     * Begin transaction
     *
     * @return SphinxQL
     */
    public function beginTransaction()
    {
        $this->connection->beginTransaction();
        return $this;
    }

    /**
     * Commit transaction
     *
     * @return SphinxQL
     */
    public function commit()
    {
        $this->connection->commit();
        return $this;
    }

    /**
     * Rollback transaction
     *
     * @return SphinxQL
     */
    public function rollback()
    {
        $this->connection->rollback();
        return $this;
    }

    /**
     * ==========================================================
     *     Information queries
     * ==========================================================
     */

    /**
     * Show additional meta-information about the latest query such as query time and keyword statistics
     *
     * SHOW META [ LIKE pattern ]
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-show-meta
     *
     * @param null|string $pattern Variable name match pattern
     * @return array
     * @throws \InvalidArgumentException
     */
    public function showMeta($pattern = null)
    {
        $query = 'SHOW META';

        if ($pattern !== null) {
            if ( !is_string($pattern) ) {
                throw new \InvalidArgumentException('Argument $pattern must be a string or NULL');
            } else if (( $pattern = trim($pattern) ) != '') {
                $query.= sprintf(' LIKE %s', $this->connection->quoteString($pattern) );
            }
        }

        return $this->arrayFlatten( $this->connection->fetchAll($query), 'Variable_name', 'Value' );
    }

    /**
     * Show performance counters
     *
     * SHOW STATUS [ LIKE pattern ]
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-show-status
     *
     * @param null|string $pattern Variable name match pattern
     * @return array
     * @throws \InvalidArgumentException
     */
    public function showStatus($pattern = null)
    {
        $query = 'SHOW STATUS';

        if ($pattern !== null) {
            if ( !is_string($pattern) ) {
                throw new \InvalidArgumentException('Argument $pattern must be a string or NULL');
            } else if (( $pattern = trim($pattern) ) != '') {
                $query.= sprintf(' LIKE %s', $this->connection->quoteString($pattern) );
            }
        }

        $result = $this->connection->fetchAll($query);

        /**
         * Sphinx 2.2.1-beta returns first column named as 'Counter'. Earlier Sphinx versions return
         * it named as 'Variable_name'.
         */

        if ( sizeof($result) == 0 ) {
            return $result;
        }

        return $this->arrayFlatten($result, ( array_key_exists('Counter', $result[0]) ? 'Counter' : 'Variable_name' ), 'Value');
    }

    /**
     * Show list of all currently active indexes along with their types
     *
     * SHOW TABLES [ LIKE pattern ]
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-show-tables
     *
     * @param null|string $pattern Table name match pattern
     * @return array
     * @throws \InvalidArgumentException
     */
    public function showTables($pattern = null)
    {
        $query = 'SHOW TABLES';

        if ($pattern !== null) {
            if ( !is_string($pattern) ) {
                throw new \InvalidArgumentException('Argument $pattern must be a string or NULL');
            } else if (( $pattern = trim($pattern) ) != '') {
                $query.= sprintf(' LIKE %s', $this->connection->quoteString($pattern) );
            }
        }

        return $this->arrayFlatten( $this->connection->fetchAll($query), 'Index', 'Type' );
    }

    /**
     * Show list of all currently active indexes along with their types
     *
     * {DESC | DESCRIBE} index [ LIKE pattern ]
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-describe
     *
     * @param string      $index   Index name
     * @param null|string $pattern Field name match pattern
     * @return array
     * @throws \InvalidArgumentException
     */
    public function describeIndex($index, $pattern = null)
    {
        if ( !is_string($index) || ( $index = trim($index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $query = sprintf('DESCRIBE %s', $this->connection->quoteIdentifier($index));

        if ($pattern !== null) {
            if ( !is_string($pattern) ) {
                throw new \InvalidArgumentException('Argument $pattern must be a string or NULL');
            } else if (( $pattern = trim($pattern) ) != '') {
                $query.= sprintf(' LIKE %s', $this->connection->quoteString($pattern) );
            }
        }

        return $this->arrayFlatten( $this->connection->fetchAll($query), 'Field', 'Type' );
    }

    /**
     * Show the statistic of remote agents or distributed index
     *
     * SHOW AGENT ['agent'|'index'|index] STATUS [ LIKE pattern ]
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-show-agent-status
     *
     * @param string      $agent   Agent name or IP address
     * @param null|string $pattern Key name match pattern
     * @return array
     * @throws \InvalidArgumentException
     */
    public function showAgentStatus($agent = null, $pattern = null)
    {
        $query = 'SHOW AGENT ';

        if ($agent !== null) {
            if ( !is_string($agent) || ( $agent = trim($agent) ) == '' ) {
                throw new \InvalidArgumentException('Argument $agent name must be a string');
            }

            $query.= $this->connection->quoteString($agent) . ' ';
        }

        $query.= 'STATUS';

        if ($pattern !== null) {
            if ( !is_string($pattern) ) {
                throw new \InvalidArgumentException('Argument $pattern must be a string or NULL');
            } else if (( $pattern = trim($pattern) ) != '') {
                $query.= sprintf(' LIKE %s', $this->connection->quoteString($pattern) );
            }
        }

        $result = $this->connection->fetchAll($query);

        /**
         * Sphinx 2.2.1-beta returns first column named as 'Variable_name'. Earlier Sphinx versions
         * return it named as 'Key'.
         */

        if ( sizeof($result) == 0 ) {
            return $result;
        }

        return $this->arrayFlatten($result, ( array_key_exists('Variable_name', $result[0]) ? 'Variable_name' : 'Key' ), 'Value');
    }

    /**
     * Returns the current values of server-wide variables
     *
     * SHOW [{GLOBAL | SESSION}] VARIABLES [WHERE variable_name='xxx'], WHERE condition is added for compatibility
     * with 3rd party connectors and does not have any effect.
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-show-variables
     *
     * @param string $scope Scope clause: {@see SphinxQL::SCOPE_SESSION} or {@see SphinxQL::SCOPE_GLOBAL}
     * @return array
     * @throws \InvalidArgumentException
     */
    public function showVariables($scope = self::SCOPE_SESSION)
    {
        if ( !is_string($scope) || ($scope != self::SCOPE_SESSION && $scope != self::SCOPE_GLOBAL) ) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid scope: must be either %1$s\\SphinxQL::SCOPE_SESSION or %1$s\\SphinxQL::SCOPE_GLOBAL',
                __NAMESPACE__
            ));
        }

        $query = sprintf('SHOW %s VARIABLES', $scope);
        return $this->arrayFlatten( $this->connection->fetchAll($query), 'Variable_name', 'Value' );
    }

    /**
     * SHOW INDEX index_name STATUS
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-show-index-status
     *
     * @param string $index Index name
     * @return array
     * @throws \InvalidArgumentException
     */
    public function showIndexStatus($index)
    {
        if ( !is_string($index) || ( $index = trim($index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $query = sprintf('SHOW INDEX %s STATUS', $this->connection->quoteIdentifier($index));
        return $this->arrayFlatten( $this->connection->fetchAll($query), 'Variable_name', 'Value' );
    }

    /**
     * Show a detailed execution profile of the previous SQL statement executed in the current SphinxQL session
     *
     * SHOW PROFILE
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-show-profile
     *
     * @return array[]
     */
    public function showProfile()
    {
        return $this->connection->fetchAll('SHOW PROFILE');
    }

    /**
     * ==========================================================
     *     RT index functions
     * ==========================================================
     */

    /**
     * Move data from a regular disk index to a RT index
     *
     * ATTACH INDEX diskindex TO RTINDEX rtindex
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-attach-index
     *
     * @param string $disk_index Source disk index
     * @param string $rt_index   Target RT index
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function attachIndex($disk_index, $rt_index)
    {
        if ( !is_string($disk_index) || ( $disk_index = trim($disk_index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $disk_index must be a string');
        } else if ( !is_string($rt_index) || ( $rt_index = trim($rt_index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $rt_index must be a string');
        }

        $this->connection->execute(sprintf(
            'ATTACH INDEX %s TO RTINDEX %s',
            $this->connection->quoteIdentifier($disk_index),
            $this->connection->quoteIdentifier($rt_index)
        ));

        return $this;
    }

    /**
     * Flush RT index RAM chunk contents to disk
     *
     * FLUSH RTINDEX rtindex
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-flush-rtindex
     *
     * @param string $rt_index Index name
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function flushIndex($rt_index)
    {
        if ( !is_string($rt_index) || ( $rt_index = trim($rt_index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $this->connection->execute( sprintf('FLUSH RTINDEX %s', $this->connection->quoteIdentifier($rt_index)) );
        return $this;
    }

    /**
     * Create a new disk chunk in a RT index
     *
     * FLUSH RAMCHUNK rtindex
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-flush-ramchunk
     *
     * @param string $index Index name
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function flushRamChunk($index)
    {
        if ( !is_string($index) || ( $index = trim($index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $this->connection->execute( sprintf('FLUSH RAMCHUNK %s', $this->connection->quoteIdentifier($index)) );
        return $this;
    }

    /**
     * Clear the RT index completely
     *
     * TRUNCATE RTINDEX rtindex
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-truncate-rtindex
     *
     * @param string $rt_index Index name
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function truncateIndex($rt_index)
    {
        if ( !is_string($rt_index) || ( $rt_index = trim($rt_index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $this->connection->execute( sprintf('TRUNCATE RTINDEX %s', $this->connection->quoteIdentifier($rt_index)) );
        return $this;
    }

    /**
     * Optimize RT index
     *
     * OPTIMIZE INDEX index_name
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-optimize-index
     *
     * @param string $rt_index Index name
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function optimizeIndex($rt_index)
    {
        if ( !is_string($rt_index) || ( $rt_index = trim($rt_index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $this->connection->execute( sprintf('OPTIMIZE INDEX %s', $this->connection->quoteIdentifier($rt_index)) );
        return $this;
    }

    /**
     * Add an attribute for both plain and RT indexes
     *
     * ALTER TABLE index ADD COLUMN new_column {INTEGER|BIGINT|FLOAT|BOOL}
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-attach
     *
     * New attributes are specified in $fields array, where 'attribute' is the array key and value is the type:
     *
     * array(
     *     'attr1' => SphinxQL::TYPE_UINT,
     *     'attr2' => SphinxQL::TYPE_BOOL
     * )
     *
     * Field type must be one of the SphinxQL::TYPE_* constants: {@see SphinxQL::TYPE_UINT}, {@see SphinxQL::TYPE_BIGINT},
     * {@see SphinxQL::TYPE_FLOAT}, {@see SphinxQL::TYPE_BOOL}.
     *
     * Note: BIGINT support is limited and depends on environment, PHP version and architecture.
     *
     * @param string $index  Index name
     * @param array  $fields New 'attribute' => 'type' pairs
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function alterTable($index, array $fields)
    {
        if ( !is_string($index) || ( $index = trim($index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $index = $this->connection->quoteIdentifier($index);

        $attributeTypes = array(
            self::TYPE_UINT     => 'INTEGER',
            self::TYPE_BIGINT   => 'BIGINT',
            self::TYPE_FLOAT    => 'FLOAT',
            self::TYPE_BOOL     => 'BOOL'
        );

        foreach($fields as $attribute => $type) {
            if (( $attribute = trim($attribute) ) == '') {
                throw new \InvalidArgumentException('Invalid attribute name, must be a string');
            } else if ( !array_key_exists($type, $attributeTypes) ) {
                throw new \InvalidArgumentException( sprintf('Invalid attribute type [%s]', $type) );
            }

            $this->connection->execute(sprintf(
                'ALTER TABLE %s ADD COLUMN %s %s',
                $index,
                $this->connection->quoteIdentifier($attribute),
                $attributeTypes[$type]
            ));
        }

        return $this;
    }

    /**
     * ==========================================================
     *     Queries
     * ==========================================================
     */

    /**
     * Create and return {@see Select} query builder
     *
     * @param null|string|array $index Index name or list of indexes as an array
     * @param null|string|array $columns Columns to select
     * @return Select
     */
    public function select($index = null, $columns = Select::STAR)
    {
        return new Select($index, $columns);
    }

    /**
     * Create and return {@see Insert} query builder
     *
     * @param null|string $index   Index name
     * @param null|array  $columns List of columns
     * @return Insert
     */
    public function insert($index = null, array $columns = null)
    {
        return new Insert($index, $columns);
    }

    /**
     * Create and return {@see Replace} query builder
     *
     * @param null|string $index   Index name
     * @param null|array  $columns List of columns
     * @return Replace
     */
    public function replace($index = null, array $columns = null)
    {
        return new Replace($index, $columns);
    }

    /**
     * Create and return {@see Delete} query builder
     *
     * @param null|string $index Index name
     * @return Delete
     */
    public function delete($index = null)
    {
        return new Delete($index);
    }

    /**
     * Create and return {@see Update} query builder
     *
     * @param null|string $index  Index name
     * @param null|array  $values UPDATE values
     * @return Update
     */
    public function update($index = null, array $values = null)
    {
        return new Update($index, $values);
    }

    /**
     * Create a new literal expression
     *
     * @param string $expression Expression
     * @return Literal
     */
    public function literalExpression($expression)
    {
        return new Literal($expression);
    }

    /**
     * Create a new value expression
     *
     * @param mixed $value Value
     * @param int   $type  One of the SphinxQL::TYPE_* constants
     * @return Literal
     */
    public function valueExpression($value, $type)
    {
        return new Literal( $this->getConnection()->quoteValue($value, $type) );
    }

    /**
     * Create a new expression
     *
     * @see Expression::__construct()
     * @param string     $expression Expression
     * @param array      $parameters Expression parameters
     * @param null|array $types      Parameters types, either {@see ExpressionInterface::VALUE} or {@see ExpressionInterface::IDENTIFIER}
     * @param null|array $dataTypes  Parameters data types, list of SphinxQL::TYPE_* constants
     * @return Expression
     */
    public function expression($expression, array $parameters, array $types = null, array $dataTypes = null)
    {
        return new Expression($expression, $parameters, $types, $dataTypes);
    }

    /**
     * Quote full-text search query
     *
     * @param string $query Full-text search query
     * @return string
     */
    public function quoteMatch($query)
    {
        return str_replace(
            array('\\', '(',')','|','-','!','@','~','"','&', '/', '^', '$', '='),
            array('\\\\', '\(','\)','\|','\-','\!','\@','\~','\"', '\&', '\/', '\^', '\$', '\='),
            $query
        );
    }

    /**
     * Execute a {@see Select} query and return a single column of the first row
     *
     * @param string|Select $query SphinxQL query
     * @return mixed
     */
    public function fetchCol($query)
    {
        $result = $this->fetchOne($query);
        return ( is_array($result) ? current($result) : $result );
    }

    /**
     * Execute a {@see Select} query and return a first row
     *
     * @param string|Select $query SphinxQL query
     * @return array
     * @throws \InvalidArgumentException
     */
    public function fetchOne($query)
    {
        $connection = $this->getConnection();

        if ($query instanceof AbstractQuery) {
            $query = $query->getQueryString($connection);
        } else if ( !is_string($query) ) {
            throw new \InvalidArgumentException(sprintf(
                'Argument $query must be a string or an instance of %s\\Query\\AbstractQuery',
                __NAMESPACE__
            ));
        }

        return $connection->fetchOne($query);
    }

    /**
     * Execute a {@see Select} query and return all fetched rows
     *
     * @param string|Select $query SphinxQL query
     * @return array[]
     * @throws \InvalidArgumentException
     */
    public function fetchAll($query)
    {
        $connection = $this->getConnection();

        if ($query instanceof AbstractQuery) {
            $query = $query->getQueryString($connection);
        } else if ( !is_string($query) ) {
            throw new \InvalidArgumentException(sprintf(
                'Argument $query must be a string or an instance of %s\\Query\\AbstractQuery',
                __NAMESPACE__
            ));
        }

        return $connection->fetchAll($query);
    }

    /**
     * Execute query
     *
     * If $query is one of the {@see Insert}, {@see Replace}, {@see Update} or {@see Delete} then the method returns
     * a number of affected rows.
     *
     * @param string|Insert|Replace|Update|Delete $query SphinxQL query
     * @return int
     * @throws \InvalidArgumentException
     */
    public function execute($query)
    {
        $connection = $this->getConnection();

        if ($query instanceof AbstractQuery) {
            $query = $query->getQueryString($connection);
        } else if ( !is_string($query) ) {
            throw new \InvalidArgumentException(sprintf(
                'Argument $query must be a string or an instance of %s\\Query\\AbstractQuery',
                __NAMESPACE__
            ));
        }

        return $connection->execute($query);
    }

    /**
     * ==========================================================
     *     Misc functions
     * ==========================================================
     */

    /**
     * Set or modify a variable value
     *
     * SET [GLOBAL] server_variable_name = value
     * SET GLOBAL @user_variable_name = (int_val1 [, int_val2, ...])     *
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-set
     *
     * @param string $name  Variable name
     * @param mixed  $value Variable value
     * @param string $scope Scope clause: {@see SphinxQL::SCOPE_SESSION} or {@see SphinxQL::SCOPE_GLOBAL}
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function setVariable($name, $value, $scope = self::SCOPE_SESSION)
    {
        if ( !is_string($name) || ( $name = trim($name) ) == '' ) {
            throw new \InvalidArgumentException('Argument $name must be a string');
        } else if ( !is_string($scope) || ($scope != self::SCOPE_SESSION && $scope != self::SCOPE_GLOBAL) ) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid $scope: must be either %1$s\\SphinxQL::SCOPE_SESSION or %1$s\\SphinxQL::SCOPE_GLOBAL',
                __NAMESPACE__
            ));
        }

        if ( substr($name, 0, 1) == '@' ) {
            $value = $this->connection->quoteValue( (array)$value, self::TYPE_MULTI_64 ); // MVA_64 are only allowed in global user vars
        } else {
            $value = $this->connection->quoteValue($value);
        }

        $query = 'SET ';

        if ($scope == self::SCOPE_GLOBAL) {
            $query.= 'GLOBAL ';
        }

        $query.= '%s = %s';

        $this->connection->execute( sprintf($query, $this->connection->quoteIdentifier($name), $value) );
        return $this;
    }

    /**
     * Set or modify several variables
     *
     * Variable name is passed as key in $variables and value as key value:
     *
     * $variables = array(
     *     'autocommit' => true
     * );
     *
     * @see SphinxQL::setVariable()
     *
     * @param array  $variables Variables
     * @param string $scope     Scope clause: {@see SphinxQL::SCOPE_SESSION} or {@see SphinxQL::SCOPE_GLOBAL}
     * @return SphinxQL
     */
    public function setVariables(array $variables, $scope = self::SCOPE_SESSION)
    {
        foreach($variables as $name => $value) {
            $this->setVariable($name, $value, $scope);
        }

        return $this;
    }

    /**
     * Build a snippet from provided data and query, using specified index settings
     *
     * $options is a hash of additional parameters as an associative array:
     *
     * $options = array(
     *     'around' => 5,
     *     'limit' => 200
     * );
     *
     * be careful with $options values types. If, for example, 'word_limit' values specified as string and Sphinx expects
     * an integer, query will fail.
     *
     * CALL SNIPPETS(data, index, query[, opt_value AS opt_name[, ...]])
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-call-snippets
     * @link http://sphinxsearch.com/docs/current.html#api-func-buildexcerpts
     *
     * @param string|array $data    Source data to extract a snippet from
     * @param string       $index   Index name
     * @param string       $query   Full-text query to build snippets for
     * @param array        $options Additional optional highlighting parameters
     * @return array[]
     * @throws \InvalidArgumentException
     */
    public function callSnippets($data, $index, $query, array $options = array())
    {
        if ( !is_string($index) || ( $index = trim($index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $sql = sprintf(
            'CALL SNIPPETS(%s, %s, %s',
            $this->connection->quoteArray( (array)$data, self::TYPE_STRING ),
            $this->connection->quoteString($index),
            $this->connection->quoteString($query)
        );

        if ( sizeof($options) > 0 ) {
            $options_list = array();

            foreach($options as $option => $value) {
                $options_list[] = sprintf('%s AS %s', $this->connection->quoteValue($value), $this->connection->quoteIdentifier($option));
            }

            $sql.= sprintf(', %s', implode(', ', $options_list));
        }

        return $this->connection->fetchAll($sql . ')');
    }

    /**
     * Split text into particular keywords and return tokenized and normalized forms of the keywords, and, optionally,
     * keyword statistics.
     *
     * CALL KEYWORDS(text, index, [hits])
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-call-keywords
     *
     * @param string $text  Text to split into keywords
     * @param string $index Index name
     * @param bool   $hits  Indicates whether to return a document and hit occurrence statistics
     * @return array[]
     * @throws \InvalidArgumentException
     */
    public function callKeywords($text, $index, $hits = false)
    {
        if ( !is_string($index) || ( $index = trim($index) ) == '' ) {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        return $this->connection->fetchAll(sprintf(
            'CALL KEYWORDS(%s, %s, %s)',
            $this->connection->quoteString($text),
            $this->connection->quoteString($index),
            $this->connection->quoteValue($hits, self::TYPE_BOOL)
        ));
    }

    /**
     * Install a user-defined function (UDF) with the given name and type from the given library file
     *
     * CREATE FUNCTION udf_name
     *    RETURNS {INT | BIGINT | FLOAT | STRING}
     *    SONAME 'udf_lib_file'
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-create-function
     *
     * Function return type must be one of the SphinxQL::TYPE_* constants: {@see SphinxQL::TYPE_UINT},
     * {@see SphinxQL::TYPE_BIGINT}, {@see SphinxQL::TYPE_FLOAT}, {@see SphinxQL::TYPE_STRING}.
     *
     * @param string $udf_name    Function name
     * @param string $return_type Function return type
     * @param string $soname      Library name
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function createFunction($udf_name, $return_type, $soname)
    {
        $returnTypes = array(
            self::TYPE_UINT     => 'INT',
            self::TYPE_BIGINT   => 'BIGINT',
            self::TYPE_FLOAT    => 'FLOAT',
            self::TYPE_STRING   => 'STRING'
        );

        if ( !is_string($udf_name) || ( $udf_name = trim($udf_name) ) == '' ) {
            throw new \InvalidArgumentException('Argument $udf_name must be a string');
        } else if ( !in_array($return_type, $returnTypes) ) {
            throw new \InvalidArgumentException( sprintf('Invalid function return type [%s]', $return_type) );
        }

        $this->connection->execute(sprintf(
            'CREATE FUNCTION %s RETURNS %s SONAME %s',
            $this->connection->quoteIdentifier($udf_name),
            $returnTypes[$return_type],
            $this->connection->quoteString($soname)
        ));

        return $this;
    }

    /**
     * Drop a user-defined function (UDF) with the given name
     *
     * DROP FUNCTION udf_name
     *
     * @link http://sphinxsearch.com/docs/current.html#sphinxql-drop-function
     *
     * @param string $udf_name Function name
     * @return SphinxQL
     * @throws \InvalidArgumentException
     */
    public function dropFunction($udf_name)
    {
        if ( !is_string($udf_name) || ( $udf_name = trim($udf_name) ) == '' ) {
            throw new \InvalidArgumentException('Argument $udf_name must be a string');
        }

        $this->connection->execute( sprintf('DROP FUNCTION %s', $this->connection->quoteIdentifier($udf_name)) );
        return $this;
    }

    /**
     * ==========================================================
     *     Internal
     * ==========================================================
     */

    /**
     * Flattens array of arrays into a single array indexed by the $keyColumn and with values from the $valueColumn
     *
     * @param array[] $input       Input array
     * @param string  $keyColumn   Nested array 'key' key
     * @param string  $valueColumn Nested array 'value' key
     * @return array
     * @throws \InvalidArgumentException
     */
    protected function arrayFlatten(array $input, $keyColumn, $valueColumn)
    {
        $result = array();

        foreach($input as $value) {
            if ( !is_array($value) ) {
                throw new \InvalidArgumentException('Argument $input must be an array of arrays');
            } else if ( !array_key_exists($keyColumn, $value) || !array_key_exists($valueColumn, $value) ) {
                throw new \InvalidArgumentException(sprintf(
                    '$keyColumn "%s" or $valueColumn "%s" is not present in the nested array',
                    $keyColumn,
                    $valueColumn
                ));
            }

            $result[$value[$keyColumn]] = $value[$valueColumn];
        }

        return $result;
    }
}
