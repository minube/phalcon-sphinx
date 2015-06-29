<?php

namespace Adapters\Sphinx\Query;

use Adapters\Sphinx\Exception\QueryBuilderException;
use Adapters\Sphinx\Connection\AbstractConnection;
use Adapters\Sphinx\Query\Expression\ExpressionInterface;

/**
 * Class Insert is a SphinxQL INSERT query builder
 *
 * INSERT statement is only supported for RT indexes. It inserts new rows (documents) into an existing index,
 * with the provided column values. ID column must be present in all cases. Rows with duplicate IDs
 * will not be overwritten by INSERT.
 *
 * INSERT INTO index [(column, ...)]
 *     VALUES (value, ...)
 *     [, (...)]
 *
 * @link        http://sphinxsearch.com/docs/current.html#sphinxql-insert
 */
class Insert extends AbstractQuery
{
    /**
     * Index name SphinxQL query part
     */
    const INDEX = 'index';

    /**
     * Columns SphinxQL query part
     */
    const COLUMNS = 'columns';

    /**
     * Values SphinxQL query part
     */
    const VALUES = 'values';

    /**
     * Index name
     *
     * @var string
     */
    protected $index = null;

    /**
     * List of columns
     *
     * @var array
     */
    protected $columns = null;

    /**
     * Values to insert
     *
     * @var array[]
     */
    protected $values = array();

    /**
     * Query template
     *
     * @var string
     */
    protected static $query = 'INSERT INTO %s VALUES %s';

    /**
     * Constructor
     *
     * @param null|string $index Index name
     * @param null|array $columns List of columns
     */
    public function __construct($index = null, array $columns = null)
    {
        if (!is_null($index)) {
            $this->into($index);
        }

        if (!is_null($columns)) {
            $this->columns($columns);
        }
    }

    /**
     * Set index name to insert to
     *
     * @param string $index Index name
     * @return Insert
     * @throws \InvalidArgumentException
     */
    public function into($index)
    {
        if (!is_string($index) || ($index = trim($index)) == '') {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $this->index = $index;
        return $this;
    }

    /**
     * Set list of columns
     *
     * @param string|array $columns List of columns
     * @return Insert
     */
    public function columns(array $columns)
    {
        $this->columns = $columns;
        return $this;
    }

    /**
     * Add $values to insert
     *
     * @param array $values Values to insert
     * @return Insert
     */
    public function values(array $values)
    {
        $this->values[] = $values;
        return $this;
    }

    /**
     * Reset a single or several parts of the SphinxQL query
     *
     * $part is one or array of Insert constants: {@see Insert::INDEX}, {@see Insert::COLUMNS} or {@see Insert::VALUES}
     *
     * @param string|array $part Query part (one of the class constants)
     * @return Insert
     * @throws \InvalidArgumentException
     */
    public function reset($part)
    {
        if (is_array($part)) {
            foreach ($part as $reset) {
                $this->reset($reset);
            }

            return $this;
        }

        switch ($part) {
            case self::INDEX:
                $this->index = null;
                break;

            case self::COLUMNS:
                $this->columns = null;
                break;

            case self::VALUES:
                $this->values = array();
                break;

            default:
                throw new \InvalidArgumentException(sprintf('Unknown SphinxQL query part to reset "%s"', $part));
        }

        return $this;
    }

    /**
     * Compile SphinxQL query and return it as a string
     *
     * @param AbstractConnection $connection Connection
     * @return string
     * @throws QueryBuilderException
     */
    public function getQueryString(AbstractConnection $connection)
    {
        if ($this->index === null) {
            throw new QueryBuilderException('Index name is required');
        } else if (sizeof($this->values) == 0) {
            throw new QueryBuilderException('No values to insert');
        }

        $index = $connection->quoteIdentifier($this->index);

        if (($columnsCount = sizeof($this->columns)) > 0) {
            $columns = array();

            foreach ($this->columns as $column) {
                if (!is_string($column) || ($column = trim($column)) == '') {
                    throw new QueryBuilderException('Column name must be a string');
                }

                $columns[] = $column;
            }

            $index = $index . ' (' . $connection->quoteIdentifierArray($columns) . ')';
        }

        $values = array();

        for ($i = 0, $n = sizeof($this->values); $i < $n; $i++) {
            if ($columnsCount > 0 && $columnsCount != sizeof($this->values[$i])) {
                throw new QueryBuilderException('Number of columns and values don\'t match');
            }

            foreach ($this->values[$i] as $value) {
                if ($value instanceof ExpressionInterface) {
                    throw new QueryBuilderException('Expressions in INSERT / REPLACE are not supported');
                }

                $values[] = $connection->quoteValue($value);
            }
        }

        return sprintf(static::$query, $index, implode(', ', $values));
    }
}
