<?php

namespace Adapters\Sphinx\Query;

use Adapters\Sphinx\Connection\AbstractConnection;
use Adapters\Sphinx\Exception\QueryBuilderException;
use Adapters\Sphinx\Query\Expression\ExpressionInterface;

/**
 * Class Update is a SphinxQL UPDATE query builder
 *
 * UPDATE statement updates existing rows from an existing index. Both RT and disk indexes are supported.
 *
 * UPDATE index SET col1 = newval1 [, ...] WHERE where_condition [OPTION opt_name = opt_value [, ...]]
 *
 * @link        http://sphinxsearch.com/docs/current.html#sphinxql-update
 */
class Update extends AbstractQuery
{
    use WhereTrait;
    use OptionTrait;

    /**
     * Index name SphinxQL query part
     */
    const INDEX = 'index';

    /**
     * Values SphinxQL query part
     */
    const VALUES = 'values';

    /**
     * WHERE SphinxQL query part
     */
    const WHERE = 'where';

    /**
     * OPTION SphinxQL query part
     */
    const OPTION = 'option';

    /**
     * Index name
     *
     * @var string
     */
    protected $index = null;

    /**
     * Update values
     *
     * @var array
     */
    protected $values = array();

    /**
     * Constructor
     *
     * @param null|string $index Index name
     * @param null|array $values UPDATE values
     */
    public function __construct($index = null, array $values = null)
    {
        if (!is_null($index)) {
            $this->index($index);
        }

        if (!is_null($values)) {
            $this->values($values);
        }

        $this->resetWhere();
    }

    /**
     * Set index name to update
     *
     * @param string $index Index name
     * @return Update
     * @throws \InvalidArgumentException
     */
    public function index($index)
    {
        if (!is_string($index) || ($index = trim($index)) == '') {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $this->index = $index;
        return $this;
    }

    /**
     * Set values to update
     *
     * $values should be an associative array with key is a column name and value is a value to update to:
     *
     * array(
     *     'col1' => 1,
     *     'col2' => (1, 2, 3)
     * )
     *
     * @param array $values Values to update
     * @return Update
     */
    public function values(array $values)
    {
        $this->values = $values;
        return $this;
    }

    /**
     * Reset a single or several parts of the SphinxQL query
     *
     * $part is one or array of Delete constants: {@see Delete::INDEX} or {@see Delete::WHERE}
     *
     * @param string|array $part Query part (one of the class constants)
     * @return Update
     * @throws \InvalidArgumentException
     */
    public function reset($part = null)
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

            case self::VALUES:
                $this->values = array();
                break;

            case self::WHERE:
                $this->resetWhere();
                break;

            case self::OPTION:
                $this->resetOptions();
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
            throw new QueryBuilderException('No values specified');
        } else if (!$this->hasWhere()) {
            throw new QueryBuilderException('Multi updates are not supported, WHERE clause must be specified for UPDATE');
        }

        $values = array();

        foreach ($this->values as $column => $value) {
            if (!is_string($column) || ($column = trim($column)) == '') {
                throw new QueryBuilderException('Invalid column name, not a string');
            } else if ($value instanceof ExpressionInterface) {
                throw new QueryBuilderException('Expressions in UPDATE are not supported');
            }

            $values[] = sprintf('%s = %s', $column, $connection->quoteValue($value));
        }

        $query = sprintf('UPDATE %s SET %s WHERE %s', $connection->quoteIdentifier($this->index), implode(', ', $values), $this->processWhere($connection));

        if ($this->hasOptions()) {
            $query .= sprintf(' OPTION %s', $this->processOptions());
        }

        return $query;
    }

    /**
     * __clone() magic method override
     */
    public function __clone()
    {
        $this->where = clone $this->where;
    }
}
