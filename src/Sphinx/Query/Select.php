<?php

namespace Adapters\Sphinx\Query;

use Adapters\Sphinx\Connection\AbstractConnection;
use Adapters\Sphinx\Exception\QueryBuilderException;
use Adapters\Sphinx\Query\Expression\ExpressionInterface;

/**
 * Class Select is a SphinxQL SELECT query builder
 *
 * SELECT
 *    select_expr [, select_expr ...]
 *    FROM index [, index2 ...]
 *    [WHERE where_condition]
 *    [GROUP BY {col_name | expr_alias} [, {sol_name | expr_alias}]]
 *    [WITHIN GROUP ORDER BY {col_name | expr_alias} {ASC | DESC}]
 *    [HAVING having_condition]
 *    [ORDER [N] BY {col_name | expr_alias} {ASC | DESC} [, ...]]
 *    [LIMIT [offset,] row_count]
 *    [OPTION opt_name = opt_value [, ...]]
 *
 * @link        http://sphinxsearch.com/docs/current.html#sphinxql-select
 * @author      Roman Kulish <roman.kulish@gmail.com>
 * @since       0.2.0-dev
 */
class Select extends AbstractQuery
{
    use WhereTrait;
    use OptionTrait;

    /**
     * Star, select "all" columns
     */
    const STAR = '*';

    /**
     * Order direction, ascending
     */
    const ORDER_ASC = 'ASC';

    /**
     * Order direction, descending
     */
    const ORDER_DESC = 'DESC';

    /**
     * Columns SphinxQL query part
     */
    const COLUMNS = 'columns';

    /**
     * Index SphinxQL query part
     */
    const INDEX = 'index';

    /**
     * WHERE SphinxQL query part
     */
    const WHERE = 'where';

    /**
     * GROUP BY SphinxQL query part
     */
    const GROUP_BY = 'group by';

    /**
     * WITHIN GROUP ORDER BY SphinxQL query part
     */
    const WITHIN_GROUP_ORDER_BY = 'within group order by';

    /**
     * HAVING SphinxQL query part
     */
    const HAVING = 'having';

    /**
     * ORDER BY SphinxQL query part
     */
    const ORDER_BY = 'order by';

    /**
     * LIMIT (offset) SphinxQL query part
     */
    const OFFSET = 'offset';

    /**
     * LIMIT (row count) SphinxQL query part
     */
    const LIMIT = 'limit';

    /**
     * OPTION SphinxQL query part
     */
    const OPTION = 'option';

    /**
     * Columns to select
     *
     * @var array
     */
    protected $columns = array(self::STAR);

    /**
     * List of indexes
     *
     * @var array
     */
    protected $indexes = null;

    /**
     * Columns or expressions aliases to group by
     *
     * @var array
     */
    protected $group = array();

    /**
     * WITHIN GROUP ORDER BY column or an expression alias
     *
     * @var array|null
     */
    protected $withinGroupOrder = null;

    /**
     * HAVING clause
     *
     * @var null|ExpressionInterface
     */
    protected $having = null;

    /**
     * Columns or expressions aliases to order by
     *
     * @var array
     */
    protected $order = array();

    /**
     * Rows offset
     *
     * @var int|null
     */
    protected $offset = null;

    /**
     * Rows limit
     *
     * @var int|null
     */
    protected $limit = null;

    /**
     * Create and return {@see Select} query builder
     *
     * @param null|string|array $index Index name or list of indexes as an array
     * @param null|string|array $columns Columns to select
     * @return Select
     */
    public function __construct($index = null, $columns = self::STAR)
    {
        if (!is_null($index)) {
            $this->from($index);
        }

        $this->columns($columns);
        $this->resetWhere();
    }

    /**
     * Set list of columns to select
     *
     * @param string|array $columns Columns to select
     * @return Select
     */
    public function columns($columns = self::STAR)
    {
        $this->columns = (array)$columns;
        return $this;
    }

    /**
     * Set index (or list of indexes as array) to select from
     *
     * @param string|Select|array $index Index or list of indexes as array
     * @return Select
     */
    public function from($index)
    {
        $this->indexes = (array)$index;
        return $this;
    }

    /**
     * Add columns or expressions aliases to group by
     *
     * @param string|array $column Columns or expressions
     * @return Select
     * @throws \InvalidArgumentException
     */
    public function group($column)
    {
        foreach ((array)$column as $name) {
            if (!is_string($name) || ($name = trim($name)) == '') {
                throw new \InvalidArgumentException('Invalid group column name or expression alias, must be a string');
            }

            $this->group[] = $name;
        }

        return $this;
    }

    /**
     * Set a column or an expression within the group that will be used to order by
     *
     * @param string $column Column or expression to order by
     * @param string $direction Order
     * @return Select
     * @throws \InvalidArgumentException
     */
    public function withinGroupOrder($column, $direction = self::ORDER_ASC)
    {
        if (!is_string($column) || ($column = trim($column)) == '') {
            throw new \InvalidArgumentException('Argument $column must be a string');
        } else if ($direction != self::ORDER_ASC && $direction != self::ORDER_DESC) {
            throw new \InvalidArgumentException(sprintf(
                'Argument $direction must be either %1$s::ORDER_ASC or %1$s::ORDER_DESC',
                __CLASS__
            ));
        }

        $this->withinGroupOrder = array($column => $direction);
        return $this;
    }

    /**
     * Set HAVING condition
     *
     * @param string|array|ExpressionInterface $condition HAVING condition
     * @return Select
     * @throws \InvalidArgumentException
     */
    public function having($condition)
    {
        if ($condition instanceof ExpressionInterface) {
            $this->having = $condition;
        } else if (is_string($condition)) {
            $this->having = new Expression\Literal($condition);
        } else if (is_array($condition)) {
            if (sizeof($condition) > 1) {
                throw new \InvalidArgumentException('Multiple conditions in HAVING clause are not supported');
            }

            $this->having = new Expression\Expression(key($condition), current($condition));
        } else {
            throw new \InvalidArgumentException(sprintf(
                'Argument $condition must be either a string, an array, an instance of %s\\Expression\\PredicateInterface',
                __NAMESPACE__
            ));
        }

        return $this;
    }

    /**
     * Add a column or an expression that will be used to order by
     *
     * $column can be a plain string, such as 'col1 ASC' or an array, e.g.,
     *
     * array(
     *     'col1' => Select::ORDER_ASC,
     *     'col2 ASC',
     *     'col3' // if order direction is not specified, it is assumed to be {@see Select::ORDER_ASC}
     * )
     *
     * @param string|array $column Column or expression to order by
     * @return Select
     * @throws \InvalidArgumentException
     */
    public function order($column)
    {
        foreach ((array)$column as $name => $direction) {
            if (is_int($name)) {
                $order = preg_split('/\s+/', trim($direction), 2);

                if (sizeof($order) == 1) {
                    $order[] = self::ORDER_ASC;
                }

                list($name, $direction) = $order;
            } else {
                $name = trim($name);
            }

            $direction = strtoupper(trim($direction));

            if (empty($name)) {
                throw new \InvalidArgumentException('Invalid column or expression alias, must be a string');
            } else if ($direction != self::ORDER_ASC && $direction != self::ORDER_DESC) {
                throw new \InvalidArgumentException(sprintf(
                    'Invalid direction, must be either %1$s::ORDER_ASC or %1$s::ORDER_DESC',
                    __CLASS__
                ));
            }

            $this->order[$name] = $direction;
        }

        return $this;
    }

    /**
     * Set rows offset
     *
     * @param int $offset Offset
     * @return Select
     * @throws \InvalidArgumentException
     */
    public function offset($offset)
    {
        if (!is_numeric($offset)) {
            throw new \InvalidArgumentException('Argument $offset must be numeric');
        }

        $this->offset = (int)$offset;
        return $this;
    }

    /**
     * Set rows limit
     *
     * @param int $limit Rows limit
     * @param null|int $offset Rows offset
     * @return Select
     * @throws \InvalidArgumentException
     */
    public function limit($limit, $offset = null)
    {
        if (!is_numeric($limit)) {
            throw new \InvalidArgumentException('Argument $limit must be numeric');
        }

        $this->limit = (int)$limit;

        if (!is_null($offset)) {
            $this->offset($offset);
        }

        return $this;
    }

    /**
     * Reset a single or several parts of the SphinxQL query
     *
     * $part is one or array of Select constants
     *
     * @param string|array $part Query part (one of the class constants)
     * @return Select
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
            case self::COLUMNS:
                $this->columns = array(self::STAR);
                break;

            case self::INDEX:
                $this->indexes = null;
                break;

            case self::WHERE:
                $this->resetWhere();
                break;

            case self::GROUP_BY:
                $this->group = array();
                break;

            case self::WITHIN_GROUP_ORDER_BY:
                $this->withinGroupOrder = null;
                break;

            case self::HAVING:
                $this->having = null;
                break;

            case self::ORDER_BY:
                $this->order = array();
                break;

            case self::OFFSET:
                $this->offset = null;
                break;

            case self::LIMIT:
                $this->limit = null;
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
        $columns = array();
        $indexes = array();

        foreach ($this->columns as $index => $column) {
            if ($column instanceof ExpressionInterface) {
                $column = '(' . $this->processExpression($column, $connection) . ')';
            } else if (is_float($column) || is_bool($column) || is_int(($column))) {
                $column = $connection->quoteValue($column);
            } else if (is_string($column)) {
                if (($column = trim($column)) == self::STAR) {
                    $columns[] = $column;
                    continue;
                }

                $column = $connection->quoteIdentifier($column);
            }

            if (is_string($index) && ($index = trim($index)) != '') {
                $columns[] = $column . ' AS ' . $connection->quoteIdentifier($index);
            } else {
                $columns[] = $column;
            }
        }

        foreach ($this->indexes as $index) {
            if ($index instanceof Select) {
                /** @var Select $index */
                $indexes[] = '(' . $index->getQueryString($connection) . ')';
            } else if (is_string($index) && ($index = trim($index)) != '') {
                $indexes[] = $connection->quoteIdentifier($index);
            } else {
                throw new QueryBuilderException(sprintf('Invalid index name, must be a string or an instance of %s\\Select', __NAMESPACE__));
            }
        }

        $query = sprintf('SELECT %s FROM %s', implode(', ', $columns), implode(', ', $indexes));
        unset($columns, $indexes);

        if ($this->hasWhere()) {
            $query .= sprintf(' WHERE %s', $this->processWhere($connection));
        }

        if (sizeof($this->group) > 0) {
            $query .= ' GROUP BY ' . $connection->quoteIdentifierArray($this->group);
        }

        if (is_array($this->withinGroupOrder)) {
            $query .= ' WITHIN GROUP ORDER BY ' . $connection->quoteIdentifier(key($this->withinGroupOrder)) . ' ' . current($this->withinGroupOrder);
        }

        if ($this->having instanceof ExpressionInterface) {
            $query .= ' HAVING ' . $this->processExpression($this->having, $connection);
        }

        if (sizeof($this->order) > 0) {
            $order = array();

            foreach ($this->order AS $column => $direction) {
                $order[] = $connection->quoteIdentifier($column) . ' ' . $direction;
            }

            $query .= ' ORDER BY ' . implode(', ', $order);
            unset($order, $column, $direction);
        }

        if (!is_null($this->limit)) {
            $query .= ' LIMIT ' . (!is_null($this->offset) ? $this->offset . ',' : '') . $this->limit;
        }

        if ($this->hasOptions()) {
            $query .= sprintf(' OPTION %s', $this->processOptions());
        }

        return $query;
    }
}