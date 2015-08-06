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
 */
class Select extends AbstractQuery
{
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
     * Set of WHERE clause conditions (predicates)
     *
     * @var Where
     */
    protected $where = null;

    /**
     * OPTION parameters
     *
     * @var array
     */
    protected $options = array();

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
                throw new \QueryBuilderException(sprintf('Invalid index name, must be a string or an instance of %s\\Select', __NAMESPACE__));
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

    /**
     * Add WHERE search condition
     *
     * Possible $predicate values:
     * - {@see Where}, replaces current set of predicates
     * - {@see PredicateInterface}, is added to the set
     * - {@see \Closure} is invoked with an instance if Where given as an argument
     * - string, is treated as a literal expression
     * - array, a set of WHERE conditions can be specified using array, see below
     *
     * Use-cases when passing an array of predicates as $predicate:
     *
     * array(
     *     'a = ?' => 1,                       // new Predicate\Expression('a = ?', array(1))
     *     'a BETWEEN ? AND ?' => array(1, 2), // new Predicate\Expression('a BETWEEN ? AND ?', array(1, 2))
     *     'a' => array(1, 2, 3),              // new Predicate\In('a', array(1, 2, 3))
     *     'a' => 1,                           // new Predicate\Operator('a', Predicate\Operator::EQ, 1)
     *     Predicate\PredicateInterface(),     // add to WHERE as it is
     *     'string'                            // new Predicate\Literal('string')
     * )
     *
     * @param string|array|Where|PredicateInterface|\Closure $predicate Predicate
     * @return AbstractQuery
     * @throws \InvalidArgumentException
     */
    public function where($predicate)
    {
        if ($predicate instanceof Where) {
            $this->where = $predicate;
        } else if ($predicate instanceof PredicateInterface) {
            $this->where->addPredicate($predicate);
        } else if ($predicate instanceof \Closure) {
            call_user_func($predicate, $this->where);
        } else if (is_string($predicate)) {
            $this->where->literal($predicate);
        } else if (is_array($predicate)) {
            foreach ($predicate as $key => $value) {
                if (is_string($key)) {
                    if (strpos($key, PredicateInterface::PLACEHOLDER) !== false) {
                        $this->where->expression($key, (array)$value);
                    } else {
                        if (is_array($value)) {
                            $this->where->in($key, $value);
                        } else {
                            $this->where->equalTo($key, $value);
                        }
                    }
                } else if ($value instanceof PredicateInterface || is_string($value)) {
                    $this->where($value);
                } else {
                    throw new \InvalidArgumentException(sprintf('Cannot add $predicate[%d] to WHERE clause', $key));
                }
            }
        } else {
            throw new \InvalidArgumentException(sprintf(
                'Argument $predicate must be either a string, an array, an instance of %1$s\\Where or %1$s\\Predicate\\PredicateInterface, or a \\Closure',
                __NAMESPACE__
            ));
        }

        return $this;
    }

    /**
     * Return TRUE if {@see WhereTrait::$where} has been initialized, otherwise return FALSE
     *
     * @return bool
     */
    protected function hasWhere()
    {
        return (sizeof($this->where) > 0);
    }

    /**
     * Reset WHERE part
     *
     * @return AbstractQuery
     */
    protected function resetWhere()
    {
        $this->where = new Where();
        return $this;
    }

    /**
     * Build WHERE clause and return it as a string
     *
     * @param AbstractConnection $connection Connection
     * @return string
     */
    protected function processWhere(AbstractConnection $connection)
    {
        if (($n = sizeof($this->where)) == 0) {
            return '';
        }

        $predicates = $this->where->getPredicates();

        $values = array();
        $where = '';

        for ($i = 0; $i < $n; $i++) {
            list($expression, $parameters, $types) = $predicates[$i]->getExpressionData();

            $isMatch = ($predicates[$i] instanceof Predicate\Match);

            if ($i > 0) {
                $where .= ' AND ';
            }

            $where .= $expression;

            foreach ($parameters as $index => $value) {
                if (!$isMatch) {
                    $value = (isset($types[$index]) && $types[$index] == PredicateInterface::IDENTIFIER ?
                        $connection->quoteIdentifier($value) :
                        $connection->quoteValue($value));
                }

                $values[] = $value;
            }
        }

        $where = str_replace('%', '%%', $where);
        $where = str_replace(PredicateInterface::PLACEHOLDER, '%s', $where);

        return vsprintf($where, $values);
    }

    /**
     * @param array $options
     * @return AbstractQuery
     */
    public function options(array $options)
    {
        foreach ($options as $option => $value) {
            $this->options[strtolower($option)] = $value;
        }

        return $this;
    }

    /**
     * Check whether any option is set
     *
     * @return bool
     */
    protected function hasOptions()
    {
        return (sizeof($this->options) > 0);
    }

    /**
     * Reset options
     *
     * @return AbstractQuery
     */
    protected function resetOptions()
    {
        $this->options = array();
        return $this;
    }

    /**
     * Process options and return them as a string
     *
     * Note: option name is not escaped / quoted
     *
     * @return string
     * @throws QueryBuilderException
     */
    protected function processOptions()
    {
        if (sizeof($this->options) == 0) {
            return '';
        }

        $options = array();

        foreach ($this->options as $option => $value) {
            if (is_array($value)) {
                $values = array();
                $isNamedIntegers = null;

                foreach ($value as $key => $val) {
                    if ($isNamedIntegers === null) {
                        $isNamedIntegers = is_string($key);
                    } else if (($isNamedIntegers && !is_string($key)) || (!$isNamedIntegers && is_string($key))) {
                        throw new QueryBuilderException('Named integer list and list of strings cannot be mixed together');
                    }

                    if ($isNamedIntegers) {
                        $values[] = sprintf('%s = %d', $key, $val);
                    } else {
                        $values[] = (string)$val;
                    }
                }

                $value = ($isNamedIntegers ? '(' : "'") . implode(', ', $values) . ($isNamedIntegers ? ')' : "'");
            }

            $options[] = sprintf('%s = %s', $option, $value);
        }

        return implode(', ', $options);
    }
}
