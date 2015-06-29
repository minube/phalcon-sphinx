<?php

namespace Adapters\Sphinx\Query;

use Adapters\Sphinx\Connection\AbstractConnection;
use Adapters\Sphinx\Query\Predicate\PredicateInterface;

/**
 * Trait WhereTrait provides functionality that adds WHERE clause to a query object
 */
trait WhereTrait
{
    /**
     * Set of WHERE clause conditions (predicates)
     *
     * @var Where
     */
    protected $where = null;

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
}
