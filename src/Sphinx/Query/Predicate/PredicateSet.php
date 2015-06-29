<?php

namespace Adapters\Sphinx\Query\Predicate;

/**
 * Class PredicateSet accepts conditions that can be evaluated to SphinxQL logic and are used in search (WHERE and HAVING)
 */
class PredicateSet implements \Countable
{
    /**
     * Array of predicates
     *
     * @var array
     */
    protected $predicates = array();

    /**
     * Add 'equal to' predicate to the set
     *
     * @param mixed $left Left expression side
     * @param mixed $right Right expression side
     * @param string $leftType Left side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @param string $rightType Right side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @return PredicateSet
     */
    public function equalTo($left, $right, $leftType = PredicateInterface::IDENTIFIER, $rightType = PredicateInterface::VALUE)
    {
        return $this->addPredicate(new Operator($left, Operator::EQ, $right, $leftType, $rightType));
    }

    /**
     * Add 'not equal to' predicate to the set
     *
     * @param mixed $left Left expression side
     * @param mixed $right Right expression side
     * @param string $leftType Left side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @param string $rightType Right side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @return PredicateSet
     */
    public function notEqualTo($left, $right, $leftType = PredicateInterface::IDENTIFIER, $rightType = PredicateInterface::VALUE)
    {
        return $this->addPredicate(new Operator($left, Operator::NE, $right, $leftType, $rightType));
    }

    /**
     * Add 'less than' predicate to the set
     *
     * @param mixed $left Left expression side
     * @param mixed $right Right expression side
     * @param string $leftType Left side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @param string $rightType Right side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @return PredicateSet
     */
    public function lessThan($left, $right, $leftType = PredicateInterface::IDENTIFIER, $rightType = PredicateInterface::VALUE)
    {
        return $this->addPredicate(new Operator($left, Operator::LT, $right, $leftType, $rightType));
    }

    /**
     * Add 'greater than' predicate to the set
     *
     * @param mixed $left Left expression side
     * @param mixed $right Right expression side
     * @param string $leftType Left side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @param string $rightType Right side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @return PredicateSet
     */
    public function greaterThan($left, $right, $leftType = PredicateInterface::IDENTIFIER, $rightType = PredicateInterface::VALUE)
    {
        return $this->addPredicate(new Operator($left, Operator::GT, $right, $leftType, $rightType));
    }

    /**
     * Add 'less than or equal to' predicate to the set
     *
     * @param mixed $left Left expression side
     * @param mixed $right Right expression side
     * @param string $leftType Left side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @param string $rightType Right side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @return PredicateSet
     */
    public function lessThanOrEqualTo($left, $right, $leftType = PredicateInterface::IDENTIFIER, $rightType = PredicateInterface::VALUE)
    {
        return $this->addPredicate(new Operator($left, Operator::LTE, $right, $leftType, $rightType));
    }

    /**
     * Add 'greater than or equal to' predicate to the set
     *
     * @param mixed $left Left expression side
     * @param mixed $right Right expression side
     * @param string $leftType Left side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @param string $rightType Right side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @return PredicateSet
     */
    public function greaterThanOrEqualTo($left, $right, $leftType = PredicateInterface::IDENTIFIER, $rightType = PredicateInterface::VALUE)
    {
        return $this->addPredicate(new Operator($left, Operator::GTE, $right, $leftType, $rightType));
    }

    /**
     * Add custom predicate expression to the set
     *
     * @param string $expression Expression
     * @param array $parameters Expression parameters
     * @param null|array $types Parameters types, either {@see ExpressionInterface::VALUE} or {@see ExpressionInterface::IDENTIFIER}
     * @return PredicateSet
     */
    public function expression($expression, array $parameters, array $types = null)
    {
        return $this->addPredicate(new Expression($expression, $parameters, $types));
    }

    /**
     * Add literal predicate expression to the set
     *
     * @param string $expression Expression
     * @return PredicateSet
     */
    public function literal($expression)
    {
        return $this->addPredicate(new Literal($expression));
    }

    /**
     * Add BETWEEN predicate to the set
     *
     * @param string $identifier Identifier
     * @param int $minValue Min value
     * @param int $maxValue Max value
     * @return PredicateSet
     */
    public function between($identifier, $minValue, $maxValue)
    {
        return $this->addPredicate(new Between($identifier, $minValue, $maxValue));
    }

    /**
     * Add IN predicate to the set
     *
     * @param string $identifier Identifier
     * @param int[] $values Values
     * @return PredicateSet
     */
    public function in($identifier, array $values)
    {
        return $this->addPredicate(new In($identifier, $values));
    }

    /**
     * Add NOT IN predicate to the set
     *
     * @param string $identifier Identifier
     * @param int[] $values Values
     * @return PredicateSet
     */
    public function notIn($identifier, array $values)
    {
        return $this->addPredicate(new NotIn($identifier, $values));
    }

    /**
     * Add MATCH() predicate to the set
     *
     * $query string is accepted AS IS, it is not escaped. If words inside query need escaping, {@see SphinxQL::quoteMatch()}
     * should be used.
     *
     * @link http://sphinxsearch.com/docs/current.html#boolean-syntax
     * @link http://sphinxsearch.com/docs/current.html#extended-syntax
     *
     * @param string $query Full-text search query
     * @return PredicateSet
     */
    public function match($query)
    {
        return $this->addPredicate(new Match($query));
    }

    /**
     * Add predicate to the set
     *
     * @param PredicateInterface $predicate Predicate
     * @return PredicateSet
     */
    public function addPredicate(PredicateInterface $predicate)
    {
        return $this->predicates[] = $predicate;
    }

    /**
     * Return predicates
     *
     * @return PredicateInterface[]
     */
    public function getPredicates()
    {
        return $this->predicates;
    }

    /**
     * Count elements of an object
     *
     * @link http://php.net/manual/en/countable.count.php
     * @return int The custom count as an integer.
     */
    public function count()
    {
        return sizeof($this->predicates);
    }
}
