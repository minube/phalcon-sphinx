<?php

namespace Adapters\Sphinx\Query\Predicate;

/**
 * Class AbstractPredicate is an abstract class for creating predicates
 */
class AbstractPredicate implements PredicateInterface
{
    /**
     * Expression
     *
     * @var string
     */
    protected $expression = '';

    /**
     * Expression parameters
     *
     * @var array
     */
    protected $parameters = array();

    /**
     * Parameters types
     *
     * @var array
     */
    protected $types = array();

    /**
     * Get expression internal data
     *
     * Data returned must be an array containing the following values in the given order:
     *
     * - string $expression Expression string
     * - array  $parameters Parameters to insert into $expression, instead of placeholders
     * - array  $types      Parameters types (either {@see ExpressionInterface::IDENTIFIER} or {@see ExpressionInterface::VALUE})
     *
     * @return array[]
     */
    public function getExpressionData()
    {
        return array($this->expression, $this->parameters, $this->types);
    }
}
