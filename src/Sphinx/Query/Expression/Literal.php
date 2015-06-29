<?php

namespace Adapters\Sphinx\Query\Expression;

/**
 * Class Literal implements a literal expression that is embedded into the query as it is
 */
class Literal implements ExpressionInterface
{
    /**
     * Expression
     *
     * @var string
     */
    protected $expression = null;

    /**
     * Constructor
     *
     * @param string $expression Expression
     */
    public function __construct($expression)
    {
        $this->expression = (string)$expression;
    }

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
        return array($this->expression, array(), array());
    }
}
