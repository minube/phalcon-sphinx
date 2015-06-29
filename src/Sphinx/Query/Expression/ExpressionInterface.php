<?php

namespace Adapters\Sphinx\Query\Expression;

/**
 * ExpressionInterface is an interface for implementing expressions
 */
interface ExpressionInterface
{
    /**
     * Value or identifier placeholder
     */
    const PLACEHOLDER = '?';

    /**
     * Define expressions parameters is an identifier
     */
    const IDENTIFIER = 'identifier';

    /**
     * Define expressions parameters is a value
     */
    const VALUE = 'value';

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
    public function getExpressionData();
}
