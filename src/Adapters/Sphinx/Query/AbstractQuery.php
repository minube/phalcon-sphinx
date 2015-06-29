<?php

namespace Adapters\Sphinx\Query;

use Adapters\Sphinx\Connection\AbstractConnection;
use Adapters\Sphinx\Query\Expression\ExpressionInterface;

/**
 * Class AbstractQuery is an abstract base class for creating SphinxQL queries
 */
abstract class AbstractQuery
{
    /**
     * Reset a single or several parts of the SphinxQL query
     *
     * @param string|array $part Query part (one of the class constants)
     * @return AbstractQuery
     */
    abstract public function reset($part);

    /**
     * Compile SphinxQL query and return it as a string
     *
     * @param AbstractConnection $connection Connection
     * @return string
     */
    abstract public function getQueryString(AbstractConnection $connection);

    /**
     * Process expression and return it as a string
     *
     * @param ExpressionInterface $expression Expression to process
     * @param AbstractConnection $connection Connection
     * @return string
     */
    protected function processExpression(ExpressionInterface $expression, AbstractConnection $connection)
    {
        list($expression, $parameters, $types) = $expression->getExpressionData();

        $expression = str_replace('%', '%%', $expression);
        $expression = str_replace(ExpressionInterface::PLACEHOLDER, '%s', $expression);

        $values = array();

        foreach ($parameters as $index => $value) {
            $values[] = (isset($types[$index]) && $types[$index] == ExpressionInterface::IDENTIFIER ?
                $connection->quoteIdentifier($value) :
                $connection->quoteValue($value));
        }

        return vsprintf($expression, $values);
    }
}
