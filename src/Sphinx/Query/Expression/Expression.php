<?php

namespace Adapters\Sphinx\Query\Expression;

/**
 * Class Expression is a generic expression
 */
class Expression implements ExpressionInterface
{
    /**
     * Expression
     *
     * @var string
     */
    protected $expression = null;

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
     * Constructor
     *
     * $types is a simple array, its number of entries should match a number of $parameters elements. $types array specifies
     * how each corresponding $parameters element should be quoted (as a value ot as an identifier). If $types is NULL
     * then all $parameters are treated as {@see ExpressionInterface::VALUE}.
     *
     * Example:
     *
     * new Expression(
     *     '? BETWEEN ? AND ?',
     *     array('col1', 1, 5),
     *     array(
     *         ExpressionInterface::IDENTIFIER, // 'col1'
     *         ExpressionInterface::VALUE       // 1
     *         // or if type is not specified, then it is ExpressionInterface::VALUE by default
     *     )
     * )
     *
     * @param string     $expression Expression
     * @param array      $parameters Expression parameters
     * @param null|array $types      Parameters types, either {@see ExpressionInterface::VALUE} or {@see ExpressionInterface::IDENTIFIER}
     * @throws \InvalidArgumentException
     */
    public function __construct($expression, array $parameters, array $types = null)
    {
        settype($expression, 'string');

        if ( ( $parametersCount = sizeof($parameters) ) != substr_count($expression, self::PLACEHOLDER) ) {
            throw new \InvalidArgumentException('Number of placeholders and given parameters don\'t match');
        }

        $this->expression = $expression;
        $this->parameters = $parameters;
        $this->types = ( $types === null ? array_fill(0, $parametersCount, self::VALUE) : $types );
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
        return array($this->expression, $this->parameters, $this->types);
    }
}
