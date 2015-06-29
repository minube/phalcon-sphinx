<?php

namespace Adapters\Sphinx\Query\Predicate;

/**
 * Class Between implements BETWEEN search predicate
 */
class Between extends AbstractPredicate
{
    /**
     * Expression
     *
     * @var string
     */
    protected $expression = '? BETWEEN ? AND ?';

    /**
     * Parameters types
     *
     * @var array
     */
    protected $types = array(self::IDENTIFIER, self::VALUE, self::VALUE);

    /**
     * Constructor
     *
     * @param string $identifier Identifier
     * @param int $minValue Min value
     * @param int $maxValue Max value
     */
    public function __construct($identifier, $minValue, $maxValue)
    {
        $this->parameters = array($identifier, $minValue, $maxValue);
    }
}
