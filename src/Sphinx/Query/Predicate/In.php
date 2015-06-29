<?php

namespace Adapters\Sphinx\Query\Predicate;

/**
 * Class In implements IN search predicate
 */
class In extends AbstractPredicate
{
    /**
     * Expression
     *
     * @var string
     */
    protected $expression = '? IN ?';

    /**
     * Parameters types
     *
     * @var array
     */
    protected $types = array(self::IDENTIFIER, self::VALUE);

    /**
     * Constructor
     *
     * @param string $identifier Identifier
     * @param int[] $values Values
     */
    public function __construct($identifier, array $values)
    {
        $this->parameters = array($identifier, $values);
    }
}
