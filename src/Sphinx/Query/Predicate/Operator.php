<?php

namespace Adapters\Sphinx\Query\Predicate;

/**
 * Class Operator implements comparison search predicate
 */
class Operator extends AbstractPredicate
{
    /**
     * 'Equal' operator
     */
    const EQ = '=';

    /**
     * 'Not equal' operator
     */
    const NE = '!=';

    /**
     * 'Less than' operator
     */
    const LT = '<';

    /**
     * 'Less than or equal' operator
     */
    const LTE = '<=';

    /**
     * 'Greater than' operator
     */
    const GT = '>';

    /**
     * 'Greater than or equal' operator
     */
    const GTE = '>=';

    /**
     * Allowed types
     *
     * @var array
     */
    protected static $allowedTypes = array(
        self::IDENTIFIER,
        self::VALUE
    );

    /**
     * Allowed operators
     *
     * @var array
     */
    protected static $allowedOperators = array(
        self::EQ,
        self::NE,
        self::LT,
        self::LTE,
        self::GT,
        self::GTE
    );

    /**
     * Constructor
     *
     * @param mixed $left Left expression side
     * @param string $operator Operator, one of the Operator constants
     * @param mixed $right Right expression side
     * @param string $leftType Left side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @param string $rightType Right side type, either {@see PredicateInterface::IDENTIFIER} or {@see PredicateInterface::VALUE}
     * @throws \InvalidArgumentException
     */
    public function __construct($left, $operator, $right, $leftType = self::IDENTIFIER, $rightType = self::VALUE)
    {
        if (!in_array($leftType, self::$allowedTypes)) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid $leftType "%s", must be one of "%s" or "%s"',
                $leftType,
                __CLASS__ . '::IDENTIFIER',
                __CLASS__ . '::VALUE'
            ));
        } else if (!in_array($rightType, self::$allowedTypes)) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid $rightType "%s", must be one of "%s" or "%s"',
                $leftType,
                __CLASS__ . '::IDENTIFIER',
                __CLASS__ . '::VALUE'
            ));
        } else if (!in_array($operator, self::$allowedOperators)) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid operator "%s", must be one of "%s" class constants',
                $operator,
                __CLASS__
            ));
        }

        $this->expression = sprintf('? %s ?', $operator);
        $this->parameters = array($left, $right);
        $this->types = array($leftType, $rightType);
    }
}
