<?php

namespace Adapters\Sphinx\Query\Predicate;

/**
 * Class NotIn implements NOT IN search predicate
 */
class NotIn extends In
{
    /**
     * Expression
     *
     * @var string
     */
    protected $expression = '? NOT IN ?';
}
