<?php

namespace Adapters\Sphinx\Query\Predicate;

/**
 * Class Match
 */
class Match extends AbstractPredicate
{
    /**
     * Expression
     *
     * @var string
     */
    protected $expression = 'MATCH(\'?\')';

    /**
     * Parameters types
     *
     * @var array
     */
    protected $types = array(self::VALUE);

    /**
     * Constructor
     *
     * $query string is accepted AS IS, it is not escaped. If words inside query need escaping, {@see SphinxQL::quoteMatch()}
     * should be used.
     *
     * @link http://sphinxsearch.com/docs/current.html#boolean-syntax
     * @link http://sphinxsearch.com/docs/current.html#extended-syntax
     *
     * @param string $query Full-text search query
     */
    public function __construct($query)
    {
        $this->parameters = array(trim($query, "'"));
    }
}
