<?php

namespace Adapters\Sphinx\Query;

use Adapters\Sphinx\Connection\AbstractConnection;
use Adapters\Sphinx\Exception\QueryBuilderException;

/**
 * Class Delete is a SphinxQL DELETE query builder
 *
 * DELETE statement is only supported for RT indexes and for distributed which contains only RT indexes as agents.
 * It deletes existing rows (documents) from an existing index.
 *
 * DELETE FROM index WHERE where_condition
 *
 * @link        http://sphinxsearch.com/docs/current.html#sphinxql-delete
 */
class Delete extends AbstractQuery
{
    use WhereTrait;

    /**
     * Index name SphinxQL query part
     */
    const INDEX = 'index';

    /**
     * WHERE SphinxQL query part
     */
    const WHERE = 'where';

    /**
     * Index name
     *
     * @var string
     */
    protected $index = null;

    /**
     * Constructor
     *
     * @param null|string $index Index name
     */
    public function __construct($index = null)
    {
        if (!is_null($index)) {
            $this->from($index);
        }

        $this->resetWhere();
    }

    /**
     * Set index name to delete from
     *
     * @param string $index Index name
     * @return Delete
     * @throws \InvalidArgumentException
     */
    public function from($index)
    {
        if (!is_string($index) || ($index = trim($index)) == '') {
            throw new \InvalidArgumentException('Argument $index must be a string');
        }

        $this->index = $index;
        return $this;
    }

    /**
     * Reset a single or several parts of the SphinxQL query
     *
     * $part is one or array of Delete constants: {@see Delete::INDEX} or {@see Delete::WHERE}
     *
     * @param string|array $part Query part (one of the class constants)
     * @return Delete
     * @throws \InvalidArgumentException
     */
    public function reset($part)
    {
        if (is_array($part)) {
            foreach ($part as $reset) {
                $this->reset($reset);
            }

            return $this;
        }

        switch ($part) {
            case self::INDEX:
                $this->index = null;
                break;

            case self::WHERE:
                $this->resetWhere();
                break;

            default:
                throw new \InvalidArgumentException(sprintf('Unknown SphinxQL query part to reset "%s"', $part));
        }

        return $this;
    }

    /**
     * Compile SphinxQL query and return it as a string
     *
     * @param AbstractConnection $connection Connection
     * @return string
     * @throws QueryBuilderException
     */
    public function getQueryString(AbstractConnection $connection)
    {
        if ($this->index === null) {
            throw new QueryBuilderException('Index name is required');
        }

        $query = sprintf('DELETE FROM %s', $connection->quoteIdentifier($this->index));

        if ($this->hasWhere()) {
            $query .= sprintf(' WHERE %s', $this->processWhere($connection));
        }

        return $query;
    }

    /**
     * __clone() magic method override
     */
    public function __clone()
    {
        $this->where = clone $this->where;
    }
}
