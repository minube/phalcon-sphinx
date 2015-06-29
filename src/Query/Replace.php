<?php

namespace Adapters\Sphinx\Query;

/**
 * Class Replace is a SphinxQL REPLACE query builder
 *
 * REPLACE statement is only supported for RT indexes. It inserts new rows (documents) into an existing index,
 * with the provided column values. ID column must be present in all cases. Rows with duplicate IDs
 * will be overwritten by REPLACE.
 *
 * REPLACE INTO index [(column, ...)]
 *     VALUES (value, ...)
 *     [, (...)]
 *
 * @link        http://sphinxsearch.com/docs/current.html#sphinxql-replace
 */
class Replace extends Insert
{
    /**
     * Query template
     *
     * @var string
     */
    protected static $query = 'REPLACE INTO %s VALUES %s';
}
