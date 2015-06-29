<?php

namespace Adapters\Sphinx\Query;

use Adapters\Sphinx\Exception\QueryBuilderException;

/**
 * Trait OptionTrait provides functionality that adds OPTION clause to a query object
 */
trait OptionTrait
{
    /**
     * OPTION parameters
     *
     * @var array
     */
    protected $options = array();

    /**
     * @param array $options
     * @return AbstractQuery
     */
    public function options(array $options)
    {
        foreach ($options as $option => $value) {
            $this->options[strtolower($option)] = $value;
        }

        return $this;
    }

    /**
     * Check whether any option is set
     *
     * @return bool
     */
    protected function hasOptions()
    {
        return (sizeof($this->options) > 0);
    }

    /**
     * Reset options
     *
     * @return AbstractQuery
     */
    protected function resetOptions()
    {
        $this->options = array();
        return $this;
    }

    /**
     * Process options and return them as a string
     *
     * Note: option name is not escaped / quoted
     *
     * @return string
     * @throws QueryBuilderException
     */
    protected function processOptions()
    {
        if (sizeof($this->options) == 0) {
            return '';
        }

        $options = array();

        foreach ($this->options as $option => $value) {
            if (is_array($value)) {
                $values = array();
                $isNamedIntegers = null;

                foreach ($value as $key => $val) {
                    if ($isNamedIntegers === null) {
                        $isNamedIntegers = is_string($key);
                    } else if (($isNamedIntegers && !is_string($key)) || (!$isNamedIntegers && is_string($key))) {
                        throw new QueryBuilderException('Named integer list and list of strings cannot be mixed together');
                    }

                    if ($isNamedIntegers) {
                        $values[] = sprintf('%s = %d', $key, $val);
                    } else {
                        $values[] = (string)$val;
                    }
                }

                $value = ($isNamedIntegers ? '(' : "'") . implode(', ', $values) . ($isNamedIntegers ? ')' : "'");
            }

            $options[] = sprintf('%s = %s', $option, $value);
        }

        return implode(', ', $options);
    }
}
