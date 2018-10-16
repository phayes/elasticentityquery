<?php

namespace Drupal\elasticentityquery;

use Drupal\Core\Entity\Query\QueryAggregateInterface;

/**
 * The SQL storage entity query class.
 */
class QueryAggregate extends Query implements QueryAggregateInterface {

  public function execute() {
    $result = $this->getResult();

    $res = [];
    foreach ($result['aggregations'] as $group_field => $agg) {
      if (isset($agg['buckets'])) {
        foreach ($agg['buckets'] as $bucket) {
          $res = array_merge($res, $this->getResultFromBucket($bucket, $group_field));
        }
      }
      else {
        $res[] = [$group_field => $agg['value']];
      }
    }

    return $res;
  }

  protected function getResultFromBucket($bucket, $group_field, $additional = []) {
    $res = [];
    $value = [];

    $additional[$group_field] = $bucket['key'];
    
    foreach ($bucket as $k => $v) {
      // Value bucket
      if (is_array($v) && isset($v['value'])) {
        $value = array_merge($value, [$k => $v['value']]);
      }
      // Bucket of buckets
      elseif (is_array($v) && isset($v['buckets'])) {
        foreach ($v['buckets'] as $subbucket) {
          $res = array_merge($res, $this->getResultFromBucket($subbucket, $k, $additional));
        }
      }
    }

    if (!empty($value)) {
      $res[] = array_merge($value, $additional);
    }

    return $res;
  }

  /**
   * {@inheritdoc}
   */
  public function conditionAggregateGroupFactory($conjunction = 'AND') {
    $class = static::getClass($this->namespaces, 'ConditionAggregate');
    return new $class($conjunction, $this, $this->namespaces);
  }

  /**
   * {@inheritdoc}
   */
  public function existsAggregate($field, $function, $langcode = NULL) {
    return $this->conditionAggregate->exists($field, $function, $langcode);
  }

  /**
   * {@inheritdoc}
   */
  public function notExistsAggregate($field, $function, $langcode = NULL) {
    return $this->conditionAggregate->notExists($field, $function, $langcode);
  }

  protected function buildRequest() {
    $params = parent::buildRequest();

    $aggregates = [];
    foreach ($this->aggregate as $aggregate) {
      $func = $this->getAggFunc($aggregate['function']);
      $aggregates[$aggregate['alias']][$func]['field'] = $this->translateField($aggregate['field']);
    }

    if (!empty($this->groupBy)) {
      $prev = FALSE;
      foreach ($this->groupBy as $group) {
        $group_by = [];
        $group['field'] = $this->translateField($group['field']);
        $group_by[$group['field']]['terms']['field'] = $group['field'];
        if ($prev) {
          $group_by[$group['field']]['aggs'] = $prev;
        }
        elseif (!empty($aggregates)) {
          $group_by[$group['field']]['aggs'] = $aggregates;
          
          // Sort by aggregate values
          if (!empty($this->sortAggregate)) {
            foreach (array_reverse($this->sortAggregate) as $alias => $sort) {
              $group_by[$group['field']]['terms']['order'][$alias] = strtolower($sort['direction']);
            }
          }
          
          // Sort by groupBy buckets
          if (!empty($this->sort)) {
            foreach ($this->sort as $sort) {
              $sort['field'] = $this->translateField($sort['field']);
              if ($sort['field'] == $group['field']) {
                $group_by[$group['field']]['terms']['order']['_term'] = strtolower($sort['direction']);
              }
            }
          }

          // Filter the buckets (simiar to SQLs HAVING)
          if ($bucket_selector = $this->getBucketSelector()) {
            $group_by[$group['field']]['aggs']['bucket_filter']['bucket_selector'] = $bucket_selector;
          }
        }
        $prev = $group_by;
      }
      $params['body']['aggs'] = $group_by;
    }
    elseif (!empty($aggregates)) {
      // Aggregate without grouping. Will only ever produce one result since it's aggregating on the whole dataset.
      $params['body']['aggs'] = $aggregates;
    }

    // hits can be ignored
    $params['body']['size'] = 0;

    return $params;
  }

  protected function getAggFunc($sql_function) {
    $map = [
      'MIN' => 'min',
      'MAX' => 'max', 
      'AVG' => 'avg', 
      'SUM' => 'sum', 
      'COUNT' => 'value_count', 
      'UNIQUE' => 'cardinality',
    ];

    // If not found just try to use what's provided directlty as an aggregate function
    return $map[strtoupper($sql_function)] ?? strtolower($sql_function);
  }

  protected function getBucketSelector() {
    if (empty($this->conditionAggregate->conditions())) {
      return NULL;
    }

    $con = $this->scriptOperator($this->conditionAggregate->getConjunction());
    $script_conditions = [];
    $paths = [];
    foreach ($this->conditionAggregate->conditions() as $condition) {
      $alias = $this->getAggregationAlias($condition['field'], $condition['function']);
      $op = $this->scriptOperator($condition['operator']) ?? '==';
      $val = is_numeric($condition['value']) ? $condition['value'] : "'" . $condition['value'] . "'";
      $script_conditions[] = 'params.' . $alias . ' ' . $op . ' ' . $val;
      $paths[$alias] = $alias;
    }
    return [
      'buckets_path' => $paths,
      'script' => implode(' ' . $con . ' ', $script_conditions)
    ];
  }

  protected function scriptOperator($sql_operator) {
    $map = [
      '=' => '==', 
      '>' => '>',
      '>=' => '>=', 
      '<' => '<',
      '<=' => '<=', 
      'AND' => '&&',
      'OR' => '||',
      'NOT' => '!',
      '<>' => '!=', 
    ];

    // If not found just try to use what's provided directlty
    return $map[strtoupper($sql_operator)] ?? strtolower($sql_operator);
  }

}