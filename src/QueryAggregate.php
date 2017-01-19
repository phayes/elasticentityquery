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

  function getResultFromBucket($bucket, $group_field, $additional = []) {
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

  function buildRequest() {
    $params = parent::buildRequest();

    $aggregates = [];
    foreach ($this->aggregate as $aggregate) {
      $func = $this->getAggFunc($aggregate['function']);
      $aggregates[$aggregate['alias']][$func]['field'] = $aggregate['field'];
    }

    if (!empty($this->groupBy)) {
      $prev = FALSE;
      foreach ($this->groupBy as $group) {
        $group_by = [];
        $group_by[$group['field']]['terms']['field'] = $group['field'];
        if ($prev) {
          $group_by[$group['field']]['aggs'] = $prev;
        }
        elseif (!empty($aggregates)) {
          $group_by[$group['field']]['aggs'] = $aggregates;
          if (!empty($this->sortAggregate)) {
            foreach (array_reverse($this->sortAggregate) as $alias => $sort) {
              $group_by[$group['field']]['terms']['order'][$alias] = strtolower($sort['direction']);
            }
          }
        }
        $prev = $group_by;
      }
      $params['body']['aggs'] = $group_by;
    }
    elseif (!empty($aggregates)) {
      $params['body']['aggs'] = $aggregates;
    }

    // hits can be ignored
    $params['body']['size'] = 0;

    return $params;
  }

  private function getAggFunc($sql_function) {
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

  

}