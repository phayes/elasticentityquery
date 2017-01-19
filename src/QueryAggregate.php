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
    foreach ($this->aggregate as $alias => $aggregate) {
      foreach ($result['aggregations'] as $groupField => $group_agg) {
        foreach ($group_agg['buckets'] as $bucket) {
          if (strtolower($aggregate['function']) != 'count') {
            $res[] = [
              $alias => $bucket[$alias]['value'],
              $groupField => $bucket['key'], 
            ];
          }
          else {
            $res[] = [
              $alias => $bucket['doc_count'],
              $groupField => $bucket['key'], 
            ];
          }
        }
      }
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


    // TODO: REDO THIS TO SUPPORT
    // - Aggregate without group by
    // - multiple groupBy clauses - http://stackoverflow.com/questions/20775040/elasticsearch-group-by-multiple-fields


    if (!empty($this->groupBy)) {
      foreach ($this->groupBy as $group) {
        $params['body']['aggs'][$group['field']]['terms']['field'] = $group['field'];
        foreach ($this->aggregate as $aggregate) {
          if (strtolower($aggregate['function']) != 'count') { // count is always included implicit
            $func = $this->getAggFunc($aggregate['function']);
            $params['body']['aggs'][$group['field']]['aggs'][$aggregate['alias']][$func]['field'] = $aggregate['field'];
          }
        }
      }
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
      'UNIQUE' => 'cardinality',
    ];

    // If not found just try to use what's provided directlty as an aggregate function
    return $map[strtoupper($sql_function)] ?? strtolower($sql_function);
  }

  

}