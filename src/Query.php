<?php

namespace Drupal\elasticentityquery;

use Drupal\Core\Entity\Query\Sql\Condition;
use Drupal\Core\Entity\EntityTypeInterface;
use Drupal\Core\Entity\Query\QueryBase;
use Drupal\Core\Entity\Query\QueryInterface;
use Elasticsearch\Client;

/**
 * The elasticsearch entity query class.
 */
class Query extends QueryBase implements QueryInterface {

  /**
   * The elasticsearch query
   *
   * @var array
   */
  protected $query;

  /**
   * An array of fields keyed by the field alias.
   *
   * Each entry correlates to the arguments of
   * \Drupal\Core\Database\Query\SelectInterface::addField(), so the first one
   * is the table alias, the second one the field and the last one optional the
   * field alias.
   *
   * @var array
   */
  protected $sqlFields = [];

  /**
   * An array of strings added as to the group by, keyed by the string to avoid
   * duplicates.
   *
   * @var array
   */
  protected $sqlGroupBy = [];

  /**
   * @var \Elasticsearch\Client
   */
  protected $client;

  /**
   * Limit query by type
   *
   * @var array
   */
  protected $content_type = [];

  /**
   * Constructs a query object.
   *
   * @param \Drupal\Core\Entity\EntityTypeInterface $entity_type
   *   The entity type definition.
   * @param string $conjunction
   *   - AND: all of the conditions on the query need to match.
   *   - OR: at least one of the conditions on the query need to match.
   * @param \Elasticsearch\Client $client
   *   Elasticsearch client to use.
   * @param array $namespaces
   *   List of potential namespaces of the classes belonging to this query.
   */
  public function __construct(EntityTypeInterface $entity_type, $conjunction = 'AND', Client $client, array $namespaces) {
    parent::__construct($entity_type, $conjunction, $namespaces);
    $this->client = $client;
  }

  /**
   * {@inheritdoc}
   */
  public function execute() {
    $result = $this->getResult();

    if ($this->count) {
      return $result['count'];
    }
    else {
      // TODO: support for revisions? See https://api.drupal.org/api/drupal/core%21lib%21Drupal%21Core%21Entity%21Query%21QueryInterface.php/function/QueryInterface%3A%3Aexecute/8.2.x
      $results = [];
      foreach ($result['hits']['hits'] as $hit) {
        $results[$hit['_id']] = $this->includeSource() ? $hit['_source'] : $hit['_id'];
      }
      return $results;
    }
  }

  /**
   * Get the results of the elastic query.
   *
   * @return array
   *   An array of results from elastic search.
   */
  public function getResult() {
    $params = $this->buildRequest();

    if ($this->count) {
      $result = $this->client->count($params);
    }
    else {
      $result = $this->client->search($params);
    }

    return $result;
  }

  /**
   * Get the elastic indexes to search
   *
   * @return array
   *   An array of elastic indexes.
   */
  public function getIndex() {
    return $this->entityTypeId;
  }

  /**
   * Translate the field name from the local name to the elasticname
   *
   * 
   * By default this does nothing, but can be overridden by child classes.
   * 
   * @param string $field
   *  The field name.
   * 
   * @return string
   *   The translated field name.
   */
  public function translateField($field) {
    return $field;
  }

  public function includeSource() {
    return FALSE;
  }

  /**
   * Build the elatic index request.
   *
   * @return array
   *   A request array that can be sent using the
   *   elasticsearch client.
   */
  protected function buildRequest() {
    $params = [
      'index' => $this->getIndex(),
    ];

    // For regular (non-count) queries
    if (!$this->count) {
      // Don't include source.
      $params['body']['_source'] = $this->includeSource();
    }

    // Top-level condition
    $topbool = $this->getElasticFilterItem($this->condition);
    if (!empty($topbool)) {
      $params['body']['query']['bool'] = $topbool;
    }

    // Sorting
    if (!empty($this->sort)) {
      foreach ($this->sort as $sort) {
        $params['body']['sort'] = [$this->translateField($sort['field']) => ['order' => strtolower($sort['direction'])]];
      }
    }

    // Range
    if (!empty($this->range) && !$this->count) {
      if (!empty($this->range['start'])) {
        $params['body']['from'] = $this->range['start'];
      }
      if (!empty($this->range['length'])) {
        $params['body']['size'] = $this->range['length'];
      }
    }

    // Allow near unlimited results by default (10,000)
    // TODO: Make this configurable
    if (!$this->count) {
      $params['body']['size'] = $params['body']['size'] ?? (10000 - ($params['body']['from'] ?? 0));
    }

    // Set the type if specified
    if (!empty($this->content_type)) {
      $params['type'] = $this->content_type;
    }

    return $params;
  }

  private function getElasticFilterItem(Condition $condition) {
    $conjunction = strtoupper($condition->getConjunction());
    $bool = [];
    foreach ($condition->conditions() as $subcondition) {
      $operator = $subcondition['operator'] ?? '=';

      if (is_string($subcondition['field'])) {
        $field = $this->translateField($subcondition['field']);
      }
      else {
        $field = $subcondition['field'];
      }
      $value = $subcondition['value'];

      if (is_object($field) && $conjunction == "AND") {
        $bool['filter'][] = ['bool' => $this->getElasticFilterItem($field)];
      }
      elseif (is_object($field) && $conjunction == "AND") {
        $bool['should'][] = ['bool' => $this->getElasticFilterItem($field)];
      }
      elseif ($operator == "=" && $conjunction == "AND") {
        if (!is_array($value)) {
          $value = [$value];
        }
        $bool['filter'][] = ['terms' => [$field => $value]];
      }
      elseif ($operator == "=" && $conjunction == "OR") {
        if (!is_array($value)) {
          $value = [$value];
        }
        $bool['should'][] = ['terms' => [$field => $value]];
      }
      elseif (($operator == "<>" || $operator == "!=") && $conjunction == "AND") {
        if (!is_array($value)) {
          $value = [$value];
        }
        $bool['must_not'][] = ['terms' => [$field => $value]];
      }
      elseif (($operator == "<>" || $operator == "!=") && $conjunction == "OR") {
        $bool['should'][] = ['bool' => ['must_not' => ['term' => [$field => $value]]]];
      }
      elseif ($operator == "IN" && $conjunction == "AND") {
        if (!is_array($value)) {
          $value = [$value];
        }
        $bool['filter'][] = ['terms' => [$field => $value]];
      }
      elseif ($operator == "IN" && $conjunction == "OR") {
        if (!is_array($value)) {
          $value = [$value];
        }
        $bool['should'][] = ['terms' => [$field => $value]];
      }
      elseif ($operator == "NOT IN" && $conjunction == "AND") {
        if (!is_array($value)) {
          $value = [$value];
        }
        $bool['must_not'][] = ['terms' => [$field => $value]];
      }
      elseif ($operator == "NOT IN" && $conjunction == "OR") {
        if (!is_array($value)) {
          $value = [$value];
        }
        $bool['should'][] = ['bool' => ['must_not' => ['terms' => [$field => $value]]]];
      }
      elseif ($operator == "IS NULL" && $conjunction == "AND") {
        $bool['must_not'][] = ['exists' => ['field' => $field]];
      }
      elseif ($operator == "IS NULL" && $conjunction == "OR") {
        $bool['should'][] = ['bool' => ['must_not' => ['exists' => ['field' => $field]]]];
      }
      elseif ($operator == "IS NOT NULL" && $conjunction == "AND") {
        $bool['filter'][] = ['exists' => ['field' => $field]];
      }
      elseif ($operator == "IS NOT NULL" && $conjunction == "OR") {
        $bool['should'][] = ['exists' => ['field' => $field]];
      }
      elseif ($operator == ">" && $conjunction == "AND") {
        $bool['filter'][] = ['range' => [$field => ['gt' => $value]]];
      }
      elseif ($operator == ">" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['gt' => $value]]];
      }
      elseif ($operator == ">=" && $conjunction == "AND") {
        $bool['filter'][] = ['range' => [$field => ['gte' => $value]]];
      }
      elseif ($operator == ">=" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['gte' => $value]]];
      }
      elseif ($operator == "<" && $conjunction == "AND") {
        $bool['filter'][] = ['range' => [$field => ['lt' => $value]]];
      }
      elseif ($operator == "<" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['lt' => $value]]];
      }
      elseif ($operator == "<=" && $conjunction == "AND") {
        $bool['filter'][] = ['range' => [$field => ['lte' => $value]]];
      }
      elseif ($operator == "<=" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['lte' => $value]]];
      }
      elseif ($operator == "BETWEEN") {
        natsort($value);
        if ($conjunction == "AND") {
          $bool['filter'][] = ['range' => [$field => [
            'gt' => $value[0],
            'lt' => $value[1],
          ]]];
        }
        elseif ($conjunction == "OR") {
          $bool['should'][] = ['range' => [$field => [
            'gt' => $value[0],
            'lt' => $value[1],
          ]]];
        }
      }
      elseif ($operator == "STARTS_WITH" && $conjunction == "AND") {
        $bool['filter'][] = ['prefix' => [$field => $value]];
      }
      elseif ($operator == "STARTS_WITH" && $conjunction == "OR") {
        $bool['should'][] = ['prefix' => [$field => $value]];
      }
      elseif ($operator == "ENDS_WITH" && $conjunction == "AND") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['filter'][] = ['wildcard' => [$field => '*'.$value]];
      }
      elseif ($operator == "ENDS_WITH" && $conjunction == "OR") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['should'][] = ['wildcard' => [$field => '*'.$value]];
      }
      elseif ($operator == "CONTAINS" && $conjunction == "AND") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['filter'][] = ['wildcard' => [$field => '*'.$value.'*']];
      }
      elseif ($operator == "CONTAINS" && $conjunction == "OR") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['should'][] = ['wildcard' => [$field => '*'.$value.'*']];
      }
    }

    return $bool;
  }

  public function addType($type) {
    $this->content_type[] = $type;
    return $this;
  }

  public function hasCondition($field, $operator = NULL) {
    foreach ($this->condition->conditions() as $i => $subcondition) {
      if ($subcondition['field'] == $field ) {
        return TRUE;
      }
    }
    return FALSE;
  }

  public function getCondition($field) {
    foreach ($this->condition->conditions() as $i => $subcondition) {
      if ($subcondition['field'] == $field) {
        return $subcondition;
      }
    }
    return FALSE;
  }

  public function deleteCondition($field) {
    $conditions = &$this->condition->conditions();
    foreach ($this->condition->conditions() as $i => $subcondition) {
      if ($subcondition['field'] == $field) {
        unset($conditions[$i]);
      }
    }
  }

}
