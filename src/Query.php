<?php

namespace Drupal\elasticentityquery;

use Drupal\Core\Database\Connection;
use Drupal\Core\Database\Query\SelectInterface;
use Drupal\Core\Entity\EntityTypeInterface;
use Drupal\Core\Entity\Query\QueryBase;
use Drupal\Core\Entity\Query\QueryException;
use Drupal\Core\Entity\Query\QueryInterface;

/**
 * The SQL storage entity query class.
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
  protected $sqlFields = array();

  /**
   * An array of strings added as to the group by, keyed by the string to avoid
   * duplicates.
   *
   * @var array
   */
  protected $sqlGroupBy = array();

  /**
   * @var \Elasticsearch\Client
   */
  protected $client;

  /**
   * Constructs a query object.
   *
   * @param \Drupal\Core\Entity\EntityTypeInterface $entity_type
   *   The entity type definition.
   * @param string $conjunction
   *   - AND: all of the conditions on the query need to match.
   *   - OR: at least one of the conditions on the query need to match.
   * @param \Elasticsearch\Client $client
   *   Elasticsearch client to use
   * @param array $namespaces
   *   List of potential namespaces of the classes belonging to this query.
   */
  public function __construct(EntityTypeInterface $entity_type, $conjunction = 'AND', \Elasticsearch\Client $client, array $namespaces) {
    parent::__construct($entity_type, $conjunction, $namespaces);
    $this->client = $client;
  }

  /**
   * {@inheritdoc}
   */
  public function execute() {
    $this->getResult();
  }

  public function getResult() {
    $params = $this->buildRequest();
    $result = $this->client->search($params);
    return $result;
  }

  public function debug() {
    return $this->buildRequest();
  }

  private function buildRequest() {
    $params = [
      'index' => $this->entityTypeId,
    ];

    // Top-level condition
    $params['body']['query']['bool'] = $this->getElasticFilterItem($this->condition);

    // Sorting
    if (!empty($this->sort)) {
      foreach ($this->sort as $sort) {
        $params['body']['sort'] = [$sort['field'] => ['order' => strtolower($sort['direction'])]];
      }
    }

    // Range
    if (!empty($this->range)) {
      if (!empty($this->range['start'])) {
        $params['body']['from'] = $this->range['start'];
      }
      if (!empty($this->range['length'])) {
        $params['body']['size'] = $this->range['length'];
      }
    }

    return $params;
  }

  private function getElasticFilterItem(\Drupal\Core\Entity\Query\Sql\Condition $condition) {
    $conjunction = $condition->getConjunction();
    $bool = ['must' => []];
    foreach ($condition->conditions() as $subcondition) {
      $operator = $subcondition['operator'] ?? '=';
      $field = $subcondition['field'];
      $value = $subcondition['value'];

      if ($operator == "=" && $conjunction == "AND") {
        $bool['must'][] = ['term' => [$field => $value]];
      }
      if ($operator == "=" && $conjunction == "OR") {
        $bool['should'][] = ['term' => [$field => $value]];
      }
      if (($operator == "<>" || $operator == "!=") && $conjunction == "AND") {
        $bool['must_not'][] = ['term' => [$field => $value]];
      }
      if (($operator == "<>" || $operator == "!=") && $conjunction == "OR") {
        $bool['should'][] = ['bool' => ['must_not' => ['term' => [$field => $value]]]];
      }
      if ($operator == "IN" && $conjunction == "AND") {
        $bool['must'][] = ['terms' => [$field => $value]];
      }
      if ($operator == "IN" && $conjunction == "OR") {
        $bool['should'][] = ['terms' => [$field => $value]];
      }
      if ($operator == "NOT IN" && $conjunction == "AND") {
        $bool['must_not'][] = ['terms' => [$field => $value]];
      }
      if ($operator == "NOT IN" && $conjunction == "OR") {
        $bool['should'][] = ['bool' => ['must_not' => ['terms' => [$field => $value]]]];
      }
      if ($operator == "IS NULL" && $conjunction == "AND") {
        $bool['must_not'][] = ['exists' => ['field' => $field]];
      }
      if ($operator == "IS NULL" && $conjunction == "OR") {
        $bool['should'][] = ['bool' => ['must_not' => ['exists' => ['field' => $field]]]];
      }
      if ($operator == "IS NOT NULL" && $conjunction == "AND") {
        $bool['must'][] = ['exists' => ['field' => $field]];
      }
      if ($operator == "IS NOT NULL" && $conjunction == "OR") {
        $bool['should'][] = ['exists' => ['field' => $field]];
      }
      if ($operator == ">" && $conjunction == "AND") {
        $bool['must'][] = ['range' => [$field => ['gt' => $value]]];
      }
      if ($operator == ">" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['gt' => $value]]];
      }
      if ($operator == ">=" && $conjunction == "AND") {
        $bool['must'][] = ['range' => [$field => ['gte' => $value]]];
      }
      if ($operator == ">=" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['gte' => $value]]];
      }
      if ($operator == "<" && $conjunction == "AND") {
        $bool['must'][] = ['range' => [$field => ['lt' => $value]]];
      }
      if ($operator == "<" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['lt' => $value]]];
      }
      if ($operator == "<=" && $conjunction == "AND") {
        $bool['must'][] = ['range' => [$field => ['lte' => $value]]];
      }
      if ($operator == "<=" && $conjunction == "OR") {
        $bool['should'][] = ['range' => [$field => ['lte' => $value]]];
      }
      if ($operator == "BETWEEN") {
        natsort($value);
        if ($conjunction == "AND") {
          $bool['must'][] = ['range' => [$field => [
            'gt' => $value[0],
            'lt' => $value[1],
          ]]];
        }
        if ($conjunction == "OR") {
          $bool['should'][] = ['range' => [$field => [
            'gt' => $value[0],
            'lt' => $value[1],
          ]]];
        }
      }
      if ($operator == "STARTS_WITH" && $conjunction == "AND") {
        $bool['must'][] = ['prefix' => [$field => $value]];
      }
      if ($operator == "STARTS_WITH" && $conjunction == "OR") {
        $bool['should'][] = ['prefix' => [$field => $value]];
      }
      if ($operator == "ENDS_WITH" && $conjunction == "AND") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['must'][] = ['wildcard' => [$field => '*'.$value]];
      }
      if ($operator == "ENDS_WITH" && $conjunction == "OR") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['should'][] = ['wildcard' => [$field => '*'.$value]];
      }
      if ($operator == "CONTAINS" && $conjunction == "AND") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['must'][] = ['wildcard' => [$field => '*'.$value.'*']];
      }
      if ($operator == "CONTAINS" && $conjunction == "OR") {
        //TODO: WARNING THIS IS EXPENSIVE
        $bool['should'][] = ['wildcard' => [$field => '*'.$value.'*']];
      }
    }

    return $bool;
  }

  /**
   * Prepares the basic query with proper metadata/tags and base fields.
   *
   * @return \Drupal\Core\Entity\Query\Sql\Query
   *   Returns the called object.
   *
   * @throws \Drupal\Core\Entity\Query\QueryException
   *   Thrown if the base table does not exist.
   */
  protected function prepare() {
    if ($this->allRevisions) {
      if (!$base_table = $this->entityType->getRevisionTable()) {
        throw new QueryException("No revision table for " . $this->entityTypeId . ", invalid query.");
      }
    }
    else {
      if (!$base_table = $this->entityType->getBaseTable()) {
        throw new QueryException("No base table for " . $this->entityTypeId . ", invalid query.");
      }
    }
    $simple_query = TRUE;
    if ($this->entityType->getDataTable()) {
      $simple_query = FALSE;
    }
    $this->sqlQuery = $this->connection->select($base_table, 'base_table', array('conjunction' => $this->conjunction));
    $this->sqlQuery->addMetaData('entity_type', $this->entityTypeId);
    $id_field = $this->entityType->getKey('id');
    // Add the key field for fetchAllKeyed().
    if (!$revision_field = $this->entityType->getKey('revision')) {
      // When there is no revision support, the key field is the entity key.
      $this->sqlFields["base_table.$id_field"] = array('base_table', $id_field);
      // Now add the value column for fetchAllKeyed(). This is always the
      // entity id.
      $this->sqlFields["base_table.$id_field" . '_1'] = array('base_table', $id_field);
    }
    else {
      // When there is revision support, the key field is the revision key.
      $this->sqlFields["base_table.$revision_field"] = array('base_table', $revision_field);
      // Now add the value column for fetchAllKeyed(). This is always the
      // entity id.
      $this->sqlFields["base_table.$id_field"] = array('base_table', $id_field);
    }
    if ($this->accessCheck) {
      $this->sqlQuery->addTag($this->entityTypeId . '_access');
    }
    $this->sqlQuery->addTag('entity_query');
    $this->sqlQuery->addTag('entity_query_' . $this->entityTypeId);

    // Add further tags added.
    if (isset($this->alterTags)) {
      foreach ($this->alterTags as $tag => $value) {
        $this->sqlQuery->addTag($tag);
      }
    }

    // Add further metadata added.
    if (isset($this->alterMetaData)) {
      foreach ($this->alterMetaData as $key => $value) {
        $this->sqlQuery->addMetaData($key, $value);
      }
    }
    // This now contains first the table containing entity properties and
    // last the entity base table. They might be the same.
    $this->sqlQuery->addMetaData('all_revisions', $this->allRevisions);
    $this->sqlQuery->addMetaData('simple_query', $simple_query);
    return $this;
  }

  /**
   * Compiles the conditions.
   *
   * @return \Drupal\Core\Entity\Query\Sql\Query
   *   Returns the called object.
   */
  protected function compile() {
    $this->condition->compile($this->sqlQuery);
    return $this;
  }

  /**
   * Adds the sort to the build query.
   *
   * @return \Drupal\Core\Entity\Query\Sql\Query
   *   Returns the called object.
   */
  protected function addSort() {
    if ($this->count) {
      $this->sort = array();
    }
    // Gather the SQL field aliases first to make sure every field table
    // necessary is added. This might change whether the query is simple or
    // not. See below for more on simple queries.
    $sort = array();
    if ($this->sort) {
      foreach ($this->sort as $key => $data) {
        $sort[$key] = $this->getSqlField($data['field'], $data['langcode']);
      }
    }
    $simple_query = $this->isSimpleQuery();
    // If the query is set up for paging either via pager or by range or a
    // count is requested, then the correct amount of rows returned is
    // important. If the entity has a data table or multiple value fields are
    // involved then each revision might appear in several rows and this needs
    // a significantly more complex query.
    if (!$simple_query) {
      // First, GROUP BY revision id (if it has been added) and entity id.
      // Now each group contains a single revision of an entity.
      foreach ($this->sqlFields as $field) {
        $group_by = "$field[0].$field[1]";
        $this->sqlGroupBy[$group_by] = $group_by;
      }
    }
    // Now we know whether this is a simple query or not, actually do the
    // sorting.
    foreach ($sort as $key => $sql_alias) {
      $direction = $this->sort[$key]['direction'];
      if ($simple_query || isset($this->sqlGroupBy[$sql_alias])) {
        // Simple queries, and the grouped columns of complicated queries
        // can be ordered normally, without the aggregation function.
        $this->sqlQuery->orderBy($sql_alias, $direction);
        if (!isset($this->sqlFields[$sql_alias])) {
          $this->sqlFields[$sql_alias] = explode('.', $sql_alias);
        }
      }
      else {
        // Order based on the smallest element of each group if the
        // direction is ascending, or on the largest element of each group
        // if the direction is descending.
        $function = $direction == 'ASC' ? 'min' : 'max';
        $expression = "$function($sql_alias)";
        $expression_alias = $this->sqlQuery->addExpression($expression);
        $this->sqlQuery->orderBy($expression_alias, $direction);
      }
    }
    return $this;
  }

  /**
   * Finish the query by adding fields, GROUP BY and range.
   *
   * @return \Drupal\Core\Entity\Query\Sql\Query
   *   Returns the called object.
   */
  protected function finish() {
    $this->initializePager();
    if ($this->range) {
      $this->sqlQuery->range($this->range['start'], $this->range['length']);
    }
    foreach ($this->sqlGroupBy as $field) {
      $this->sqlQuery->groupBy($field);
    }
    foreach ($this->sqlFields as $field) {
      $this->sqlQuery->addField($field[0], $field[1], isset($field[2]) ? $field[2] : NULL);
    }
    return $this;
  }

  /**
   * Executes the query and returns the result.
   *
   * @return int|array
   *   Returns the query result as entity IDs.
   */
  protected function result() {
    if ($this->count) {
      return $this->sqlQuery->countQuery()->execute()->fetchField();
    }
    // Return a keyed array of results. The key is either the revision_id or
    // the entity_id depending on whether the entity type supports revisions.
    // The value is always the entity id.
    return $this->sqlQuery->execute()->fetchAllKeyed();
  }

  /**
   * Constructs a select expression for a given field and language.
   *
   * @param string $field
   *   The name of the field being queried.
   * @param string $langcode
   *   The language code of the field.
   *
   * @return string
   *   An expression that will select the given field for the given language in
   *   a SELECT query, such as 'base_table.id'.
   */
  protected function getSqlField($field, $langcode) {
    if (!isset($this->tables)) {
      $this->tables = $this->getTables($this->sqlQuery);
    }
    $base_property = "base_table.$field";
    if (isset($this->sqlFields[$base_property])) {
      return $base_property;
    }
    else {
      return $this->tables->addField($field, 'LEFT', $langcode);
    }
  }

  /**
   * Determines whether the query requires GROUP BY and ORDER BY MIN/MAX.
   *
   * @return bool
   */
  protected function isSimpleQuery() {
    return (!$this->pager && !$this->range && !$this->count) || $this->sqlQuery->getMetaData('simple_query');
  }

  /**
   * Implements the magic __clone method.
   *
   * Reset fields and GROUP BY when cloning.
   */
  public function __clone() {
    parent::__clone();
    $this->sqlFields = array();
    $this->sqlGroupBy = array();
  }

  /**
   * Gets the Tables object for this query.
   *
   * @param \Drupal\Core\Database\Query\SelectInterface $sql_query
   *   The SQL query object being built.
   *
   * @return \Drupal\Core\Entity\Query\Sql\TablesInterface
   *   The object that adds tables and fields to the SQL query object.
   */
  public function getTables(SelectInterface $sql_query) {
    $class = static::getClass($this->namespaces, 'Tables');
    return new $class($sql_query);
  }

}