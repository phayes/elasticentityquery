<?php

namespace Drupal\Tests\elasticentityquery\Unit;

use Drupal\elasticentityquery\Query;
use Drupal\elasticentityquery\QueryAggregate;
use Drupal\elasticentityquery\Elastic;
use Drupal\Core\Entity\EntityType;
use Drupal\Tests\UnitTestCase;

/**
 * Query units tests.
 * 
 * @ingroup elasticentityquery
 *
 * @group elasticentityquery
 */
class QueryAggregateTest extends UnitTestCase {

  /**
   * The elasticsearch client.
   */
  protected $client;

  /**
   * {@inheritdoc}
   */
  protected function setUp() {
    parent::setUp();

    $items = [
      [
        'id' => 1,
        'user_id' => 1,
        'field_test_1' => 1,
        'field_test_2' => 2
      ], 
      [
        'id' => 2,
        'user_id' => 2,
        'field_test_1' => 1,
        'field_test_2' => 7,
      ], 
      [
        'id' => 3,
        'user_id' => 2,
        'field_test_1' => 2,
        'field_test_2' => 1,
      ], 
      [
        'id' => 4,
        'user_id' => 2,
        'field_test_1' => 2,
        'field_test_2' => 8,
      ], 
      [
        'id' => 5,
        'user_id' => 3,
        'field_test_1' => 2,
        'field_test_2' => 2,
      ], 
      [
        'id' => 6,
        'user_id' => 3,
        'field_test_1' => 3,
        'field_test_2' => 8,
      ]
    ];


    $this->client = Elastic::client();

    try {
      $this->client->indices()->stats();
    } catch (\Exception $e) {
      $this->markTestSkipped("Could Not connect to elasticsearch. Skipping elasticentityquery");
      return;
    }

    // Create a test index and load content
    if (!$this->client->indices()->exists(['index' => 'elasticentityquery_test'])) {
      $index = json_decode(file_get_contents(__DIR__ . '/content/index-definition.json'), TRUE);
      $this->client->indices()->create(['index' => 'elasticentityquery_test', 'body' => $index]);
      foreach ($items as $item) {
        $this->client->index([
          'index' => 'elasticentityquery_test', 
          'type' => 'item', 
          'id' => $item['id'], 
          'body' => $item, 
        ]);
      }
      // Wait for indexing to complete
      sleep(1);
    }
  }

  protected function newQueryAggregate($conjunction = 'AND') {
    $entity_type = new EntityType(['id' => 'elasticentityquery_test']);
    $namespaces = ['Drupal\Core\Entity\Query\Sql'];
    $query = new QueryAggregate($entity_type, $conjunction, $this->client, $namespaces);
    return $query;
  }

  /**
   * Test aggregation support.
   */
  public function testAggregation() {
    // Apply a simple groupby.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('user_id')
      ->execute();

    $this->assertResults(array(
      array('user_id' => 1),
      array('user_id' => 2),
      array('user_id' => 3),
    ));

    $function_expected = array();
    $function_expected['count'] = array(array('id_count' => 6));
    $function_expected['min'] = array(array('id_min' => 1));
    $function_expected['max'] = array(array('id_max' => 6));
    $function_expected['sum'] = array(array('id_sum' => 21));
    $function_expected['avg'] = array(array('id_avg' => (21.0 / 6.0)));

    // Apply a simple aggregation for different aggregation functions.
    foreach ($function_expected as $aggregation_function => $expected) {
      $this->queryResult = $this->newQueryAggregate()
        ->aggregate('id', $aggregation_function)
        ->execute();
      $this->assertEquals($this->queryResult, $expected);
    }

    // Apply aggregation and groupby on the same query.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(
      array('user_id' => 1, 'id_count' => 1),
      array('user_id' => 2, 'id_count' => 3),
      array('user_id' => 3, 'id_count' => 2),
    ));

    // Apply aggregation and a condition which matches.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->groupBy('id')
      ->conditionAggregate('id', 'COUNT', 8)
      ->execute();
    $this->assertResults(array());

    // Don't call aggregate to test the implicit aggregate call.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('id')
      ->conditionAggregate('id', 'COUNT', 8)
      ->execute();
    $this->assertResults(array());

    // Apply aggregation and a condition which matches.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'count')
      ->groupBy('id')
      ->conditionAggregate('id', 'COUNT', 6)
      ->execute();
    $this->assertResults(array(array('id_count' => 6)));

    // Apply aggregation, a groupby and a condition which matches partially via
    // the operator '='.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'count')
      ->conditionAggregate('id', 'count', 2)
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(array('id_count' => 2, 'user_id' => 3)));

    // Apply aggregation, a groupby and a condition which matches partially via
    // the operator '>'.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'count')
      ->conditionAggregate('id', 'COUNT', 1, '>')
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(
      array('id_count' => 2, 'user_id' => 3),
      array('id_count' => 3, 'user_id' => 2),
    ));

    // Apply aggregation and a sort. This might not be useful, but have a proper
    // test coverage.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->sortAggregate('id', 'COUNT')
      ->execute();
    $this->assertSortedResults(array(array('id_count' => 6)));

    // Don't call aggregate to test the implicit aggregate call.
    $this->queryResult = $this->newQueryAggregate()
      ->sortAggregate('id', 'COUNT')
      ->execute();
    $this->assertSortedResults(array(array('id_count' => 6)));

    // Apply aggregation, groupby and a sort descending.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->groupBy('user_id')
      ->sortAggregate('id', 'COUNT', 'DESC')
      ->execute();
    $this->assertSortedResults(array(
      array('user_id' => 2, 'id_count' => 3),
      array('user_id' => 3, 'id_count' => 2),
      array('user_id' => 1, 'id_count' => 1),
    ));

    // Apply aggregation, groupby and a sort ascending.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->groupBy('user_id')
      ->sortAggregate('id', 'COUNT', 'ASC')
      ->execute();
    $this->assertSortedResults(array(
      array('user_id' => 1, 'id_count' => 1),
      array('user_id' => 3, 'id_count' => 2),
      array('user_id' => 2, 'id_count' => 3),
    ));

    // Apply aggregation, groupby, an aggregation condition and a sort with the
    // operator '='.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->groupBy('user_id')
      ->sortAggregate('id', 'COUNT')
      ->conditionAggregate('id', 'COUNT', 2)
      ->execute();
    $this->assertSortedResults(array(array('id_count' => 2, 'user_id' => 3)));

    // Apply aggregation, groupby, an aggregation condition and a sort with the
    // operator '<' and order ASC.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->groupBy('user_id')
      ->sortAggregate('id', 'COUNT', 'ASC')
      ->conditionAggregate('id', 'COUNT', 3, '<')
      ->execute();
    $this->assertSortedResults(array(
      array('id_count' => 1, 'user_id' => 1),
      array('id_count' => 2, 'user_id' => 3),
    ));

    // Apply aggregation, groupby, an aggregation condition and a sort with the
    // operator '<' and order DESC.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('id', 'COUNT')
      ->groupBy('user_id')
      ->sortAggregate('id', 'COUNT', 'DESC')
      ->conditionAggregate('id', 'COUNT', 3, '<')
      ->execute();
    $this->assertSortedResults(array(
      array('id_count' => 2, 'user_id' => 3),
      array('id_count' => 1, 'user_id' => 1),
    ));

    // Test aggregation/groupby support for fieldapi fields.

    // Just group by a fieldapi field.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1),
      array('field_test_1' => 2),
      array('field_test_1' => 3),
    ));

    // Group by a fieldapi field and aggregate a normal property.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('user_id', 'COUNT')
      ->groupBy('field_test_1')
      ->execute();

    $this->assertResults(array(
      array('field_test_1' => 1, 'user_id_count' => 2),
      array('field_test_1' => 2, 'user_id_count' => 3),
      array('field_test_1' => 3, 'user_id_count' => 1),
    ));

    // Group by a normal property and aggregate a fieldapi field.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('field_test_1', 'COUNT')
      ->groupBy('user_id')
      ->execute();

    $this->assertResults(array(
      array('user_id' => 1, 'field_test_1_count' => 1),
      array('user_id' => 2, 'field_test_1_count' => 3),
      array('user_id' => 3, 'field_test_1_count' => 2),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('field_test_1', 'SUM')
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(
      array('user_id' => 1, 'field_test_1_sum' => 1),
      array('user_id' => 2, 'field_test_1_sum' => 5),
      array('user_id' => 3, 'field_test_1_sum' => 5),
    ));

    // Aggregate by two different fieldapi fields.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('field_test_1', 'SUM')
      ->aggregate('field_test_2', 'SUM')
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(
      array('user_id' => 1, 'field_test_1_sum' => 1, 'field_test_2_sum' => 2),
      array('user_id' => 2, 'field_test_1_sum' => 5, 'field_test_2_sum' => 16),
      array('user_id' => 3, 'field_test_1_sum' => 5, 'field_test_2_sum' => 10),
    ));

    // This time aggregate the same field twice.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('field_test_1', 'SUM')
      ->aggregate('field_test_1', 'COUNT')
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(
      array('user_id' => 1, 'field_test_1_sum' => 1, 'field_test_1_count' => 1),
      array('user_id' => 2, 'field_test_1_sum' => 5, 'field_test_1_count' => 3),
      array('user_id' => 3, 'field_test_1_sum' => 5, 'field_test_1_count' => 2),
    ));

    // Group by and aggregate by a fieldapi field.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->aggregate('field_test_2', 'COUNT')
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1, 'field_test_2_count' => 2),
      array('field_test_1' => 2, 'field_test_2_count' => 3),
      array('field_test_1' => 3, 'field_test_2_count' => 1),
    ));

    // Group by and aggregate by a fieldapi field and use multiple aggregate
    // functions.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->aggregate('field_test_2', 'COUNT')
      ->aggregate('field_test_2', 'SUM')
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1, 'field_test_2_count' => 2, 'field_test_2_sum' => 9),
      array('field_test_1' => 2, 'field_test_2_count' => 3, 'field_test_2_sum' => 11),
      array('field_test_1' => 3, 'field_test_2_count' => 1, 'field_test_2_sum' => 8),
    ));

    // Apply an aggregate condition for a fieldapi field and group by a simple
    // property.
    $this->queryResult = $this->newQueryAggregate()
      ->conditionAggregate('field_test_1', 'COUNT', 3)
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(
      array('user_id' => 2, 'field_test_1_count' => 3),
      array('user_id' => 3, 'field_test_1_count' => 2),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('field_test_1', 'SUM')
      ->conditionAggregate('field_test_1', 'COUNT', 2, '>')
      ->groupBy('user_id')
      ->execute();
    $this->assertResults(array(
      array('user_id' => 2, 'field_test_1_sum' => 5, 'field_test_1_count' => 3),
      array('user_id' => 3, 'field_test_1_sum' => 5, 'field_test_1_count' => 2),
    ));

    // Apply an aggregate condition for a simple property and a group by a
    // fieldapi field.
    $this->queryResult = $this->newQueryAggregate()
      ->conditionAggregate('user_id', 'COUNT', 2)
      ->groupBy('field_test_1')
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1, 'user_id_count' => 2),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->conditionAggregate('user_id', 'COUNT', 2, '>')
      ->groupBy('field_test_1')
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1, 'user_id_count' => 2),
      array('field_test_1' => 2, 'user_id_count' => 3),
    ));

    // Apply an aggregate condition and a group by fieldapi fields.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->conditionAggregate('field_test_2', 'COUNT', 2)
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1, 'field_test_2_count' => 2),
    ));
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->conditionAggregate('field_test_2', 'COUNT', 2, '>')
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1, 'field_test_2_count' => 2),
      array('field_test_1' => 2, 'field_test_2_count' => 3),
    ));

    // Apply an aggregate condition and a group by fieldapi fields with multiple
    // conditions via AND.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->conditionAggregate('field_test_2', 'COUNT', 2)
      ->conditionAggregate('field_test_2', 'SUM', 8)
      ->execute();
    $this->assertResults(array());

    // Apply an aggregate condition and a group by fieldapi fields with multiple
    // conditions via OR.
    $this->queryResult = $this->newQueryAggregate('OR')
      ->groupBy('field_test_1')
      ->conditionAggregate('field_test_2', 'COUNT', 2)
      ->conditionAggregate('field_test_2', 'SUM', 8)
      ->execute();
    $this->assertResults(array(
      array('field_test_1' => 1, 'field_test_2_count' => 2, 'field_test_2_sum' => 9),
      array('field_test_1' => 3, 'field_test_2_count' => 1, 'field_test_2_sum' => 8),
    ));

    // Group by a normal property and aggregate a fieldapi field and sort by the
    // groupby field.
    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('field_test_1', 'COUNT')
      ->groupBy('user_id')
      ->sort('user_id', 'DESC')
      ->execute();
    $this->assertSortedResults(array(
      array('user_id' => 3, 'field_test_1_count' => 2),
      array('user_id' => 2, 'field_test_1_count' => 3),
      array('user_id' => 1, 'field_test_1_count' => 1),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->aggregate('field_test_1', 'COUNT')
      ->groupBy('user_id')
      ->sort('user_id', 'ASC')
      ->execute();
    $this->assertSortedResults(array(
      array('user_id' => 1, 'field_test_1_count' => 1),
      array('user_id' => 2, 'field_test_1_count' => 3),
      array('user_id' => 3, 'field_test_1_count' => 2),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->conditionAggregate('field_test_1', 'COUNT', 2, '>')
      ->groupBy('user_id')
      ->sort('user_id', 'ASC')
      ->execute();
    $this->assertSortedResults(array(
      array('user_id' => 2, 'field_test_1_count' => 3),
      array('user_id' => 3, 'field_test_1_count' => 2),
    ));

    // Group by a normal property, aggregate a fieldapi field, and sort by the
    // aggregated field.
    $this->queryResult = $this->newQueryAggregate()
      ->sortAggregate('field_test_1', 'COUNT', 'DESC')
      ->groupBy('user_id')
      ->execute();
    $this->assertSortedResults(array(
      array('user_id' => 2, 'field_test_1_count' => 3),
      array('user_id' => 3, 'field_test_1_count' => 2),
      array('user_id' => 1, 'field_test_1_count' => 1),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->sortAggregate('field_test_1', 'COUNT', 'ASC')
      ->groupBy('user_id')
      ->execute();
    $this->assertSortedResults(array(
      array('user_id' => 1, 'field_test_1_count' => 1),
      array('user_id' => 3, 'field_test_1_count' => 2),
      array('user_id' => 2, 'field_test_1_count' => 3),
    ));

    // Group by and aggregate by fieldapi field, and sort by the groupby field.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->aggregate('field_test_2', 'COUNT')
      ->sort('field_test_1', 'ASC')
      ->execute();
    $this->assertSortedResults(array(
      array('field_test_1' => 1, 'field_test_2_count' => 2),
      array('field_test_1' => 2, 'field_test_2_count' => 3),
      array('field_test_1' => 3, 'field_test_2_count' => 1),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->aggregate('field_test_2', 'COUNT')
      ->sort('field_test_1', 'DESC')
      ->execute();
    $this->assertSortedResults(array(
      array('field_test_1' => 3, 'field_test_2_count' => 1),
      array('field_test_1' => 2, 'field_test_2_count' => 3),
      array('field_test_1' => 1, 'field_test_2_count' => 2),
    ));

    // Groupby and aggregate by fieldapi field, and sort by the aggregated
    // field.
    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->sortAggregate('field_test_2', 'COUNT', 'DESC')
      ->execute();
    $this->assertSortedResults(array(
      array('field_test_1' => 2, 'field_test_2_count' => 3),
      array('field_test_1' => 1, 'field_test_2_count' => 2),
      array('field_test_1' => 3, 'field_test_2_count' => 1),
    ));

    $this->queryResult = $this->newQueryAggregate()
      ->groupBy('field_test_1')
      ->sortAggregate('field_test_2', 'COUNT', 'ASC')
      ->execute();
    $this->assertSortedResults(array(
      array('field_test_1' => 3, 'field_test_2_count' => 1),
      array('field_test_1' => 1, 'field_test_2_count' => 2),
      array('field_test_1' => 2, 'field_test_2_count' => 3),
    ));

  }

  /**
   * Asserts the results as expected regardless of order between and in rows.
   *
   * @param array $expected
   *   An array of the expected results.
   */
  protected function assertResults($expected, $sorted = FALSE) {
    $found = TRUE;
    $expected_keys = array_keys($expected);
    foreach ($this->queryResult as $key => $row) {
      $keys = $sorted ? array($key) : $expected_keys;
      foreach ($keys as $key) {
        $expected_row = $expected[$key];
        if (!array_diff_assoc($row, $expected_row) && !array_diff_assoc($expected_row, $row)) {
          continue 2;
        }
      }
      $found = FALSE;
      break;
    }
    return $this->assertTrue($found, strtr('!expected expected, !found found', array('!expected' => print_r($expected, TRUE), '!found' => print_r($this->queryResult, TRUE))));
  }

  /**
   * Asserts the results as expected regardless of order in rows.
   *
   * @param array $expected
   *   An array of the expected results.
   */
  protected function assertSortedResults($expected) {
    return $this->assertResults($expected, TRUE);
  }

  static function tearDownAfterClass() {
    $client = Elastic::client();
    $client->indices()->delete(['index' => 'elasticentityquery_test']);
  }
}