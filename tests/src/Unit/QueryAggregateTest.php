<?php

namespace Drupal\Tests\elasticentityquery\Unit;

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
      $items = json_decode(file_get_contents(__DIR__ . '/content/testcontent.json'), TRUE);
      foreach ($items as $item) {
        $this->client->index([
          'index' => 'elasticentityquery_test', 
          'type' => 'item', 
          'id' => $item['guid'], 
          'body' => $item]
        );
      }
      // Wait for indexing to complete
      sleep(1);
    }
  }

  protected function newQuery($conjunction = 'AND') {
    $entity_type = new EntityType(['id' => 'elasticentityquery_test']);
    $namespaces = ['Drupal\Core\Entity\Query\Sql'];
    $query = new QueryAggregate($entity_type, $conjunction, $this->client, $namespaces);
    return $query;
  }
  
  public function testBasicAggregateQuery() {
    // avg aggregation
    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')->aggregate('age', 'avg')->execute();
    $this->assertEquals(3, count($result));
    foreach ($result as $res) {
      if ($res['eyeColor'] == 'blue') {
        $this->assertEquals(30, $res['age_avg'], 'average age blue');
      }
      if ($res['eyeColor'] == 'brown') {
        $this->assertEquals(22, $res['age_avg'], 'average age brown');
      }
      if ($res['eyeColor'] == 'green') {
        $this->assertEquals(29, $res['age_avg'], 'average age green');
      }
    }

    // count aggregation
    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')->aggregate('guid', 'count')->execute();
    $this->assertEquals(3, count($result));
    foreach ($result as $res) {
      if ($res['eyeColor'] == 'blue') {
        $this->assertEquals(2, $res['guid_count'], 'count blue');
      }
      if ($res['eyeColor'] == 'brown') {
        $this->assertEquals(2, $res['guid_count'], 'count brown');
      }
      if ($res['eyeColor'] == 'green') {
        $this->assertEquals(1, $res['guid_count'], 'count green');
      }
    }

    // aggregation with no grouping
    $query = $this->newQuery();
    $result = $query->aggregate('age', 'sum')->execute();
    $this->assertEquals(133, $result[0]['age_sum'], 'sum all ages with no grouping');
  }

  public function testMultiGroup() {
    // get the average age, sorted by eye-colour and activity
    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')->groupBy('isActive')->aggregate('age', 'avg')->execute();
    $this->assertEquals(4, count($result));
    foreach ($result as $res) {
      if (!$res['isActive'] && $res['eyeColor'] == 'blue') {
        $this->assertEquals(30, $res['age_avg'], '!isActive/blue, age_avg');
      }
      if (!$res['isActive'] && $res['eyeColor'] == 'brown') {
        $this->assertEquals(21, $res['age_avg'], '!isActive/brown, age_avg');
      }
    }
  }

  public function testMultiAggregation() {
    // For each eye-colour, get the max-age and the min-age
    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')->aggregate('age', 'min')->aggregate('age', 'max')->execute();
    $this->assertEquals(3, count($result));
    foreach ($result as $res) {
      if ($res['eyeColor'] == 'blue') {
        $this->assertEquals(27, $res['age_min'], 'age_min blue');
        $this->assertEquals(33, $res['age_max'], 'age_max blue');
      }
      if ($res['eyeColor'] == 'green') {
        $this->assertEquals(29, $res['age_min'], 'age_min green');
        $this->assertEquals(29, $res['age_max'], 'age_max green');
      }
    }
  }

  public function testMultiGroupMultiAggregation() {
    // For each eye-colour and activity, get the max-age and the min-age
    $query = $this->newQuery();
    $result = $query
      ->groupBy('eyeColor')
      ->groupBy('isActive')
      ->aggregate('age', 'min')
      ->aggregate('age', 'max')
      ->execute();
    $this->assertEquals(4, count($result));
    foreach ($result as $res) {
      if (!$res['isActive'] && $res['eyeColor'] == 'blue') {
        $this->assertEquals(27, $res['age_min'], 'age_min !isActive-blue');
        $this->assertEquals(33, $res['age_max'], 'age_max !isActive-blue');
      }
      if (!$res['isActive'] && $res['eyeColor'] == 'brown') {
        $this->assertEquals(21, $res['age_min'], 'age_min !isActive-brown');
        $this->assertEquals(21, $res['age_max'], 'age_max !isActive-brown');
      }
    }
  }

  public function testSortAggregation() {
    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')->sortAggregate('age', 'avg', 'asc')->execute();
    $this->assertEquals(3, count($result));
    $this->assertEquals(22, $result[0]['age_avg']);
    $this->assertEquals('brown', $result[0]['eyeColor']);
    $this->assertEquals(29, $result[1]['age_avg']);
    $this->assertEquals('green', $result[1]['eyeColor']);
    $this->assertEquals(30, $result[2]['age_avg']);
    $this->assertEquals('blue', $result[2]['eyeColor']);

    // sortAggregation with no grouping - has no impact since there is only one result
    $query = $this->newQuery();
    $result = $query->sortAggregate('age', 'sum', 'desc')->execute();
    $this->assertEquals(133, $result[0]['age_sum'], 'sum all ages with no grouping');

    // multi-level sort on an aggregation
    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')->sortAggregate('isActive', 'max', 'asc')->sortAggregate('age', 'avg', 'asc')->execute();
    $this->assertEquals(3, count($result));
    $this->assertEquals(0, $result[0]['isactive_max']);
    $this->assertEquals(30, $result[0]['age_avg']);
    $this->assertEquals(1, $result[1]['isactive_max']);
    $this->assertEquals(22, $result[1]['age_avg']);
    $this->assertEquals(1, $result[2]['isactive_max']);
    $this->assertEquals(29, $result[2]['age_avg']);
  }

  public function testConditionAggregation() {
    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')->conditionAggregate('age', 'avg', '25', '>')->execute();
    $this->assertEquals(2, count($result));

    $query = $this->newQuery();
    $result = $query->groupBy('eyeColor')
      ->conditionAggregate('age', 'avg', '25', '>')
      ->conditionAggregate('age', 'min', 27)
      ->execute();
    $this->assertEquals(1, count($result));
    $this->assertEquals(30, $result[0]['age_avg']);
    $this->assertEquals(27, $result[0]['age_min']);
    $this->assertEquals('blue', $result[0]['eyeColor']);
  }



}