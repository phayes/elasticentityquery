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
      // avg aggregation
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


}