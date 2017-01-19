<?php

namespace Drupal\Tests\elasticentityquery\Unit;

use Drupal\elasticentityquery\Query;
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
class QueryTest extends UnitTestCase {

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
    $query = new Query($entity_type, $conjunction, $this->client, $namespaces);
    return $query;
  }
  
  public function testBasicQuery() {
    $query = $this->newQuery();
    $result = $query->getResult();
    $this->assertEquals(5, $result['hits']['total']);
  }

  public function testSingleAnd() {
    $conjunction = 'AND';

    // =
    $query = $this->newQuery($conjunction);
    $query->condition('guid', '0d24fe77-5fef-49fe-9696-7ea784b241b6');
    $result = $query->execute();
    $this->assertEquals(1, count($result), '=');
    $this->assertEquals('0d24fe77-5fef-49fe-9696-7ea784b241b6', reset($result), '=');

    // <>
    $query = $this->newQuery($conjunction);
    $query->condition('guid', '0d24fe77-5fef-49fe-9696-7ea784b241b6', '<>');
    $result = $query->execute();
    $this->assertEquals(4, count($result), '<>');

    // IN
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', ['brown', 'green'], 'IN');
    $result = $query->execute();
    $this->assertEquals(3, count($result), 'IN');

    // NOT IN
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', ['brown', 'green'], 'NOT IN');
    $result = $query->execute();
    $this->assertEquals(2, count($result), 'NOT IN');

    // IS NOT NULL / exists
    $query = $this->newQuery($conjunction);
    $query->exists('favoriteFruit');
    $result = $query->execute();
    $this->assertEquals(5, count($result), 'IS NOT NULL / exists');

    // IS NULL / notExists
    $query = $this->newQuery($conjunction);
    $query->notExists('favoriteFruit');
    $result = $query->execute();
    $this->assertEquals(0, count($result), 'IS NULL / notExists');

    // >
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '>');
    $result = $query->execute();
    $this->assertEquals(0, count($result), '>');

    // >=
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '>=');
    $result = $query->execute();
    $this->assertEquals(1, count($result), '>=');
    $this->assertEquals('0d24fe77-5fef-49fe-9696-7ea784b241b6', reset($result), '>=');

    // <
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '<');
    $result = $query->execute();
    $this->assertEquals(4, count($result), '<');

    // <=
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '<=');
    $result = $query->execute();
    $this->assertEquals(5, count($result), '<=');

    // BETWEEN
    $query = $this->newQuery($conjunction);
    $query->condition('age', [20, 25], 'BETWEEN');
    $result = $query->execute();
    $this->assertEquals(2, count($result), 'BETWEEN');

    // STARTS_WITH
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', 'b', 'STARTS_WITH');
    $result = $query->execute();
    $this->assertEquals(4, count($result), 'STARTS_WITH');

    // ENDS_WITH
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', 'een', 'ENDS_WITH');
    $result = $query->execute();
    $this->assertEquals(1, count($result), 'ENDS_WITH');
    $this->assertEquals('29d3077e-0378-4949-a201-b36574e6c35c', reset($result), '>=');

    // CONTAINS
    $query = $this->newQuery($conjunction);
    $query->condition('address', 'Tennessee', 'CONTAINS');
    $result = $query->execute();
    $this->assertEquals(2, count($result), 'CONTAINS');
  }

  function testMultipleAnd() {
    $conjunction = 'AND';

    // =
    $query = $this->newQuery($conjunction);
    $query->condition('tags', 'Lorem');
    $query->condition('eyeColor', 'blue');
    $result = $query->execute();
    $this->assertEquals(1, count($result), '=');
    $this->assertEquals('0d24fe77-5fef-49fe-9696-7ea784b241b6', reset($result), '=');
  }

  function testCount() {
    $conjunction = 'AND';

    $query = $this->newQuery($conjunction);
    $query->condition('tags', 'Lorem');
    $query->condition('eyeColor', 'blue');
    $query->count();
    $count = $query->execute();
    $this->assertEquals(1, $count);
  }

  function testRange() {
    $conjunction = 'AND';

    $query = $this->newQuery($conjunction);
    $query->range(3);
    $result = $query->execute();
    $this->assertEquals(2, count($result));

    $query = $this->newQuery($conjunction);
    $query->range(NULL, 2);
    $result = $query->execute();
    $this->assertEquals(2, count($result));

    $query = $this->newQuery($conjunction);
    $query->range(1, 2);
    $result = $query->execute();
    $this->assertEquals(2, count($result));
  }

  function testConditionGroup() {
    $query = $this->newQuery('AND');

    $query->condition('tags', 'dolore');
    
    $group = $query->orConditionGroup()
      ->condition('eyeColor', 'green')
      ->condition('eyeColor', 'blue');
    
    $result = $query->condition($group)->execute();

    $this->assertEquals(2, count($result));
    $this->assertContains('29d3077e-0378-4949-a201-b36574e6c35c', $result);
    $this->assertContains('1578747e-248a-4351-ae6c-aa273d8f297e', $result);
  }

  function testSort() {
    $query = $this->newQuery('AND');
    $query->sort('age', 'asc');
    $result = $query->execute();
    $this->assertEquals('1578747e-248a-4351-ae6c-aa273d8f297e', reset($result));
  
    $query = $this->newQuery('AND');
    $query->sort('age', 'desc');
    $result = $query->execute();
    $this->assertEquals('0d24fe77-5fef-49fe-9696-7ea784b241b6', reset($result));
  }

  static function tearDownAfterClass() {
    //$client = Elastic::client();
    //$client->indices()->delete(['index' => 'elasticentityquery_test']);
  }

}