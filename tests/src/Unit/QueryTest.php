<?php

namespace Drupal\Tests\elasticentityquery\Unit;

use Drupal\elasticentityquery\Query;
use Drupal\Core\Entity\EntityType;
use Drupal\Tests\UnitTestCase;
use Elasticsearch\ClientBuilder;

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

    $this->client = ClientBuilder::create()->build();

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
    $result = $query->getResult();
    $this->assertEquals(1, $result['hits']['total'], '=');

    // <>
    $query = $this->newQuery($conjunction);
    $query->condition('guid', '0d24fe77-5fef-49fe-9696-7ea784b241b6', '<>');
    $result = $query->getResult();
    $this->assertEquals(4, $result['hits']['total'], '<>');

    // IN
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', ['brown', 'green'], 'IN');
    $result = $query->getResult();
    $this->assertEquals(3, $result['hits']['total'], 'IN');

    // NOT IN
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', ['brown', 'green'], 'NOT IN');
    $result = $query->getResult();
    $this->assertEquals(2, $result['hits']['total'], 'NOT IN');

    // IS NOT NULL / exists
    $query = $this->newQuery($conjunction);
    $query->exists('favoriteFruit');
    $result = $query->getResult();
    $this->assertEquals(5, $result['hits']['total'], 'IS NOT NULL / exists');

    // IS NULL / notExists
    $query = $this->newQuery($conjunction);
    $query->notExists('favoriteFruit');
    $result = $query->getResult();
    $this->assertEquals(0, $result['hits']['total'], 'IS NULL / notExists');

    // >
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '>');
    $result = $query->getResult();
    $this->assertEquals(0, $result['hits']['total'], '>');

    // >=
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '>=');
    $result = $query->getResult();
    $this->assertEquals(1, $result['hits']['total'], '>=');

    // <
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '<');
    $result = $query->getResult();
    $this->assertEquals(4, $result['hits']['total'], '<');

    // <=
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '<=');
    $result = $query->getResult();
    $this->assertEquals(5, $result['hits']['total'], '<=');

    // BETWEEN
    $query = $this->newQuery($conjunction);
    $query->condition('age', [20, 25], 'BETWEEN');
    $result = $query->getResult();
    $this->assertEquals(2, $result['hits']['total'], 'BETWEEN');

    // STARTS_WITH
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', 'b', 'STARTS_WITH');
    $result = $query->getResult();
    $this->assertEquals(4, $result['hits']['total'], 'STARTS_WITH');

    // ENDS_WITH
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', 'een', 'ENDS_WITH');
    $result = $query->getResult();
    $this->assertEquals(1, $result['hits']['total'], 'ENDS_WITH');

    // CONTAINS
    $query = $this->newQuery($conjunction);
    $query->condition('address', 'Tennessee', 'CONTAINS');
    $result = $query->getResult();
    $this->assertEquals(2, $result['hits']['total'], 'CONTAINS');
  }

  function testSingleOr() {
    $conjunction = 'OR';

    // =
    $query = $this->newQuery($conjunction);
    $query->condition('guid', '0d24fe77-5fef-49fe-9696-7ea784b241b6');
    $result = $query->getResult();
    $this->assertEquals(1, $result['hits']['total'], '=');

    // <>
    $query = $this->newQuery($conjunction);
    $query->condition('guid', '0d24fe77-5fef-49fe-9696-7ea784b241b6', '<>');
    $result = $query->getResult();
    $this->assertEquals(4, $result['hits']['total'], '<>');

    // IN
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', ['brown', 'green'], 'IN');
    $result = $query->getResult();
    $this->assertEquals(3, $result['hits']['total'], 'IN');

    // NOT IN
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', ['brown', 'green'], 'NOT IN');
    $result = $query->getResult();
    $this->assertEquals(2, $result['hits']['total'], 'NOT IN');

    // IS NOT NULL / exists
    $query = $this->newQuery($conjunction);
    $query->exists('favoriteFruit');
    $result = $query->getResult();
    $this->assertEquals(5, $result['hits']['total'], 'IS NOT NULL / exists');

    // IS NULL / notExists
    $query = $this->newQuery($conjunction);
    $query->notExists('favoriteFruit');
    $result = $query->getResult();
    $this->assertEquals(0, $result['hits']['total'], 'IS NULL / notExists');

    // >
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '>');
    $result = $query->getResult();
    $this->assertEquals(0, $result['hits']['total'], '>');

    // >=
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '>=');
    $result = $query->getResult();
    $this->assertEquals(1, $result['hits']['total'], '>=');

    // <
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '<');
    $result = $query->getResult();
    $this->assertEquals(4, $result['hits']['total'], '<');

    // <=
    $query = $this->newQuery($conjunction);
    $query->condition('age', 33, '<=');
    $result = $query->getResult();
    $this->assertEquals(5, $result['hits']['total'], '<=');

    // BETWEEN
    $query = $this->newQuery($conjunction);
    $query->condition('age', [20, 25], 'BETWEEN');
    $result = $query->getResult();
    $this->assertEquals(2, $result['hits']['total'], 'BETWEEN');

    // STARTS_WITH
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', 'b', 'STARTS_WITH');
    $result = $query->getResult();
    $this->assertEquals(4, $result['hits']['total'], 'STARTS_WITH');

    // ENDS_WITH
    $query = $this->newQuery($conjunction);
    $query->condition('eyeColor', 'een', 'ENDS_WITH');
    $result = $query->getResult();
    $this->assertEquals(1, $result['hits']['total'], 'ENDS_WITH');

    // CONTAINS
    $query = $this->newQuery($conjunction);
    $query->condition('address', 'Tennessee', 'CONTAINS');
    $result = $query->getResult();
    $this->assertEquals(2, $result['hits']['total'], 'CONTAINS');
  }

  static function tearDownAfterClass() {
    $client = ClientBuilder::create()->build();
    $client->indices()->delete(['index' => 'elasticentityquery_test']);
  }

}