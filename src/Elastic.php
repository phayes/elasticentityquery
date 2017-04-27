<?php

namespace Drupal\elasticentityquery;

use Elasticsearch\ClientBuilder;
use Drupal\elasticentityquery\Query;
use Drupal\elasticentityquery\QueryAggregate;
use Drupal\Core\Entity\EntityType;

/**
 * Utility funcctions for working with elasticseach
 */
class Elastic {

  public static function client($config =[]) {
    static $client; 
    if (!empty($client)) {
      return $client;
    }

    $builder = ClientBuilder::create();

    if (!empty($config['hosts'])) {
      $builder->setHosts($config['hosts']);
    }
    $builder->setSelector('\Elasticsearch\ConnectionPool\Selectors\StickyRoundRobinSelector');
    $client = $builder->build();
    return $client;
  }

  public static function IndexQuery($index, $conjunction = 'AND', array $config = []) {
    $client = Elastic::client($config);
    $entity_type = new EntityType(['id' => $index]);
    $namespaces = ['Drupal\Core\Entity\Query\Sql'];
    $query = new Query($entity_type, $conjunction, $client, $namespaces);
    return $query;
  }

  public static function IndexQueryAggregate($index, $conjunction = 'AND', array $config = []) {
    $client = Elastic::client($config);
    $entity_type = new EntityType(['id' => $index]);
    $namespaces = ['Drupal\Core\Entity\Query\Sql'];
    $query = new QueryAggregate($entity_type, $conjunction, $client, $namespaces);
    return $query;
  }
}