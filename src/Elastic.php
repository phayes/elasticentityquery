<?php

namespace Drupal\elasticentityquery;

use Elasticsearch\ClientBuilder;

/**
 * Utility funcctions for working with elasticseach
 */
class Elastic {

  static function client($config =[]) {
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
}