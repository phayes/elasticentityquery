# elasticsearch backend for EntityQuery

[![Build Status](https://scrutinizer-ci.com/g/phayes/elasticentityquery/badges/build.png?b=8.x-1.x)](https://scrutinizer-ci.com/g/phayes/elasticentityquery/build-status/8.x-1.x)
[![Code Coverage](https://scrutinizer-ci.com/g/phayes/elasticentityquery/badges/coverage.png?b=8.x-1.x)](https://scrutinizer-ci.com/g/phayes/elasticentityquery/?branch=8.x-1.x)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/phayes/elasticentityquery/badges/quality-score.png?b=8.x-1.x)](https://scrutinizer-ci.com/g/phayes/elasticentityquery/?branch=8.x-1.x)

Fully implements the EntityQuery interface using elasticsearch. Anything you can do using the regular drupal EntityQuery can be done in elasticsearch. 

Example:

```php
use Drupal\elasticentityquery\Elastic;

// Regular queries
$results = Elastic::IndexQuery('my_elastic_index');
  ->condition('tags', 'Lorem')
  ->condition('eyeColor', 'blue')
  ->execute();

// Count Queries
$count = Elastic::IndexQuery('my_elastic_index');
  ->condition('tags', 'Lorem')
  ->count()
  ->execute();

// Compound Queries
$query = Elastic::IndexQuery('my_elastic_index');
  ->condition('tags', 'dolore');
$group = $query->orConditionGroup()
  ->condition('eyeColor', 'green')
  ->condition('eyeColor', 'blue');
$results = $query->condition($group)->execute();

// Sorting
$results = Elastic::IndexQuery('my_elastic_index');
  ->sort('age', 'asc')
  ->execute();

// Aggregate Queries
$result = Elastic::IndexQueryAggregate('my_elastic_index')
  ->groupBy('eyeColor')
  ->aggregate('age', 'avg')
  ->execute();
  
// OR queries
$result = Elastic::IndexQuery('my_elastic_index', 'OR')
  ->condition('eyeColor', 'green')
  ->condition('eyeColor', 'blue')
  ->execute();

// Specify connection information for elasticsearch
$results = Elastic::IndexQuery('my_elastic_index', 'AND', ['hosts' => ['1.2.3.4:9200','1.2.3.5:9200']]);
  ->condition('tags', 'Lorem')
  ->condition('eyeColor', 'blue')
  ->execute();

```
