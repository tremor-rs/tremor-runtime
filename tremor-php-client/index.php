<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Titel</title>
  </head>
  <body>
<?php
       require_once "vendor/autoload.php";
$url = 'http://127.0.0.1:80/put.php'; //$_GET['url']

$class = new \Wayfair\Tremor\Client($url, ['event_encoding' => 'json']);
echo $class->publish(['key' => 42], ['parent_uuid' => \Ramsey\Uuid\Uuid::uuid1() ]);
?>

  </body>
</html>
