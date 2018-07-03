<!DOCTYPE html>
<html lang="en">
          <head>
               <meta charset="utf-8">
                            <title>Titel</title>
                                 </head>
                                       <body>
PUT:
<?php
    $putdata = fopen("php://input","r");

$data = fread($putdata,1024);
echo $data;

?>
</body>
</html>
