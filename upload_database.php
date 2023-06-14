<!doctype html>
<html>
<head>
<meta charset = "UTF-8">
<title>Обновление базы данных с вакансиями</title>
</head>
<body>
<?php 
$pyout = exec(‘python create_db.py’);
echo $pyout;
echo “\n”;
?>
<body>