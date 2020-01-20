# HW5 - Spark - Гид по безопасному Бостону

Цель: В этом задании предлагается собрать статистику по криминогенной обстановке в разных районах Бостона, используя Apache Spark.
https://docs.google.com/document/d/1elWInbWsLrIDqB4FMMgFTUMNEmiYev9HJUfg9LXxydE/edit?usp=sharing
Критерии оценки: Программа выдает корректный файл на выходе - 3 балла
Здание сдано в срок - 1 балл
Задание сдано с первой попытки (повторные попытки из-за неточностей в условиях задания не считаются) - 1 балл
Рекомендуем сдать до: 14.01.2020


## Создание проекта

Запускаем создание проекта:
```
e:

cd E:\_Files\__personal\_git\otus-de-andreevds-2019-11\HW5_Lesson7

sbt new MrPowers/spark-sbt.g8

```

Указываем имя проекта:
```
BostonCrimesMap
```

Указываем имя пакета:
```
com.example
```

Импортим проект в IDE, указав **полный путь к build.sbt**
```
E:\_Files\__personal\_git\otus-de-andreevds-2019-11\HW5_Lesson7\bostoncrimesmap\build.sbt
```


На время разработки комментим % "provided" в build.sbt
```
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" // % "provided"
```

Создаем новый объект в main'e и именем:
```
BostonCrimesMap
```

И погнали кодить =) После того как завершили кодить, пошли собирать проект:
```
e:

E:\>cd E:\_Files\__personal\_git\otus-de-andreevds-2019-11\HW5_Lesson7\bostoncrimesmap

sbt assembly
```

После того как проект собрался, можем пойти запустить его:
```
E:\app\spark-2.4.4-bin-hadoop2.7\bin\spark-submit --master local[*] --class com.example.BostonCrimesMap E:\_Files\__personal\_git\otus-de-andreevds-2019-11\HW5_Lesson7\bostoncrimesmap\target\scala-2.11\BostonCrimesMap-assembly-0.0.1.jar C:\temp\crimes-in-boston\crime.csv C:\temp\crimes-in-boston\offense_codes.csv C:\temp\crimes-in-boston\result

```

## Результат

![result](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW5_Lesson7/_images/result.JPG)

## Что стоит запомнить из этого ДЗ
* Крайне **полезный ресурс**, на котором можно посмотреть с какими версиями scala работает та или иная версия какого-то пакета: https://mvnrepository.com/artifact
* sbt-assembly 0.14.6 вполне себе взлетает с версией скалы 2.11.12
* Для версии Spark 2.4.x при импорте проекта надо явно указать версию JDK 1.8, иначе всплывают проблемы при джоинах
* Чтобы автоматом подцеплялись имена колонок из файлов во время read.csv необходимо либо добавлять .options(Map("inferSchema"->"true","delimiter"->",","header"->"true")), либо создавать кастомную схему (что предпочтительнее ибо может избавить от проблем при автоматическом определении типов данных в колонках)
* Для типизации резульатов запроса используем case class, чтобы потом можно было пользоваться groupByKey , flatMapGroups и т.д.
* Все колонки которые планируем использовать в .agg(count(), avg(), ...) - должны быть заранее выбраны в .select(...)

## Полезные ссылочки:
https://sparkbyexamples.com/spark/spark-read-csv-file-into-dataframe/ - про чтение CSV и переметры
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-SparkSession.html - функционал и примеры по SparkSession
https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/SparkSession.html - SparkSession
https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html - про Window Functions
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-windows.htm - про Window Functions
https://www.mikulskibartosz.name/row-number-in-apache-spark-window-row-number-rank-and-dense-rank/ - row_number, rank, and dense_rank
https://blog.codecentric.de/en/2016/07/spark-2-0-datasets-case-classes/ - 

## OPTIONAL TODO:
* не везде использовать broadcast в джоинах, а только в джоине фреймов-источников
* переписать кусок с Widnow-function LAG на какой-нибудь другой метод с использованием magGroups + функция которая будет обрабатывать строки, формировать массив, а массив уже конвертить в DF (эт только предположение, понятия не имею взлетит ли такое, потому и ипользовал LAG =) 
* попрактиковаться с groupByKey , flatMapGroups и т.д. ибо, мягко говоря, не очень понятно как это всё работает =)
