# HW4 - Введение в Scala

Цель: Написать распределенное приложение для чтения JSON-файлов.
https://docs.google.com/document/d/1HPhwYCQ1AYklBeQ75RVsYV4MMGBEw_UBeTWp0c0Q678/edit?usp=sharing

Критерии оценки: Результат необходимо прислать в виде ссылки на git-репозиторий.

Если приложение запускается и распечатывает кейс-классы с данными из файла - 4 балла

Если задание сдано в срок - 1 балл

Рекомендуем сдать до: 22.12.2019


## Полезные ссылки
Установка Apache Spark на Windows:

https://www.ics.uci.edu/~shantas/Install_Spark_on_Windows10.pdf

По фактку пришлось пошаманить с версиями, но в целом можно поднять всё по этой доке

## Результат
**Обратите внимание, что полный "путь" к классу получился не таким как в описании к ДЗ**


*Полное имя класса:*
```
com.github.adm8.de.hw4.JsonReader
```

*Путь к spark:*
```
E:\app\spark-2.4.4-bin-hadoop2.7\bin\
```

*Путь к JARнику:*
```
E:\_Files\__personal\_git\otus-de-andreevds-2019-11\HW4_Lesson5\json_reader_andreev\target\scala-2.11\json_reader_andreev_2.11-0.0.1.jar
```

*Путь к JSON-файлу:*
```
C:\temp\winemag-data-130k-v2.json
```

*Итоговая команда для запуска:*
```
E:\app\spark-2.4.4-bin-hadoop2.7\bin\spark-submit --master local[*] --class com.github.adm8.de.hw4.JsonReader E:\_Files\__personal\_git\otus-de-andreevds-2019-11\HW4_Lesson5\json_reader_andreev\target\scala-2.11\json_reader_andreev_2.11-0.0.1.jar C:\temp\winemag-data-130k-v2.json
```

![WorkDoneAndError](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW4_Lesson5/_images/WorkDoneAndError.JPG)
