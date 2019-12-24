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

## Некоторые замечания по ДЗ
* Прям очень важный момент! Когда импортим проект в IDE после создания через "sbt new MrPowers/spark-sbt.g8", необходиимо указывать путь именно к файлу build.sbt, а не к папке с проектом. Иначе она не чухает, что это SBTшный проект и ты испытываешь боль.
* Под Windows 10 постоянно валятся ошибки как на скрине, а именно невозможность удаления файлов из C:\Users\ADM8\AppData\Local\Temp\* пробовал запускать CMD под аднимном - не помогает, пробовал качать какую-то утилиту типа расширенного CMD - тоже не помогает. В итоге заблик т.к. решил, что это всего лишь очистка временных файлов. Вроде и без этого всё взлетело.
* В самих данных, в полях id, paonts и price, в некоторых строках лежат null значения которое после получения из JSON преобразуется в троку { }, поэтому приведение к типу Int и Float в кейс-классе не взлетело, пришлось добавлять Option в типы переменных в Case Class'e и писать кастомный метод, который печатает содержимое объекта без Some(...)*