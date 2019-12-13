# HW3 - Развернуть дистрибутив Cloudera

#### Цель
Цель этого ДЗ - научиться выполнять базовые операции на кластере Hadoop. В его ходе нужно будет развернуть свой мини-кластер в Google Cloud Platform и создать таблицу в Hive.

#### Инструкции к ДЗ
https://docs.google.com/document/d/1iLTiN7D1kM4njOEbF_f_YGT7Jj7OnvT_WjbU3rX6Hzw/edit?usp=sharing

#### Критерии оценки:
* Развернут кластер - 2 балла (в подтверждение - скинуть в чат скриншот главной страницы Cloudera Manager)
* Создана таблица в Hive - 2 балла (в подтверждение - скинуть скриншот результата SELECT-запроса к этой таблице)
* Задание сдано в срок (Рекомендуем сдать до: 15.12.2019) - 1 балл
* Минимальное количество баллов для сдачи задания - 3


![ClouderaManagerHome](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/ClouderaManagerHome.JPG)

![FirstSelectOk](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/FirstSelectOk.JPG)

### Опиcание решения
Было принято решение не использовать Docker, а поднять Cloudera руками с нуля т.к. 
* в ходе занятия стало ясно, что Cloudera, развернутая через Docker, работает так себе 
* считаю, что в начале того или иного пути необходимо хотя бы раз в жизни испытывать боли, связанные с поднятиями и связками разнообразных сиситем. 

### Что имеем 
* Учётку для Google Cloud с 300 баками на тестовый период
* Полумертвый VPS с установленнми на нем Ubuntu 16.04 + PostgreSQL 9.5

## Что делаем 
### Начинаем знакомиться с требованиями 
https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_requirements_supported_versions.html

И понимаем, что нам вполне подойдет машина с CentOS7:
https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_hardware_requirements.html#concept_vvv_cxt_gbb
https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_os_requirements.html#os_requirements

Да и наша VPSка тоже вполне подходит: https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_database_requirements.html#cdh_cm_supported_db

### Поднимаем\ настраиваем виртуалку на GCP в соответствии с требованиями
Создаем вируртуалку на GCP с CentOS7, берем 4 проца, 15 ОЗУ + 50гб Диска

![OS Installed](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/OS_Intalled.jpg)

### Требования JAVA

```
Only 64 bit JDKs are supported. Cloudera Manager 6 and CDH 6 do not support JDK 7. Although JDK 7 is supported on all versions of CDH 5, a CDH 5.x cluster that is managed by Cloudera Manager 6.x must use JDK 8 on all cluster hosts. Oracle JDK 8 is supported in Cloudera Manager 6 and CDH 6. JDK 8 is also supported in CDH 5.3 and higher.

OpenJDK 8 is supported in Cloudera Enterprise 6.1.0 and higher, as well as Cloudera Enterprise 5.16.1 and higher. For installation and migration instructions, see Upgrading the JDK.
```
Всё необходимое по JAVA мы будем ставить на втором шаге

### Требования безопасности и сети
https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_network_and_security_requirements.html#concept_o3g_kvl_rcb

Для простоты откроем все порты на виртуалке. **В боевых условиях ни в коем случае так делать нельзя!**

https://cloud.google.com/vpc/docs/using-firewalls

У меня получилось как-то так:
![Network Allow](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/NetworkAllow.JPG)


### Требования по шифрованию
https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_encryption_requirements.html#data_at_rest_encryption_reqs

для начала нам предлагают проверить, судя по всему, некий уровень доступности энтропии, N раз выполнив команду ниже. И если будут значения меньше 500, то надо будет подтюнить шарманку.
```
cat /proc/sys/kernel/random/entropy_avail
```
У меня всё ок:

![EntropyAvail](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/EntropyAvail.jpg)

Далее нам перечисляют список необхоидимых портов, но они у нас все открыты, так что все должно быть ок.

Кроме того, советуют использовать Transport Layer Security (TLS) сертификаты для обеспечения безопасности. Нас это сейчас не интересует, поэтому скипнем эту часть.

### Прочие требования
Для корректной работы Cloudera Manager, Cloudera Navigator, and Hue вы должны юзать свежие браузеры с включенными куками и JS.

ну и казалось бы всё, можно перевести дух и приступать к 

## Установка Cloudera Manager, CDH, and Managed Services
### Шаг 1. Конфигурация репозитория
https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/configure_cm_repo.html
```
sudo yum install wget

sudo wget https://archive.cloudera.com/cm6/6.3.1/redhat7/yum/cloudera-manager.repo -P /etc/yum.repos.d/

sudo rpm --import https://archive.cloudera.com/cm6/6.3.0/redhat7/yum/RPM-GPG-KEY-cloudera

sudo yum update

```

### Шаг 2. Установка  Oracle JDK 
https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/cdh_ig_jdk_installation.html#topic_29

```
sudo yum install oracle-j2sdk1.8

```


### Шаг 3. Установка Cloudera Manager Server
https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/install_cm_server.html
```
sudo yum install cloudera-manager-daemons cloudera-manager-agent cloudera-manager-server

```

### Шаг 4. Установка баз данных
Т.к. в моем случае уже есть VPS с PostgreSQL, использую https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/cm_ig_extrnl_pstgrs.html

#### Installing the psycopg2 Python Package
https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/cm_ig_extrnl_pstgrs.html#cmig_topic_5_6
```
sudo yum install python-pip

sudo pip install psycopg2==2.7.5 --ignore-installed

```

#### Configuring and Starting the PostgreSQL Server
Идём на тачку где поднят PostgreSQL, стартуем базу если она не поднята:
```
sudo service postgresql start
```
*база должна слушать (для простоты) все айпишники и должна быть включена аутентификация MD5*

Далее надо сконфигурить PostgreSQL, в нашем случае настраивать будем как для "Small to mid-sized clusters". Открываем конфиг, в моем случае он лежит в:
```
/etc/postgresql/9.5/main/postgresql.conf
```

Нужно выставить:

* max_connection = 100
* shared_buffers = 256MB
* wal_buffers = 8MB
* max_wal_size = 786	#(3 * checkpoint_segments) * 16MB
* checkpoint_completion_target = 0.9
* в мануале ещё есть вот этот парамметр - checkpoint_segments = 16  , но у меня его не было. После его добавления БД не поднималась. Видать в 9.5 нет такого*

**В идеале бы разботать, что означает каждый из этого параметра, но это оставим на потом.**

Перезагружаем:
```
sudo service postgresql restart
```

Далее необходимо создать кучку юзеров и баз для разных софтин, входящих в состав Cloudera. Для этого коннектимся к базе:
```
sudo -u postgres psql
```

**НЕ делать для первой установки**
*Мне это пригодилось т.к. первый раз накатывал на Ubuntu18 и у меня не взлетело и греха подальше сносил базы, созданные при попытке поднять все это добро в предыдущий раз.* 
```
DROP DATABASE scm;
DROP DATABASE amon;
DROP DATABASE rman;
DROP DATABASE hue;
DROP DATABASE metastore;
DROP DATABASE sentry;
DROP DATABASE nav;
DROP DATABASE navms; 
DROP DATABASE oozie;

```
**а вот это уже делать:**
Создаем юзеров:
```
CREATE ROLE scm LOGIN PASSWORD 'scm';
CREATE ROLE amon LOGIN PASSWORD 'amon';
CREATE ROLE rman LOGIN PASSWORD 'rman';
CREATE ROLE hue LOGIN PASSWORD 'hue';
CREATE ROLE hive LOGIN PASSWORD 'hive';
CREATE ROLE sentry LOGIN PASSWORD 'sentry';
CREATE ROLE nav LOGIN PASSWORD 'nav';
CREATE ROLE navms LOGIN PASSWORD 'navms';
CREATE ROLE oozie LOGIN PASSWORD 'oozie';

```
Создаем базы:
```
CREATE DATABASE scm OWNER scm ENCODING 'UTF8';
CREATE DATABASE amon OWNER amon ENCODING 'UTF8';
CREATE DATABASE rman OWNER rman ENCODING 'UTF8';
CREATE DATABASE hue OWNER hue ENCODING 'UTF8';
CREATE DATABASE metastore OWNER hive ENCODING 'UTF8';
CREATE DATABASE sentry OWNER sentry ENCODING 'UTF8';
CREATE DATABASE nav OWNER nav ENCODING 'UTF8';
CREATE DATABASE navms OWNER navms ENCODING 'UTF8';
CREATE DATABASE oozie OWNER oozie ENCODING 'UTF8';

```
т.к. наша версия PostgreSQL gt 8.4, нам необходимо изменить настройки некоторых баз:
```
ALTER DATABASE metastore SET standard_conforming_strings=off;
ALTER DATABASE oozie SET standard_conforming_strings=off;

```


### Шаг 5. Настройка Cloudera Manager Database

Для настройки базы Cloudera Manager достаточно выполнить заранее подготовленный скрипт:
```
sudo /opt/cloudera/cm/schema/scm_prepare_database.sh postgresql scm scm scm -h [HOST]
```
* где [HOST] - адрес сервака где крутится PostgrSQL

### Шаг 6. Install CDH and Other Software.

Стартуем Clouder'у
```
sudo systemctl start cloudera-scm-server
```

И читаем логи, запустив команду:
```
sudo tail -f /var/log/cloudera-scm-server/cloudera-scm-server.log
```

Ждем пока появится надпись ниже. Мне её пришлось ждать минут 5-10:
```
INFO WebServerImpl:com.cloudera.server.cmf.WebServerImpl: Started Jetty server.
```

Если не появляется - идём выяснить почему: https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/cm_ig_troubleshooting.html#cmig_topic_19
Ежели всё хорошо, открываем браузер и идём в веб морду Cloudera Manager:
```
http://[IP]:7180
```
* где [IP] - внешний адрес вашей машины GCP

Запускается визард. Нам необходимо пройти немного шагов:

##### Cluster Basics
Даем имя кластеру. По мне так "HW3_Cluster" будет вполне себе ок.

##### Specify Hosts
======================

А вот тут то я и поплыл. Т.к. о SHH ключиках я заранее не позаботился. Да и как их создавать \ тестировать в GCP - поянтия не имею. Долго тыкрался-мыркался ещё на Ubunu, ничего у меня не получалось т.к. Cloudera Manager\Сервак наотрез отказывался видеть\использовать SSH ключ. По итогу я вообще не понимаю как (а сейчас внимание будет спойлер) у меня всё же это взлетело. Ибо то, что будет описано ниже больше на какую-то магию. С другой стороны, куда без неё в нашем деле =) 

======================

Я указал hostname (FQDN) =  "localhost", оставил порт SHH = 22 и жмахнул "Search". В моем случае появилась одна запись, как я и ожидал. Жмахнул далее. 

#####  Accept JDK License
В обязательном порядке читаем лицензионное соглашение. (Нет).
Т.к. мы поставили JDK в одном из пунктов ранее, НЕ ставим галку и идём дальше.


#####  ??? уже не помню как этот пункт назывался. 
Спрашивали про логины\пароли или же SSH ключи, которые я не подготовил. Долго вытался генерить ключи, но так ничего и не взлетало.

### тут произошла магия
Я обратил внимание, что если уйти назад на шаг выбора хостов (уже после неуспешных попыток законнектиться к localhost по SSH) появляется вкладка ... не помню уже точно как называется ... "Уже преднастроенные хосты". открываем вкладку, там есть наш localhost, выбираем его, жмахаем далее. 

##### Select Repository
Тут уже решил ничего не трогать и оставить как выбрано по дефолту.

##### Install Parcels 
Шарманка что-то ставила-ставила и наконец поставила!

##### Inspect Cluster
На данном шаге мне предложили обследовать кластер. Результат был таковым:

![Inspect Cluster](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/InspectNetworkPerf_error.JPG)

Я выбрал радиобаттон в котором написано, что я понимаю риски и го дальше (хотя я конечно же ничего к этому моменту уже не понимал).

## Cluster Configuration
Далее пошёл процесс настройки кластера

##### Select Services 
Я выбрал пункт Data Engineering. Мы же вроде как на них учимся. В его состав входит:
HDFS, YARN (MapReduce 2 Included), ZooKeeper, Oozie, Hive, Hue, and Spark.

##### Assign Roles
Т.к. ни о каких ролях в разрезе хадупа я не слышал, решил ничего на этом экране не трогать. Ну его. 
По итогу оказалось, что всё норм. Хотя конечно разобраться что за роли - однозначно надо.

##### Setup Databases
Тут нам предлагают вбить данные для БД. Вбили, делаем Test Connection и всё прекрасно:
![DB OK](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/db_test_ok.JPG)

Идём дальше. Смотрим изменения, жмахаем далее и попадаем на 

##### Summary
![Summary OK](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/SummaryOk.JPG)

Уже не плохо, но это ещё не конец ДЗ.

### Cloudera Home Page
![ClouderaManagerHome](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/ClouderaManagerHome.JPG)

### Hue Creating Account
![HueCreatingAccount](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/HueCreatingAccount.JPG)

### Hue Home Page
![HueHome](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/HueHome.JPG)

### Creating Table
![CreateTableSuccess](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/CreateTableSuccess.JPG)

### First Select
![FirstSelectOk](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/FirstSelectOk.JPG)


## Заключение

![Result](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/ok_result.jpg)






