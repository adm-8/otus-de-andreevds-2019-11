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

И понимаем, что нам вполне подойдет машина и Ubuntu 18.04 LTS, описанные в инструкции к ДЗ выше по списку:
https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_hardware_requirements.html#concept_vvv_cxt_gbb
https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_os_requirements.html#os_requirements

Да и наша VPSка тоже вполне подходит: https://docs.cloudera.com/documentation/enterprise/6/release-notes/topics/rg_database_requirements.html#cdh_cm_supported_db

### Поднимаем\ настраиваем виртуалку на GCP в соответствии с требованиями
Настройки для виртуалки используем из инструкции к ДЗ. Для простоты будем использовать браузерную консоль. Во всяком случае до тех пор, пока нам этого будет достаточно. 

![OS Installed](https://github.com/adm-8/otus-de-andreevds-2019-11/raw/master/HW3_Lesson3/pics/OS_Intalled.jpg)

### требования JAVA

```
Only 64 bit JDKs are supported. Cloudera Manager 6 and CDH 6 do not support JDK 7. Although JDK 7 is supported on all versions of CDH 5, a CDH 5.x cluster that is managed by Cloudera Manager 6.x must use JDK 8 on all cluster hosts. Oracle JDK 8 is supported in Cloudera Manager 6 and CDH 6. JDK 8 is also supported in CDH 5.3 and higher.

OpenJDK 8 is supported in Cloudera Enterprise 6.1.0 and higher, as well as Cloudera Enterprise 5.16.1 and higher. For installation and migration instructions, see Upgrading the JDK.
```

Идём и устанавливаем всё необходимое по [мануалу](https://docs.cloudera.com/documentation/enterprise/upgrade/topics/ug_jdk8.html) :
```
sudo apt-get update
sudo apt-get install openjdk-8-jdk
java -version
```

И видим, что встала нужная версия. В моем случе это :
```
openjdk version "1.8.0_222"
OpenJDK Runtime Environment (build 1.8.0_222-8u222-b10-1ubuntu1~18.04.1-b10)
OpenJDK 64-Bit Server VM (build 25.222-b10, mixed mode)
```

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
* Для корректной работы Cloudera Manager, Cloudera Navigator, and Hue вы должны юзать свежие браузеры с включенными куками и JS.

ну и казалось бы всё, можно перевести дух и приступать к 

## Установка Cloudera Manager, CDH, and Managed Services
### Шаг 1. Конфигурация репозитория
https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/install_cm_cdh.html

```
cd /etc/apt/sources.list.d/
sudo wget https://archive.cloudera.com/cm6/6.3.1/ubuntu1804/apt/cloudera-manager.list

sudo wget https://archive.cloudera.com/cm6/6.3.0/ubuntu1604/apt/archive.key
sudo apt-key add archive.key

sudo apt-get update
```
### Шаг 2. Установка JDK
необходимые действия мы сделали ещё на жтапе проверки требований

### Шаг 3. Установка Cloudera Manager Server
```
sudo apt-get install cloudera-manager-daemons cloudera-manager-agent cloudera-manager-server
```

### Шаг 4. Установка баз данных
Т.к. в моем случае уже есть VPS с PostgreSQL, использую https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/cm_ig_extrnl_pstgrs.html

#### Installing the psycopg2 Python Package
```
sudo apt-get install python-pip
sudo pip install psycopg2==2.7.5 --ignore-installed
```

#### Configuring and Starting the PostgreSQL Server
Идём на тачку где поднят PostgreSQL, стартуем базу если она не поднята:
```
sudo service postgresql start
```
*база должна слушать (для простоты) все айпишники и должен быть включена аутентификация MD5*

Далее надо сконфигурить PostgreSQL, в нашем случае настраивать будем как для "Small to mid-sized clusters". Открываем конфиг, в моем случае он лежит в:
```
/etc/postgresql/9.5/main/postgresql.conf
```

Нужно выставить:

max_connection = 100
shared_buffers = 256MB
wal_buffers = 8MB
max_wal_size = 786	#(3 * checkpoint_segments) * 16MB
checkpoint_completion_target = 0.9
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
т.к. наше версия PostgreSQL gt 8.4, нам необходимо изменить настройки некоторых баз:
```
ALTER DATABASE metastore SET standard_conforming_strings=off;
ALTER DATABASE oozie SET standard_conforming_strings=off;
```









