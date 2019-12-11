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



