# Настройка dbt с использованием BigQuery в Docker

Это быстрое руководство по настройке dbt с использованием BigQuery в Docker.

**Примечание:** Вам понадобится ваш ключевой файл аутентификации JSON для этого метода работы. Вы также можете использовать OAuth.

1. Создайте каталог с выбранным вами именем.
    ```
    mkdir <dir-name>
    ```
2. Перейдите в каталог.
    ```
    cd <dir-name>
    ```
3. Скопируйте этот [Dockerfile](Dockerfile) в ваш каталог из официального git-репозитория dbt [здесь](https://github.com/dbt-labs/dbt-core/blob/main/docker/Dockerfile).
4. Создайте файл `docker-compose.yaml` [здесь](docker-compose.yaml).
    ```yaml
    version: '3'
      services:
        dbt-bq-dtc:
          build:
            context: .
            target: dbt-bigquery
          image: dbt/bigquery
          volumes:
            - .:/usr/app
            - ~/.dbt/:/root/.dbt/
            - ~/.google/credentials/google_credentials.json:/.google/credentials/google_credentials.json
          network_mode: host
    ```
    - Назовите сервис по своему усмотрению или `dbt-bq-dtc`.
    - Используйте `Dockerfile` в текущем каталоге для создания образа, передавая `.` в качестве контекста.
    - `target` указывает, что мы хотим установить плагин `dbt-bigquery` в дополнение к `dbt-core`.
    - Примонтируйте 3 тома -
        - для сохранения данных dbt
        - путь к `profiles.yml` dbt
        - путь к файлу `google_credentials.json`, который должен находиться в пути `~/.google/credentials/`

5. Создайте файл `profiles.yml` в `~/.dbt/` на вашем локальном компьютере или добавьте следующий код в ваш существующий файл `profiles.yml` - 
    ```yaml
    bq-dbt-workshop:
      outputs:
        dev:
          dataset: <bigquery-dataset>
          fixed_retries: 1
          keyfile: /.google/credentials/google_credentials.json
          location: EU
          method: service-account
          priority: interactive
          project: <gcp-project-id>
          threads: 4
          timeout_seconds: 300
          type: bigquery
      target: dev
    ```
    - Назовите профиль. В моем случае `bq-dbt-workshop`. Он будет использоваться в файле `dbt_project.yml` для ссылки и инициализации dbt.
    - Замените значения `dataset`, `location` (мой бакет GCS находится в регионе `EU`, измените на `US`, если нужно), `project`.
6. Выполните следующие команды -
  - ```bash 
    docker compose build 
    ```
  - ```bash 
    docker compose run dbt-bq-dtc init
    ``` 
    - **Примечание:** В основном мы запускаем `dbt init` выше, потому что `ENTRYPOINT` в [Dockerfile](Dockerfile) является `['dbt']`.
    - Введите необходимые значения. Имя проекта будет `taxi_rides_ny`
    - Это создаст `dbt/taxi_rides_ny/`, и вы увидите там `dbt_project.yml`.
    - В `dbt_project.yml` замените `profile: 'taxi_rides_ny'` на `profile: 'bq-dbt-workshop'`, так как у нас есть профиль с последним именем в нашем `profiles.yml`
  - ```bash
    docker compose run --workdir="//usr/app/dbt/taxi_rides_ny" dbt-bq-dtc debug
     ``` 
    - для тестирования подключения. Это должно вывести `All checks passed!` в конце.
    - **Примечание:** Автоматическое преобразование пути в Git Bash приведет к сбоям команд с флагом `--workdir`. Это можно исправить, префиксируя путь `//`, как показано выше. Решение было найдено [здесь](https://github.com/docker/cli/issues/2204#issuecomment-638993192).
    - Также мы изменяем рабочий каталог на dbt-проект, потому что файл `dbt_project.yml` должен быть в текущем каталоге. Иначе он выбросит `1 check failed: Could not load dbt_project.yml`
