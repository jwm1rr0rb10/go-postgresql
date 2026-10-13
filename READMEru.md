испра# PSQL
**Готовый к использованию в продакшене клиент PostgreSQL с повторными попытками, проверками работоспособности и оптимизацией для высоконагруженных сервисов Go**

`psql` — это легковесная оболочка для [`github.com/jackc/pgx/v5/pgxpool`](https://github.com/jackc/pgx), которая предоставляет чистый, настраиваемый и высоконадежный клиент PostgreSQL.

Он включает в себя интеллектуальное объединение соединений, автоматические повторные попытки с учетом дрожания, двойной мониторинг работоспособности, централизованную обработку ошибок и настройки по умолчанию, соответствующие продакшену — все, что вам нужно для высоконагруженных бэкендов без лишнего шаблонного кода.


Поддерживается:
- Высокопроизводительный пул соединений с разумными настройками по умолчанию
- Логика повторных попыток с экспоненциальной задержкой + дрожанием
- Двойная проверка работоспособности (универсальный `HealthChecker` + метрики в стиле Prometheus)
- Централизованная обработка ошибок PostgreSQL и вспомогательные функции
- Корректное завершение работы
- Красивое логирование SQL
- Безопасный построитель DSN
- Бинарный режим выполнения запросов

Идеально подходит для REST API, gRPC-сервисов, микросервисов или любого бэкэнда, которому необходим стабильный и наблюдаемый доступ к PostgreSQL.

## Особенности
- Конфигурация без **шаблонов** благодаря мощному шаблону `Option`
- Оптимизированные для производственной среды настройки по умолчанию (20/0 соединений, время жизни 1 час с jitter и т. д.)
- Автоматические повторные попытки при сбоях соединения с учетом дрожания
- Двухуровневый мониторинг состояния (пользовательский интерфейс + метрики)
- Централизованные обработчики ошибок (`ErrCommit`, `ErrDoQuery` и т. д.)
- Встроенная уникальная система обнаружения нарушений и анализа ошибок
- Утилита `PrettySQL` для чистого логирования запросов
- Безопасное построение DSN (правильное экранирование)
- Потокобезопасный и контекстно-зависимый интерфейс
- Полная совместимость с `pgxpool.Pool` (встроенная)
- 100% англоязычная документация godoc

## Installation
```bash
go get github.com/jwm1rr0rb10/go-postgresql
```

---

The library depends on:

- github.com/jackc/pgx/v5
- github.com/prometheus/client_golang
- go.opentelemetry.io/otel

After adding the module, run:
```bash
go mod tidy
```

---

## Quick Start
### 1. Basic usage (recommended)
```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	psql "github.com/jwm1rr0rb10/go-postgresql"
)

func main() {
	ctx := context.Background()

	// Create config with options
	cfg, err := psql.NewConfig("postgres://user:pass@localhost:5432/mydb?sslmode=disable",
		psql.WithMaxAttempts(7),
		psql.WithQueryExecMode(pgx.QueryExecModeCacheDescribe), // only behind PgBouncer
		psql.WithConnectionPool(100, 20, 2*time.Hour),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create client
	client, err := psql.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// You can use client exactly like *pgxpool.Pool
	rows, err := client.Query(ctx, "SELECT id, name FROM users WHERE status = $1", "active")
	// ... your normal pgx code
}
```

### 2. Using DSN builder

```go
dsnCfg := &psql.DSNConfig{
	Username: "user",
	Password: "super@secret:pass",
	Host:     "db.example.com",
	Port:     "5432",
	Database: "mydb",
}
dsn := dsnCfg.ToPostgresDSN()

cfg, _ := psql.NewConfig(dsn)
```

### 3. Health checking + metrics
```go
// Example with custom health checker
type MyHealth struct{ /* ... */ }
func (h *MyHealth) SetStatus(name string, status bool, details interface{}) { /* ... */ }

cfg, _ := psql.NewConfig(dsn,
	psql.WithHealthChecker("postgres-main", &MyHealth{}),
	psql.WithIntervalCheck(15*time.Second),
	psql.WithFailureThreshold(3), // неудачных пингов подряд до статуса "unhealthy"
)
```

Клиент становится unhealthy после `WithFailureThreshold` неудачных пингов подряд
(по умолчанию 3) и восстанавливается после первого успешного. Пинг, не
дождавшийся соединения из полностью занятого пула, статус не меняет: перегрузка —
не авария, и иначе `Cluster` выкидывал бы реплики, а readiness-проба падала бы
именно на пике нагрузки. Пинги не попадают в трейсы и метрики латентности.

---

## Основные концепции
### Шаблон «Конфигурация + Параметры»
Вся конфигурация выполняется через `NewConfig(dsn, options...)`.
Вы можете переопределить количество повторных попыток, настройки пула, проверки работоспособности, режим выполнения запросов и т. д.

### HealthChecker interface

```go
type HealthChecker interface {
	SetStatus(dependencyName string, status bool, details interface{})
}
```

Используется библиотекой для составления справочных отчетов о состоянии здоровья.

### Error handling
```go
if pgErr, ok := psql.IsErrUniqueViolation(err); ok {
	// handle unique violation
}

err = psql.ParsePgError(err) // nice formatted error
```

### PrettySQL

```go
clean := psql.PrettySQL(`
    SELECT * FROM users 
    WHERE id = $1
`)
```

---

## API Overview
### Main functions & types

| Function / Type            | Description                                                             |
|:---------------------------|:------------------------------------------------------------------------|
| "NewConfig(dsn, opts...)"  | Анализ DSN и применение параметров                                      |
| "NewClient(ctx, cfg)"      | Создайте готовый к использованию пул соединений.                        |
| (*Client).Close()          | Плавное завершение работы                                               |
| WithMaxAttempts            | Установить повторные попытки                                            |
| WithMaxDelay               | Установите максимальную задержку отката.                                |
| WithQueryExecMode          | Режим выполнения запросов pgx (CacheDescribe/Exec — для PgBouncer)      |
| WithHealthChecker          | Прикрепить пользовательский отчет о состоянии здоровья                  |
| WithIntervalCheck          | Установить интервал проверки работоспособности                          |
| WithFailureThreshold       | Число неудачных пингов подряд до статуса unhealthy                      |
| WithRegisterer             | Свой реестр Prometheus (nil отключает метрики)                          |
| WithMaxReplicationLag      | Учитывать отставание реплики при маршрутизации в Cluster                |
| WithConnectionPool         | Настройте размер пула и время жизни пула.                               |
| "ErrCommit, ErrDoQuery..." | Обертки ошибок с контекстом                                             |
| IsErrUniqueViolation       | Проверьте уникальность нарушения                                        |
| ParsePgError               | Подробности ошибки PostgreSQL; *PgError остаётся в цепочке              |
| Config.RedactedDSN()       | DSN с замаскированным паролем, безопасно для логов                      |
| PrettySQL                  | Свернуть пробелы для логов                                              |
| DSNConfig.ToPostgresDSN()  | Безопасный конструктор DSN                                              |


`Client` содержит `*pgxpool.Pool`, поэтому вы получаете все `методы pgx` + дополнительные вспомогательные функции.

---

## Complete Example (REST service)
```go
func initDB() (*psql.Client, error) {
	cfg, err := psql.NewConfig(os.Getenv("DATABASE_URL"),
		psql.WithMaxAttempts(10),
		psql.WithQueryExecMode(pgx.QueryExecModeCacheDescribe), // only behind PgBouncer
		psql.WithHealthChecker("postgres", healthChecker),
		psql.WithIntervalCheck(10*time.Second),
	)
	if err != nil {
		return nil, err
	}

	return psql.NewClient(context.Background(), cfg)
}

// In your handler:
rows, err := db.Query(ctx, "SELECT * FROM orders WHERE user_id = $1", userID)
```

---

## Продакшен / хайлоад

### Pool defaults
`max=20`, `min=0` на инстанс (каждая реплика сервиса не держит простаивающие
соединения), время жизни 1ч с джиттером 10%, чтобы соединения не пересоздавались
одновременно. Приоритет: умолчания пакета < параметры `pool_*` в DSN < опции.

```go
cfg, _ := psql.NewConfig(dsn,
	psql.WithPoolName("primary"),                 // "pool" label in metrics/traces
	psql.WithConnectionPool(40, 0, time.Hour),
	psql.WithMaxConnIdleTime(5*time.Minute),
	psql.WithStatementTimeout(5*time.Second),     // server-side backstop
	psql.WithApplicationName("kline-service"),    // visible in pg_stat_activity
)
```

### Observability
Встроено, настройка не нужна:
- `postgres_query_duration_seconds{pool,query,status}`: label queries with
  `psql.WithQueryName(ctx, "kline.save")`, otherwise the SQL verb is used.
- `postgres_pool_*`: `pgxpool.Stat` (acquired/idle conns, empty acquires,
  acquire wait, destroyed conns). Рост `postgres_pool_empty_acquires_total`
  означает, что пул мал.
- OpenTelemetry client spans for queries, batches and COPY via the global
  TracerProvider. `WithTracing(false)`, `WithTraceSQL(false)` to tune;
  `WithQueryTracer` adds your own pgx tracer.
- Метрики регистрируются в `prometheus.DefaultRegisterer`; передайте
  `psql.WithRegisterer(reg)` для своего реестра или `nil`, чтобы отключить их.

### Transactions
```go
err := client.WithTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(tx pgx.Tx) error {
	// must be safe to re-run: retried on 40001 / 40P01 (WithTxMaxAttempts, default 3)
	_, err := tx.Exec(ctx, "UPDATE ...")
	return err
})
```

### Read replicas
```go
cluster, _ := psql.NewCluster(primary, replica1, replica2)
cluster.Replica().Query(ctx, ...) // healthy replica round-robin, falls back to primary
cluster.Primary().Exec(ctx, ...)  // writes and read-your-writes
```
Создавайте клиентов реплик с `psql.WithMaxReplicationLag(5*time.Second)`, чтобы
измерять отставание при каждой проверке: `Replica()` пропускает реплику, пока она
отстаёт сильнее, а значение экспортируется как
`postgres_replication_lag_seconds`. Без опции отставание не отслеживается.

### Testability
Зависьте от `psql.Querier` (его реализуют `*Client`, `pgx.Tx`, `*pgx.Conn`) и
`psql.TxRunner` вместо `*psql.Client`, чтобы мокать базу.

### Benchmarks
`go test -run '^$' -bench . ./...` (`-short` пропускает
бенчмарк с embedded PostgreSQL).

## Тестирование
Репозиторий содержит тесты, охватывающие:

- Анализ конфигурации
- Подключение с повторными попытками
- Горутины проверки работоспособности
- Обертки ошибок
- Экранирование построителя D-SN
- PrettySQL
- Корректное завершение работы

Run tests:
```bash
go test ./... -race
```

---

## License

[MIT License](https://github.com/jwm1rr0rb10/libraries/blob/main/backend/golang/LICENSE) – © Raman Zaitsau [@jwm1rrr0rb10](https://github.com/jwm1rr0rb10)

---

### 🙌 Contributions

Приветствуются запросы на изменения и предложения! Не стесняйтесь!




