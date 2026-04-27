using System.Data.Common;
using System.Text.Json;
using NexusDataSpace.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace NexusDataSpace.Core.Services;

public sealed class DataSourceService
{
    private const int BusyTimeoutMs = 5_000;
    private readonly string _dbPath;
    private readonly string _defaultAnalyticsDb;
    private readonly IHostEnvironment _environment;
    private readonly TenantContext _tenantContext;
    private readonly SecretProtector _secretProtector;
    private readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };
    private static readonly string[] ForbiddenSemanticSchemas =
    {
        "information_schema",
        "pg_catalog",
        "pg_toast",
        "mysql",
        "performance_schema",
        "sys",
        "sqlite_master",
        "sqlite_schema"
    };

    public DataSourceService(IConfiguration configuration, IHostEnvironment environment, TenantContext tenantContext, SecretProtector secretProtector)
    {
        _environment = environment;
        _tenantContext = tenantContext;
        _secretProtector = secretProtector;
        _dbPath = DataPathResolver.Resolve(environment, configuration["Data:ReportsDb"], "Data/reports.db");
        _defaultAnalyticsDb = configuration["Data:AnalyticsDb"] ?? "Data/nexus-data-space.db";

        var directory = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        EnsureSchema();
        EnsureDefaultDataSource();
    }

    public List<CompanyDataSource> List(int? companyId = null)
    {
        var tenantId = NormalizeCompanyId(companyId);
        using var connection = OpenMetadataConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, company_id, name, provider, connection_string, connection_secret, semantic_json, schema_json, is_builtin, is_active,
                   created_at, updated_at, last_validated_at, last_validation_error
            FROM data_sources
            WHERE company_id = $company_id
            ORDER BY is_active DESC, is_builtin DESC, name";
        command.Parameters.AddWithValue("$company_id", tenantId);
        return ReadDataSources(command, exposeSecret: false);
    }

    public CompanyDataSource? Get(int id, int? companyId = null)
    {
        using var connection = OpenMetadataConnection();
        return GetCore(connection, id, NormalizeCompanyId(companyId), exposeSecret: false);
    }

    private CompanyDataSource? GetForExecution(int id, int? companyId = null)
    {
        using var connection = OpenMetadataConnection();
        return GetCore(connection, id, NormalizeCompanyId(companyId), exposeSecret: true);
    }

    public CompanyDataSource GetActive(int? companyId = null)
    {
        var tenantId = NormalizeCompanyId(companyId);
        using var connection = OpenMetadataConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, company_id, name, provider, connection_string, connection_secret, semantic_json, schema_json, is_builtin, is_active,
                   created_at, updated_at, last_validated_at, last_validation_error
            FROM data_sources
            WHERE company_id = $company_id AND is_active = 1
            ORDER BY is_builtin DESC, id
            LIMIT 1";
        command.Parameters.AddWithValue("$company_id", tenantId);
        var active = ReadDataSource(command, exposeSecret: true);
        if (active != null)
            return active;

        EnsureDefaultDataSource(tenantId);
        return GetActive(tenantId);
    }

    public int Save(DataSourceInput input, int? companyId = null)
    {
        var tenantId = NormalizeCompanyId(companyId);
        var name = Require(input.Name, "Укажите название источника данных.");
        var provider = NormalizeProvider(input.Provider);
        var connectionString = input.ConnectionString?.Trim() ?? "";
        var semanticJson = NormalizeSemanticJson(input.SemanticJson);
        var now = DateTime.UtcNow;

        using var connection = OpenMetadataConnection();
        using var transaction = connection.BeginTransaction();

        int id;
        if (input.Id is > 0)
        {
            var existing = GetForUpdate(connection, transaction, input.Id.Value)
                ?? throw new InvalidOperationException("Источник данных не найден.");
            if (existing.CompanyId != tenantId)
                throw new InvalidOperationException("Источник данных не найден в текущей компании.");
            if (existing.IsBuiltin)
                throw new InvalidOperationException("Встроенный демо-источник нельзя перезаписать. Создайте новый источник компании.");

            var secret = SecretProtector.IsMasked(connectionString) || string.IsNullOrWhiteSpace(connectionString)
                ? existing.ConnectionString
                : connectionString;
            if (string.IsNullOrWhiteSpace(secret))
                throw new InvalidOperationException("Укажите строку подключения.");

            var runtimeChanged =
                !string.Equals(existing.Provider, provider, StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(existing.ConnectionString, secret, StringComparison.Ordinal);

            using var update = connection.CreateCommand();
            update.Transaction = transaction;
            update.CommandText = @"
                UPDATE data_sources
                SET name = $name,
                    provider = $provider,
                    connection_string = $connection_mask,
                    connection_secret = $connection_secret,
                    semantic_json = $semantic_json,
                    schema_json = CASE WHEN $runtime_changed = 1 THEN NULL ELSE schema_json END,
                    last_validated_at = CASE WHEN $runtime_changed = 1 THEN NULL ELSE last_validated_at END,
                    last_validation_error = CASE WHEN $runtime_changed = 1 THEN NULL ELSE last_validation_error END,
                    updated_at = $updated_at
                WHERE id = $id AND company_id = $company_id";
            update.Parameters.AddWithValue("$id", input.Id.Value);
            update.Parameters.AddWithValue("$company_id", tenantId);
            update.Parameters.AddWithValue("$name", name);
            update.Parameters.AddWithValue("$provider", provider);
            update.Parameters.AddWithValue("$connection_mask", SecretProtector.MaskConnectionString(secret));
            update.Parameters.AddWithValue("$connection_secret", _secretProtector.Protect(secret));
            update.Parameters.AddWithValue("$semantic_json", (object?)semanticJson ?? DBNull.Value);
            update.Parameters.AddWithValue("$runtime_changed", runtimeChanged ? 1 : 0);
            update.Parameters.AddWithValue("$updated_at", now.ToString("O"));
            update.ExecuteNonQuery();
            id = input.Id.Value;
        }
        else
        {
            connectionString = Require(connectionString, "Укажите строку подключения.");
            using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = @"
                INSERT INTO data_sources(
                    company_id, name, provider, connection_string, connection_secret, semantic_json, schema_json,
                    is_builtin, is_active, created_at, updated_at)
                VALUES(
                    $company_id, $name, $provider, $connection_mask, $connection_secret, $semantic_json, NULL,
                    0, 0, $created_at, $updated_at);
                SELECT last_insert_rowid();";
            insert.Parameters.AddWithValue("$company_id", tenantId);
            insert.Parameters.AddWithValue("$name", name);
            insert.Parameters.AddWithValue("$provider", provider);
            insert.Parameters.AddWithValue("$connection_mask", SecretProtector.MaskConnectionString(connectionString));
            insert.Parameters.AddWithValue("$connection_secret", _secretProtector.Protect(connectionString));
            insert.Parameters.AddWithValue("$semantic_json", (object?)semanticJson ?? DBNull.Value);
            insert.Parameters.AddWithValue("$created_at", now.ToString("O"));
            insert.Parameters.AddWithValue("$updated_at", now.ToString("O"));
            id = Convert.ToInt32(insert.ExecuteScalar());
        }

        if (input.MakeActive)
            ActivateCore(connection, transaction, tenantId, id);

        transaction.Commit();
        return id;
    }

    public void Activate(int id, int? companyId = null)
    {
        var tenantId = NormalizeCompanyId(companyId);
        using var connection = OpenMetadataConnection();
        using var transaction = connection.BeginTransaction();
        ActivateCore(connection, transaction, tenantId, id);
        transaction.Commit();
    }

    public void EnsureActiveSourceReadyForAnalytics(int? companyId = null)
    {
        var active = GetActive(companyId);
        if (!active.IsVerified)
        {
            var detail = string.IsNullOrWhiteSpace(active.LastValidationError)
                ? "Сначала запустите проверку подключения."
                : active.LastValidationError;
            throw new InvalidOperationException($"Активный источник `{active.Name}` не прошёл проверку. {detail}");
        }

        if (!active.IsBuiltin && string.IsNullOrWhiteSpace(active.SemanticJson))
            throw new InvalidOperationException($"Активный источник `{active.Name}` не содержит semantic layer.");
    }

    public SecretRotationResult RotateConnectionSecrets(int? companyId = null)
    {
        var tenantId = NormalizeCompanyId(companyId);
        var rows = new List<(int Id, string ConnectionString)>();
        using (var connection = OpenMetadataConnection())
        {
            using var select = connection.CreateCommand();
            select.CommandText = @"
                SELECT id, connection_secret, connection_string
                FROM data_sources
                WHERE company_id = $company_id";
            select.Parameters.AddWithValue("$company_id", tenantId);

            using var reader = select.ExecuteReader();
            while (reader.Read())
            {
                var protectedSecret = reader.IsDBNull(1) ? null : reader.GetString(1);
                var fallbackMask = reader.GetString(2);
                if (string.IsNullOrWhiteSpace(protectedSecret))
                    continue;

                rows.Add((reader.GetInt32(0), _secretProtector.Unprotect(protectedSecret) ?? fallbackMask));
            }
        }

        var rotated = 0;
        using (var connection = OpenMetadataConnection())
        using (var transaction = connection.BeginTransaction())
        {
            foreach (var row in rows)
            {
                using var update = connection.CreateCommand();
                update.Transaction = transaction;
                update.CommandText = @"
                    UPDATE data_sources
                    SET connection_secret = $connection_secret,
                        connection_string = $connection_mask,
                        updated_at = $updated_at
                    WHERE id = $id AND company_id = $company_id";
                update.Parameters.AddWithValue("$id", row.Id);
                update.Parameters.AddWithValue("$company_id", tenantId);
                update.Parameters.AddWithValue("$connection_secret", _secretProtector.Protect(row.ConnectionString));
                update.Parameters.AddWithValue("$connection_mask", SecretProtector.MaskConnectionString(row.ConnectionString));
                update.Parameters.AddWithValue("$updated_at", DateTime.UtcNow.ToString("O"));
                rotated += update.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        return new SecretRotationResult(rotated, rows.Count - rotated);
    }

    public void Delete(int id, int? companyId = null)
    {
        var tenantId = NormalizeCompanyId(companyId);
        using var connection = OpenMetadataConnection();
        using var transaction = connection.BeginTransaction();

        var existing = GetForUpdate(connection, transaction, id)
            ?? throw new InvalidOperationException("Источник данных не найден.");
        if (existing.CompanyId != tenantId)
            throw new InvalidOperationException("Источник данных не найден в текущей компании.");
        if (existing.IsBuiltin)
            throw new InvalidOperationException("Встроенный демо-источник нельзя удалить.");
        if (existing.IsActive)
            throw new InvalidOperationException("Активный источник нельзя удалить. Сначала активируйте другой источник.");

        using var delete = connection.CreateCommand();
        delete.Transaction = transaction;
        delete.CommandText = "DELETE FROM data_sources WHERE id = $id AND company_id = $company_id";
        delete.Parameters.AddWithValue("$id", id);
        delete.Parameters.AddWithValue("$company_id", tenantId);
        delete.ExecuteNonQuery();

        transaction.Commit();
    }

    public DataSourceTestResult Test(int id, int? companyId = null)
    {
        var dataSource = GetForExecution(id, companyId) ?? throw new InvalidOperationException("Источник данных не найден.");
        return Test(dataSource);
    }

    public DataSourceTestResult Test(CompanyDataSource dataSource)
    {
        var result = new DataSourceTestResult { CheckedAt = DateTime.UtcNow };
        try
        {
            using var connection = OpenReadOnlyConnection(dataSource);
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT 1";
            _ = command.ExecuteScalar();
            result.Ok = true;
            result.Message = "Подключение успешно.";
            SaveValidationResult(dataSource.Id, result.CheckedAt, null);
        }
        catch (Exception exception) when (exception is InvalidOperationException or DbException or IOException)
        {
            result.Ok = false;
            result.Message = exception.Message;
            SaveValidationResult(dataSource.Id, result.CheckedAt, result.Message);
        }

        return result;
    }

    public SchemaInspectionResult InspectSchema(int id, int? companyId = null)
    {
        var dataSource = GetForExecution(id, companyId) ?? throw new InvalidOperationException("Источник данных не найден.");
        if (string.Equals(dataSource.Provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase))
            return InspectPostgreSqlSchema(dataSource);

        if (!string.Equals(dataSource.Provider, DataSourceProviders.Sqlite, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Автоанализ схемы сейчас реализован для SQLite. Для других провайдеров заполните semantic layer вручную или добавьте ADO.NET inspector.");

        using var connection = (SqliteConnection)OpenReadOnlyConnection(dataSource);
        var result = new SchemaInspectionResult { Provider = dataSource.Provider };

        using var tablesCommand = connection.CreateCommand();
        tablesCommand.CommandText = @"
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name";

        using var tablesReader = tablesCommand.ExecuteReader();
        while (tablesReader.Read())
        {
            var table = new SchemaTableInfo { Name = tablesReader.GetString(0) };
            result.Tables.Add(table);
        }

        foreach (var table in result.Tables)
        {
            using var countCommand = connection.CreateCommand();
            countCommand.CommandText = $"SELECT COUNT(*) FROM {QuoteSqliteIdentifier(table.Name)}";
            table.EstimatedRows = Convert.ToInt64(countCommand.ExecuteScalar());

            using var columnsCommand = connection.CreateCommand();
            columnsCommand.CommandText = $"PRAGMA table_info({QuoteSqliteIdentifier(table.Name)})";
            using var columnsReader = columnsCommand.ExecuteReader();
            while (columnsReader.Read())
            {
                table.Columns.Add(new SchemaColumnInfo
                {
                    Name = columnsReader.GetString(1),
                    DataType = columnsReader.IsDBNull(2) ? "" : columnsReader.GetString(2),
                    IsNullable = columnsReader.GetInt32(3) == 0,
                    IsPrimaryKey = columnsReader.GetInt32(5) > 0
                });
            }
        }

        SaveSchemaJson(id, JsonSerializer.Serialize(result, _jsonOptions));
        return result;
    }

    private SchemaInspectionResult InspectPostgreSqlSchema(CompanyDataSource dataSource)
    {
        using var connection = (NpgsqlConnection)OpenReadOnlyConnection(dataSource);
        var result = new SchemaInspectionResult { Provider = dataSource.Provider };

        using var tablesCommand = connection.CreateCommand();
        tablesCommand.CommandText = @"
            SELECT n.nspname AS schema_name,
                   c.relname AS table_name,
                   GREATEST(COALESCE(s.n_live_tup, 0), COALESCE(c.reltuples, 0))::bigint AS estimated_rows
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname NOT LIKE 'pg_toast%'
            ORDER BY n.nspname, c.relname";

        using (var tablesReader = tablesCommand.ExecuteReader())
        {
            while (tablesReader.Read())
            {
                result.Tables.Add(new SchemaTableInfo
                {
                    SchemaName = tablesReader.GetString(0),
                    Name = tablesReader.GetString(1),
                    EstimatedRows = tablesReader.IsDBNull(2) ? null : Convert.ToInt64(tablesReader.GetValue(2))
                });
            }
        }

        foreach (var table in result.Tables)
        {
            using var columnsCommand = connection.CreateCommand();
            columnsCommand.CommandText = @"
                SELECT c.column_name,
                       c.data_type,
                       c.is_nullable,
                       EXISTS (
                           SELECT 1
                           FROM information_schema.table_constraints tc
                           JOIN information_schema.key_column_usage kcu
                             ON kcu.constraint_schema = tc.constraint_schema
                            AND kcu.constraint_name = tc.constraint_name
                            AND kcu.table_schema = tc.table_schema
                            AND kcu.table_name = tc.table_name
                           WHERE tc.constraint_type = 'PRIMARY KEY'
                             AND tc.table_schema = c.table_schema
                             AND tc.table_name = c.table_name
                             AND kcu.column_name = c.column_name
                       ) AS is_primary_key
                FROM information_schema.columns c
                WHERE c.table_schema = @schema_name
                  AND c.table_name = @table_name
                ORDER BY c.ordinal_position";
            columnsCommand.Parameters.AddWithValue("@schema_name", table.SchemaName);
            columnsCommand.Parameters.AddWithValue("@table_name", table.Name);

            using var columnsReader = columnsCommand.ExecuteReader();
            while (columnsReader.Read())
            {
                table.Columns.Add(new SchemaColumnInfo
                {
                    Name = columnsReader.GetString(0),
                    DataType = columnsReader.IsDBNull(1) ? "" : columnsReader.GetString(1),
                    IsNullable = string.Equals(columnsReader.GetString(2), "YES", StringComparison.OrdinalIgnoreCase),
                    IsPrimaryKey = columnsReader.GetBoolean(3)
                });
            }
        }

        SaveSchemaJson(dataSource.Id, JsonSerializer.Serialize(result, _jsonOptions));
        return result;
    }

    public string BuildSemanticDraftFromLastSchema(int id, int? companyId = null)
    {
        var dataSource = Get(id, companyId) ?? throw new InvalidOperationException("Источник данных не найден.");
        if (string.IsNullOrWhiteSpace(dataSource.SchemaJson))
            throw new InvalidOperationException("Сначала выполните анализ схемы.");

        var schema = JsonSerializer.Deserialize<SchemaInspectionResult>(dataSource.SchemaJson, _jsonOptions)
            ?? throw new InvalidOperationException("Не удалось прочитать сохранённую схему.");
        return BuildSemanticDraft(schema);
    }

    public DbConnection OpenReadOnlyConnection(CompanyDataSource dataSource)
    {
        if (!DataSourceProviders.CanExecute(dataSource.Provider))
        {
            throw new InvalidOperationException(
                $"Провайдер `{dataSource.Provider}` сохранён, но выполнение запросов пока не включено в runtime. Добавьте ADO.NET provider и SQL dialect adapter для этого типа БД.");
        }

        if (string.Equals(dataSource.Provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase))
            return OpenPostgreSqlReadOnlyConnection(dataSource.ConnectionString);

        return OpenSqliteReadOnlyConnection(dataSource.ConnectionString);
    }

    public string ResolveSqliteDataSourcePath(string connectionString)
    {
        var builder = BuildSqliteBuilder(connectionString);
        return ResolveDataPath(builder.DataSource);
    }

    private void EnsureSchema()
    {
        using var connection = OpenMetadataConnection();
        EnsurePragmas(connection);
        using var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS data_sources (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id            INTEGER NOT NULL DEFAULT 1,
                name                  TEXT NOT NULL,
                provider              TEXT NOT NULL,
                connection_string     TEXT NOT NULL,
                connection_secret     TEXT NULL,
                semantic_json         TEXT NULL,
                schema_json           TEXT NULL,
                is_builtin            INTEGER NOT NULL DEFAULT 0,
                is_active             INTEGER NOT NULL DEFAULT 0,
                created_at            TEXT NOT NULL,
                updated_at            TEXT NOT NULL,
                last_validated_at     TEXT NULL,
                last_validation_error TEXT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_data_sources_active
                ON data_sources(company_id, is_active);";
        command.ExecuteNonQuery();

        EnsureColumn(connection, "data_sources", "company_id", "INTEGER NOT NULL DEFAULT 1");
        EnsureColumn(connection, "data_sources", "is_builtin", "INTEGER NOT NULL DEFAULT 0");
        EnsureColumn(connection, "data_sources", "connection_secret", "TEXT NULL");
        EnsureColumn(connection, "data_sources", "schema_json", "TEXT NULL");
        EnsureColumn(connection, "data_sources", "last_validated_at", "TEXT NULL");
        EnsureColumn(connection, "data_sources", "last_validation_error", "TEXT NULL");
        MigrateConnectionSecrets(connection);
        MigrateLegacyBranding(connection);
    }

    private void EnsureDefaultDataSource(int? companyId = null)
    {
        var tenantId = NormalizeCompanyId(companyId);
        using var connection = OpenMetadataConnection();
        using var transaction = connection.BeginTransaction();

        using var existing = connection.CreateCommand();
        existing.Transaction = transaction;
        existing.CommandText = "SELECT id FROM data_sources WHERE company_id = $company_id AND is_builtin = 1 LIMIT 1";
        existing.Parameters.AddWithValue("$company_id", tenantId);
        var existingId = existing.ExecuteScalar();
        if (existingId == null)
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = @"
                INSERT INTO data_sources(
                    company_id, name, provider, connection_string, connection_secret, semantic_json, schema_json,
                    is_builtin, is_active, created_at, updated_at, last_validated_at, last_validation_error)
                VALUES(
                    $company_id, $name, $provider, $connection_mask, $connection_secret, NULL, NULL,
                    1, 1, $created_at, $updated_at, $last_validated_at, NULL)";
            insert.Parameters.AddWithValue("$company_id", tenantId);
            insert.Parameters.AddWithValue("$name", "Nexus Data Space demo dataset");
            insert.Parameters.AddWithValue("$provider", DataSourceProviders.Sqlite);
            insert.Parameters.AddWithValue("$connection_mask", SecretProtector.MaskConnectionString(_defaultAnalyticsDb));
            insert.Parameters.AddWithValue("$connection_secret", _secretProtector.Protect(_defaultAnalyticsDb));
            insert.Parameters.AddWithValue("$created_at", DateTime.UtcNow.ToString("O"));
            insert.Parameters.AddWithValue("$updated_at", DateTime.UtcNow.ToString("O"));
            insert.Parameters.AddWithValue("$last_validated_at", DateTime.UtcNow.ToString("O"));
            insert.ExecuteNonQuery();
        }
        else
        {
            using var markBuiltinVerified = connection.CreateCommand();
            markBuiltinVerified.Transaction = transaction;
            markBuiltinVerified.CommandText = @"
                UPDATE data_sources
                SET last_validated_at = COALESCE(last_validated_at, $last_validated_at),
                    last_validation_error = NULL
                WHERE company_id = $company_id
                  AND is_builtin = 1
                  AND last_validated_at IS NULL";
            markBuiltinVerified.Parameters.AddWithValue("$company_id", tenantId);
            markBuiltinVerified.Parameters.AddWithValue("$last_validated_at", DateTime.UtcNow.ToString("O"));
            markBuiltinVerified.ExecuteNonQuery();
        }

        using var anyActive = connection.CreateCommand();
        anyActive.Transaction = transaction;
        anyActive.CommandText = "SELECT 1 FROM data_sources WHERE company_id = $company_id AND is_active = 1 LIMIT 1";
        anyActive.Parameters.AddWithValue("$company_id", tenantId);
        if (anyActive.ExecuteScalar() == null)
        {
            using var activate = connection.CreateCommand();
            activate.Transaction = transaction;
            activate.CommandText = "UPDATE data_sources SET is_active = CASE WHEN is_builtin = 1 THEN 1 ELSE 0 END WHERE company_id = $company_id";
            activate.Parameters.AddWithValue("$company_id", tenantId);
            activate.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private void ActivateCore(SqliteConnection connection, SqliteTransaction transaction, int companyId, int id)
    {
        var dataSource = GetForUpdate(connection, transaction, id)
            ?? throw new InvalidOperationException("Источник данных не найден.");
        if (dataSource.CompanyId != companyId)
            throw new InvalidOperationException("Источник данных не найден в текущей компании.");

        if (!dataSource.IsBuiltin && string.IsNullOrWhiteSpace(dataSource.SemanticJson))
            throw new InvalidOperationException("Перед активацией заполните semantic layer: метрики, таблицы, поля, фильтры и группировки.");

        if (!dataSource.IsVerified)
        {
            var detail = string.IsNullOrWhiteSpace(dataSource.LastValidationError)
                ? "Сначала запустите проверку подключения."
                : dataSource.LastValidationError;
            throw new InvalidOperationException($"Перед активацией источник должен пройти проверку. {detail}");
        }

        using var deactivate = connection.CreateCommand();
        deactivate.Transaction = transaction;
        deactivate.CommandText = "UPDATE data_sources SET is_active = 0 WHERE company_id = $company_id";
        deactivate.Parameters.AddWithValue("$company_id", companyId);
        deactivate.ExecuteNonQuery();

        using var activate = connection.CreateCommand();
        activate.Transaction = transaction;
        activate.CommandText = "UPDATE data_sources SET is_active = 1, updated_at = $updated_at WHERE id = $id AND company_id = $company_id";
        activate.Parameters.AddWithValue("$id", id);
        activate.Parameters.AddWithValue("$company_id", companyId);
        activate.Parameters.AddWithValue("$updated_at", DateTime.UtcNow.ToString("O"));
        activate.ExecuteNonQuery();
    }

    private string BuildSemanticDraft(SchemaInspectionResult schema)
    {
        var table = schema.Tables
            .OrderByDescending(item => item.EstimatedRows ?? 0)
            .FirstOrDefault(item => item.Columns.Count > 0)
            ?? throw new InvalidOperationException("В схеме не найдено таблиц с колонками.");

        var key = NormalizeKey(string.IsNullOrWhiteSpace(table.SchemaName) || string.Equals(table.SchemaName, "public", StringComparison.OrdinalIgnoreCase)
            ? table.Name
            : $"{table.SchemaName}_{table.Name}");
        var tableReference = BuildTableReference(schema.Provider, table);
        var countColumn = table.Columns.FirstOrDefault(column => column.IsPrimaryKey)?.Name
            ?? table.Columns.First().Name;
        var dateColumn = table.Columns.FirstOrDefault(column => LooksLikeDateColumn(column))?.Name
            ?? table.Columns.First().Name;
        var quotedCountColumn = QuoteColumnReference(schema.Provider, countColumn);
        var quotedDateColumn = QuoteColumnReference(schema.Provider, dateColumn);

        var dimensions = BuildDimensionDrafts(schema.Provider, table, key, dateColumn).ToList();

        var filters = table.Columns
            .Take(20)
            .Select(column => new SemanticFilterJson
            {
                Key = NormalizeKey(column.Name),
                DisplayLabel = column.Name,
                Column = QuoteColumnReference(schema.Provider, column.Name),
                Source = key,
                AllowedOperators = LooksLikeNumericMeasure(column) || LooksLikeDateColumn(column)
                    ? new List<string> { "=", "!=", ">", ">=", "<", "<=" }
                    : new List<string> { "=", "!=", "in", "not_in" },
                Synonyms = BuildColumnSynonyms(column.Name).ToList()
            })
            .ToList();

        var metrics = new List<SemanticMetricJson>
        {
            new()
            {
                Key = $"{key}_count",
                DisplayLabel = $"Количество строк {table.Name}",
                Aggregation = "count",
                Expression = quotedCountColumn,
                Source = key,
                DateColumn = quotedDateColumn,
                AllowedDimensions = dimensions.Select(dimension => dimension.Key).ToList(),
                AllowedFilters = filters.Select(filter => filter.Key).ToList(),
                Synonyms = new List<string> { table.Name, $"{table.Name} count", "количество", "число" }
            }
        };

        foreach (var measure in table.Columns.Where(LooksLikeNumericMeasure).Take(8))
        {
            metrics.Add(new SemanticMetricJson
            {
                Key = $"{NormalizeKey(measure.Name)}_sum",
                DisplayLabel = $"Сумма {measure.Name}",
                Aggregation = "sum",
                Expression = QuoteColumnReference(schema.Provider, measure.Name),
                Source = key,
                DateColumn = quotedDateColumn,
                AllowedDimensions = dimensions.Select(dimension => dimension.Key).ToList(),
                AllowedFilters = filters.Select(filter => filter.Key).ToList(),
                Synonyms = BuildColumnSynonyms(measure.Name).Append($"сумма {measure.Name}").ToList()
            });
        }

        var profile = new SemanticLayerJson
        {
            Sources = new List<SemanticSourceJson>
            {
                new()
                {
                    Key = key,
                    Table = tableReference,
                    DisplayLabel = table.Name,
                    AllowedColumns = table.Columns.Select(column => column.Name).ToList()
                }
            },
            Metrics = metrics,
            Dimensions = dimensions,
            Filters = filters
        };

        return JsonSerializer.Serialize(profile, _jsonOptions);
    }

    private static IEnumerable<SemanticDimensionJson> BuildDimensionDrafts(
        string provider,
        SchemaTableInfo table,
        string sourceKey,
        string dateColumn)
    {
        var usedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var quotedDateColumn = QuoteColumnReference(provider, dateColumn);

        if (table.Columns.Any(LooksLikeDateColumn))
        {
            foreach (var timeDimension in BuildTimeDimensions(provider, sourceKey, quotedDateColumn))
            {
                usedKeys.Add(timeDimension.Key);
                yield return timeDimension;
            }
        }

        foreach (var column in table.Columns.Where(column => !LooksLikeNumericMeasure(column)).Take(12))
        {
            var columnKey = NormalizeKey(column.Name);
            if (!usedKeys.Add(columnKey))
                columnKey = NormalizeKey($"{table.Name}_{column.Name}");

            yield return new SemanticDimensionJson
            {
                Key = columnKey,
                DisplayLabel = column.Name,
                Expression = LooksLikeDateColumn(column)
                    ? BuildTimeDimensionExpression(provider, "day", QuoteColumnReference(provider, column.Name))
                    : QuoteColumnReference(provider, column.Name),
                IsTimeDimension = LooksLikeDateColumn(column),
                Source = sourceKey,
                Synonyms = BuildColumnSynonyms(column.Name).ToList()
            };
        }
    }

    private static IEnumerable<SemanticDimensionJson> BuildTimeDimensions(string provider, string sourceKey, string quotedDateColumn)
    {
        yield return new SemanticDimensionJson
        {
            Key = "day",
            DisplayLabel = "по дням",
            Expression = BuildTimeDimensionExpression(provider, "day", quotedDateColumn),
            IsTimeDimension = true,
            Source = sourceKey,
            Synonyms = new List<string> { "day", "date", "день", "дням", "по дням", "дата" }
        };

        yield return new SemanticDimensionJson
        {
            Key = "week",
            DisplayLabel = "по неделям",
            Expression = BuildTimeDimensionExpression(provider, "week", quotedDateColumn),
            IsTimeDimension = true,
            Source = sourceKey,
            Synonyms = new List<string> { "week", "неделя", "неделям", "по неделям" }
        };

        yield return new SemanticDimensionJson
        {
            Key = "month",
            DisplayLabel = "по месяцам",
            Expression = BuildTimeDimensionExpression(provider, "month", quotedDateColumn),
            IsTimeDimension = true,
            Source = sourceKey,
            Synonyms = new List<string> { "month", "месяц", "месяцам", "по месяцам" }
        };

        yield return new SemanticDimensionJson
        {
            Key = "year",
            DisplayLabel = "по годам",
            Expression = BuildTimeDimensionExpression(provider, "year", quotedDateColumn),
            IsTimeDimension = true,
            Source = sourceKey,
            Synonyms = new List<string> { "year", "год", "годам", "по годам" }
        };
    }

    private static string BuildTimeDimensionExpression(string provider, string unit, string quotedDateColumn)
    {
        if (string.Equals(provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase))
        {
            return unit switch
            {
                "day" => $"date_trunc('day', {quotedDateColumn})::date",
                "week" => $"date_trunc('week', {quotedDateColumn})::date",
                "month" => $"date_trunc('month', {quotedDateColumn})::date",
                "year" => $"date_trunc('year', {quotedDateColumn})::date",
                _ => quotedDateColumn
            };
        }

        return unit switch
        {
            "day" => $"date({quotedDateColumn})",
            "week" => $"strftime('%Y-W%W', {quotedDateColumn})",
            "month" => $"strftime('%Y-%m', {quotedDateColumn})",
            "year" => $"strftime('%Y', {quotedDateColumn})",
            _ => quotedDateColumn
        };
    }

    private static string BuildTableReference(string provider, SchemaTableInfo table)
    {
        if (string.Equals(provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase))
        {
            return string.IsNullOrWhiteSpace(table.SchemaName)
                ? QuotePostgreSqlIdentifier(table.Name)
                : $"{QuotePostgreSqlIdentifier(table.SchemaName)}.{QuotePostgreSqlIdentifier(table.Name)}";
        }

        return IsSimpleIdentifier(table.Name) ? table.Name : QuoteSqliteIdentifier(table.Name);
    }

    private static string QuoteColumnReference(string provider, string columnName)
    {
        if (string.Equals(provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase))
            return QuotePostgreSqlIdentifier(columnName);

        return IsSimpleIdentifier(columnName) ? columnName : QuoteSqliteIdentifier(columnName);
    }

    private static string QuotePostgreSqlIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static bool IsSimpleIdentifier(string identifier) =>
        !string.IsNullOrWhiteSpace(identifier) &&
        (char.IsLetter(identifier[0]) || identifier[0] == '_') &&
        identifier.All(character => char.IsLetterOrDigit(character) || character == '_');

    private static bool LooksLikeDateColumn(SchemaColumnInfo column) =>
        column.Name.Contains("date", StringComparison.OrdinalIgnoreCase) ||
        column.Name.Contains("time", StringComparison.OrdinalIgnoreCase) ||
        column.Name.Contains("_at", StringComparison.OrdinalIgnoreCase) ||
        column.DataType.Contains("date", StringComparison.OrdinalIgnoreCase) ||
        column.DataType.Contains("time", StringComparison.OrdinalIgnoreCase);

    private static bool LooksLikeNumericMeasure(SchemaColumnInfo column)
    {
        var type = column.DataType.ToLowerInvariant();
        if (type.Contains("int", StringComparison.Ordinal) ||
            type.Contains("real", StringComparison.Ordinal) ||
            type.Contains("numeric", StringComparison.Ordinal) ||
            type.Contains("decimal", StringComparison.Ordinal) ||
            type.Contains("double", StringComparison.Ordinal) ||
            type.Contains("float", StringComparison.Ordinal))
        {
            return !column.IsPrimaryKey && !column.Name.EndsWith("_id", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static IEnumerable<string> BuildColumnSynonyms(string columnName)
    {
        yield return columnName;
        yield return columnName.Replace("_", " ", StringComparison.Ordinal);
    }

    private static string NormalizeSemanticJson(string? semanticJson)
    {
        if (string.IsNullOrWhiteSpace(semanticJson))
            return "";

        var parsed = JsonSerializer.Deserialize<SemanticLayerJson>(semanticJson, new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidOperationException("Semantic layer должен быть JSON-объектом.");

        if (parsed.Sources.Count == 0 || parsed.Metrics.Count == 0)
            throw new InvalidOperationException("Semantic layer должен содержать хотя бы один source и одну metric.");

        ValidateSemanticLayerSafety(parsed);

        return JsonSerializer.Serialize(parsed, new JsonSerializerOptions(JsonSerializerDefaults.Web) { WriteIndented = true });
    }

    private static void ValidateSemanticLayerSafety(SemanticLayerJson semanticLayer)
    {
        foreach (var source in semanticLayer.Sources)
            ValidateSemanticTableReference(source.Table, source.Key);
    }

    private static void ValidateSemanticTableReference(string tableReference, string sourceKey)
    {
        if (string.IsNullOrWhiteSpace(tableReference))
            throw new InvalidOperationException($"Semantic source `{sourceKey}` должен содержать table.");

        if (tableReference.Contains(';', StringComparison.Ordinal) ||
            tableReference.Contains("--", StringComparison.Ordinal) ||
            tableReference.Contains("/*", StringComparison.Ordinal) ||
            tableReference.Contains("*/", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Semantic source `{sourceKey}` содержит опасную table-ссылку.");
        }

        var parts = tableReference
            .Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(UnquoteIdentifier)
            .Where(part => !string.IsNullOrWhiteSpace(part))
            .ToList();

        if (parts.Count == 0 || parts.Count > 2)
            throw new InvalidOperationException($"Semantic source `{sourceKey}` должен ссылаться на table или schema.table.");

        foreach (var part in parts)
        {
            if (ForbiddenSemanticSchemas.Any(schema => string.Equals(schema, part, StringComparison.OrdinalIgnoreCase)) ||
                part.StartsWith("pg_", StringComparison.OrdinalIgnoreCase) ||
                part.StartsWith("sqlite_", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Semantic source `{sourceKey}` ссылается на запрещённую системную схему или таблицу `{part}`.");
            }
        }
    }

    private static string UnquoteIdentifier(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.Length >= 2 &&
            ((trimmed[0] == '"' && trimmed[^1] == '"') ||
             (trimmed[0] == '`' && trimmed[^1] == '`') ||
             (trimmed[0] == '[' && trimmed[^1] == ']')))
        {
            return trimmed[1..^1].Replace("\"\"", "\"", StringComparison.Ordinal);
        }

        return trimmed;
    }

    private static string NormalizeProvider(string provider)
    {
        var normalized = string.IsNullOrWhiteSpace(provider)
            ? DataSourceProviders.Sqlite
            : provider.Trim().ToLowerInvariant();

        if (!DataSourceProviders.All.Contains(normalized, StringComparer.OrdinalIgnoreCase))
            throw new InvalidOperationException($"Неподдерживаемый provider `{provider}`.");

        return normalized;
    }

    private static string Require(string value, string message)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException(message);

        return value.Trim();
    }

    private static string NormalizeKey(string value)
    {
        var chars = value
            .Trim()
            .Select(character => char.IsLetterOrDigit(character) ? char.ToLowerInvariant(character) : '_')
            .ToArray();
        var key = new string(chars).Trim('_');
        while (key.Contains("__", StringComparison.Ordinal))
            key = key.Replace("__", "_", StringComparison.Ordinal);
        return string.IsNullOrWhiteSpace(key) ? "source" : key;
    }

    private string BuildSqliteConnectionString(string connectionString, SqliteOpenMode mode)
    {
        var builder = BuildSqliteBuilder(connectionString);
        builder.DataSource = ResolveDataPath(builder.DataSource);
        builder.Mode = mode;
        builder.Pooling = true;
        return builder.ToString();
    }

    private SqliteConnection OpenSqliteReadOnlyConnection(string connectionString)
    {
        var connection = new SqliteConnection(BuildSqliteConnectionString(connectionString, SqliteOpenMode.ReadOnly));
        connection.Open();
        using var timeout = connection.CreateCommand();
        timeout.CommandText = $"PRAGMA busy_timeout = {BusyTimeoutMs};";
        timeout.ExecuteNonQuery();

        using var queryOnly = connection.CreateCommand();
        queryOnly.CommandText = "PRAGMA query_only = ON;";
        queryOnly.ExecuteNonQuery();
        return connection;
    }

    private static NpgsqlConnection OpenPostgreSqlReadOnlyConnection(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString)
        {
            Pooling = true
        };

        var connection = new NpgsqlConnection(builder.ConnectionString);
        connection.Open();

        using (var readOnlyCommand = connection.CreateCommand())
        {
            readOnlyCommand.CommandText = "SET default_transaction_read_only = on";
            readOnlyCommand.ExecuteNonQuery();
        }

        using (var timeoutCommand = connection.CreateCommand())
        {
            timeoutCommand.CommandText = "SET statement_timeout = 15000";
            timeoutCommand.ExecuteNonQuery();
        }

        return connection;
    }

    private static SqliteConnectionStringBuilder BuildSqliteBuilder(string connectionString)
    {
        if (connectionString.Contains('=', StringComparison.Ordinal))
            return new SqliteConnectionStringBuilder(connectionString);

        return new SqliteConnectionStringBuilder { DataSource = connectionString };
    }

    private string ResolveDataPath(string path) =>
        Path.IsPathRooted(path) ? path : Path.Combine(_environment.ContentRootPath, path);

    private static string QuoteSqliteIdentifier(string identifier) =>
        "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private void SaveSchemaJson(int id, string schemaJson)
    {
        using var connection = OpenMetadataConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            UPDATE data_sources
            SET schema_json = $schema_json,
                updated_at = $updated_at
            WHERE id = $id";
        command.Parameters.AddWithValue("$id", id);
        command.Parameters.AddWithValue("$schema_json", schemaJson);
        command.Parameters.AddWithValue("$updated_at", DateTime.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    private void SaveValidationResult(int id, DateTime checkedAt, string? error)
    {
        if (id <= 0)
            return;

        using var connection = OpenMetadataConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            UPDATE data_sources
            SET last_validated_at = $last_validated_at,
                last_validation_error = $last_validation_error,
                updated_at = $updated_at
            WHERE id = $id";
        command.Parameters.AddWithValue("$id", id);
        command.Parameters.AddWithValue("$last_validated_at", checkedAt.ToString("O"));
        command.Parameters.AddWithValue("$last_validation_error", (object?)error ?? DBNull.Value);
        command.Parameters.AddWithValue("$updated_at", DateTime.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    private void MigrateConnectionSecrets(SqliteConnection connection)
    {
        using var select = connection.CreateCommand();
        select.CommandText = @"
            SELECT id, connection_string, connection_secret
            FROM data_sources
            WHERE connection_secret IS NULL OR connection_secret = ''";

        var rows = new List<(int Id, string ConnectionString)>();
        using (var reader = select.ExecuteReader())
        {
            while (reader.Read())
                rows.Add((reader.GetInt32(0), reader.GetString(1)));
        }

        foreach (var row in rows)
        {
            using var update = connection.CreateCommand();
            update.CommandText = @"
                UPDATE data_sources
                SET connection_string = $connection_mask,
                    connection_secret = $connection_secret
                WHERE id = $id";
            update.Parameters.AddWithValue("$id", row.Id);
            update.Parameters.AddWithValue("$connection_mask", SecretProtector.MaskConnectionString(row.ConnectionString));
            update.Parameters.AddWithValue("$connection_secret", _secretProtector.Protect(row.ConnectionString));
            update.ExecuteNonQuery();
        }
    }

    private void MigrateLegacyBranding(SqliteConnection connection)
    {
        using var select = connection.CreateCommand();
        select.CommandText = @"
            SELECT id, name, provider, connection_string, connection_secret, is_builtin
            FROM data_sources";

        var rows = new List<(int Id, string Name, string Provider, string ConnectionMask, string? ProtectedSecret, bool IsBuiltin)>();
        using (var reader = select.ExecuteReader())
        {
            while (reader.Read())
            {
                rows.Add((
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.GetInt64(5) == 1));
            }
        }

        foreach (var row in rows)
        {
            var name = string.Equals(row.Name, LegacyDemoDataSourceName(), StringComparison.Ordinal)
                ? "Nexus Data Space demo dataset"
                : row.Name;

            string? clearConnectionString = null;
            if (!string.IsNullOrWhiteSpace(row.ProtectedSecret))
                _secretProtector.TryUnprotect(row.ProtectedSecret, out clearConnectionString);

            if (string.IsNullOrWhiteSpace(clearConnectionString) &&
                row.IsBuiltin &&
                IsLegacyAnalyticsConnectionString(row.ConnectionMask))
            {
                clearConnectionString = _defaultAnalyticsDb;
            }

            if (IsLegacyAnalyticsConnectionString(clearConnectionString))
                clearConnectionString = _defaultAnalyticsDb;

            var shouldUpdateConnection =
                !string.IsNullOrWhiteSpace(clearConnectionString) &&
                (IsLegacyAnalyticsConnectionString(row.ConnectionMask) ||
                 !_secretProtector.IsProtectedWithCurrentKey(row.ProtectedSecret ?? ""));
            var shouldUpdateName = !string.Equals(name, row.Name, StringComparison.Ordinal);

            if (!shouldUpdateName && !shouldUpdateConnection)
                continue;

            using var update = connection.CreateCommand();
            update.CommandText = @"
                UPDATE data_sources
                SET name = $name,
                    connection_string = CASE WHEN $update_connection = 1 THEN $connection_mask ELSE connection_string END,
                    connection_secret = CASE WHEN $update_connection = 1 THEN $connection_secret ELSE connection_secret END,
                    updated_at = $updated_at
                WHERE id = $id";
            update.Parameters.AddWithValue("$id", row.Id);
            update.Parameters.AddWithValue("$name", name);
            update.Parameters.AddWithValue("$update_connection", shouldUpdateConnection ? 1 : 0);
            update.Parameters.AddWithValue("$connection_mask", shouldUpdateConnection
                ? SecretProtector.MaskConnectionString(clearConnectionString!)
                : row.ConnectionMask);
            update.Parameters.AddWithValue("$connection_secret", shouldUpdateConnection
                ? _secretProtector.Protect(clearConnectionString!)
                : (object?)row.ProtectedSecret ?? DBNull.Value);
            update.Parameters.AddWithValue("$updated_at", DateTime.UtcNow.ToString("O"));
            update.ExecuteNonQuery();
        }
    }

    private static bool IsLegacyAnalyticsConnectionString(string? value) =>
        !string.IsNullOrWhiteSpace(value) &&
        value.Contains(LegacyAnalyticsDbFileName(), StringComparison.OrdinalIgnoreCase);

    private static string LegacyDemoDataSourceName() =>
        string.Concat("Dri", "vee demo dataset");

    private static string LegacyAnalyticsDbFileName() =>
        string.Concat("dri", "vee.db");

    private int NormalizeCompanyId(int? companyId) =>
        (companyId ?? _tenantContext.CompanyId) <= 0 ? CompanyDefaults.DefaultCompanyId : (companyId ?? _tenantContext.CompanyId);

    private CompanyDataSource? GetForUpdate(SqliteConnection connection, SqliteTransaction transaction, int id)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = @"
            SELECT id, company_id, name, provider, connection_string, connection_secret, semantic_json, schema_json, is_builtin, is_active,
                   created_at, updated_at, last_validated_at, last_validation_error
            FROM data_sources
            WHERE id = $id";
        command.Parameters.AddWithValue("$id", id);
        return ReadDataSource(command, exposeSecret: true);
    }

    private CompanyDataSource? GetCore(SqliteConnection connection, int id, int companyId, bool exposeSecret)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, company_id, name, provider, connection_string, connection_secret, semantic_json, schema_json, is_builtin, is_active,
                   created_at, updated_at, last_validated_at, last_validation_error
            FROM data_sources
            WHERE id = $id AND company_id = $company_id";
        command.Parameters.AddWithValue("$id", id);
        command.Parameters.AddWithValue("$company_id", companyId);
        return ReadDataSource(command, exposeSecret);
    }

    private List<CompanyDataSource> ReadDataSources(SqliteCommand command, bool exposeSecret)
    {
        var items = new List<CompanyDataSource>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
            items.Add(ReadDataSource(reader, exposeSecret));
        return items;
    }

    private CompanyDataSource? ReadDataSource(SqliteCommand command, bool exposeSecret)
    {
        using var reader = command.ExecuteReader();
        return reader.Read() ? ReadDataSource(reader, exposeSecret) : null;
    }

    private CompanyDataSource ReadDataSource(SqliteDataReader reader, bool exposeSecret)
    {
        var connectionMask = reader.GetString(4);
        var protectedSecret = reader.IsDBNull(5) ? null : reader.GetString(5);
        var connectionString = connectionMask;
        if (exposeSecret)
        {
            if (string.IsNullOrWhiteSpace(protectedSecret))
            {
                connectionString = connectionMask;
            }
            else if (!_secretProtector.TryUnprotect(protectedSecret, out connectionString))
            {
                throw new InvalidOperationException(
                    $"Connection secret for data source `{reader.GetString(2)}` cannot be decrypted with configured keys.");
            }
        }

        return new CompanyDataSource
        {
            Id = reader.GetInt32(0),
            CompanyId = reader.GetInt32(1),
            Name = reader.GetString(2),
            Provider = reader.GetString(3),
            ConnectionString = exposeSecret ? connectionString : connectionMask,
            IsConnectionStringMasked = !exposeSecret,
            SemanticJson = reader.IsDBNull(6) ? null : reader.GetString(6),
            SchemaJson = reader.IsDBNull(7) ? null : reader.GetString(7),
            IsBuiltin = reader.GetInt64(8) == 1,
            IsActive = reader.GetInt64(9) == 1,
            CreatedAt = ParseDateTime(reader.GetString(10)),
            UpdatedAt = ParseDateTime(reader.GetString(11)),
            LastValidatedAt = reader.IsDBNull(12) ? null : ParseDateTime(reader.GetString(12)),
            LastValidationError = reader.IsDBNull(13) ? null : reader.GetString(13)
        };
    }

    private SqliteConnection OpenMetadataConnection()
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = _dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = true
        }.ToString();

        var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA busy_timeout = {BusyTimeoutMs};";
        command.ExecuteNonQuery();
        return connection;
    }

    private static void EnsurePragmas(SqliteConnection connection)
    {
        using var journalCommand = connection.CreateCommand();
        journalCommand.CommandText = "PRAGMA journal_mode = WAL;";
        _ = journalCommand.ExecuteScalar();
    }

    private static void EnsureColumn(SqliteConnection connection, string tableName, string columnName, string definition)
    {
        using var tableInfo = connection.CreateCommand();
        tableInfo.CommandText = $"PRAGMA table_info({tableName});";

        using (var reader = tableInfo.ExecuteReader())
        {
            while (reader.Read())
            {
                if (string.Equals(reader.GetString(1), columnName, StringComparison.OrdinalIgnoreCase))
                    return;
            }
        }

        using var alter = connection.CreateCommand();
        alter.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {definition};";
        alter.ExecuteNonQuery();
    }

    private static DateTime ParseDateTime(string value) =>
        DateTime.TryParse(value, out var parsed) ? parsed : DateTime.UtcNow;
}
