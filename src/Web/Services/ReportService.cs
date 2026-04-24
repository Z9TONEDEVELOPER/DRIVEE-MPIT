using DriveeDataSpace.Web.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Web.Services;

public class ReportService
{
    private const int BusyTimeoutMs = 5_000;
    private readonly string _db;

    public ReportService(IConfiguration cfg, IHostEnvironment environment)
    {
        _db = DataPathResolver.Resolve(environment, cfg["Data:ReportsDb"], "Data/reports.db");
        var dir = Path.GetDirectoryName(_db);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        EnsureSchema();
    }

    private void EnsureSchema()
    {
        using var c = OpenConnection();
        EnsurePragmas(c);
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS reports (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                name          TEXT NOT NULL,
                user_query    TEXT NOT NULL,
                intent_json   TEXT NOT NULL,
                sql_text      TEXT NOT NULL,
                visualization TEXT NOT NULL,
                author        TEXT NOT NULL,
                created_at    TEXT NOT NULL
            );";
        cmd.ExecuteNonQuery();
    }

    public int Save(Report r)
    {
        using var c = OpenConnection();
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"INSERT INTO reports(name,user_query,intent_json,sql_text,visualization,author,created_at)
                            VALUES($n,$q,$i,$s,$v,$a,$t); SELECT last_insert_rowid();";
        cmd.Parameters.AddWithValue("$n", r.Name);
        cmd.Parameters.AddWithValue("$q", r.UserQuery);
        cmd.Parameters.AddWithValue("$i", r.IntentJson);
        cmd.Parameters.AddWithValue("$s", r.Sql);
        cmd.Parameters.AddWithValue("$v", r.Visualization);
        cmd.Parameters.AddWithValue("$a", r.Author);
        cmd.Parameters.AddWithValue("$t", r.CreatedAt.ToString("O"));
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    public List<Report> List()
    {
        using var c = OpenConnection();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT id,name,user_query,intent_json,sql_text,visualization,author,created_at FROM reports ORDER BY id DESC";
        return ReadReports(cmd);
    }

    public List<Report> ListForAuthor(string author)
    {
        using var c = OpenConnection();
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"
            SELECT id,name,user_query,intent_json,sql_text,visualization,author,created_at
            FROM reports
            WHERE lower(author) = $author
            ORDER BY id DESC";
        cmd.Parameters.AddWithValue("$author", NormalizeAuthor(author));
        return ReadReports(cmd);
    }

    public Report? Get(int id)
    {
        using var c = OpenConnection();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT id,name,user_query,intent_json,sql_text,visualization,author,created_at FROM reports WHERE id=$id";
        cmd.Parameters.AddWithValue("$id", id);
        return ReadSingle(cmd);
    }

    public Report? GetForAuthor(int id, string author, bool isAdmin)
    {
        using var c = OpenConnection();
        using var cmd = c.CreateCommand();
        cmd.CommandText = isAdmin
            ? "SELECT id,name,user_query,intent_json,sql_text,visualization,author,created_at FROM reports WHERE id=$id"
            : "SELECT id,name,user_query,intent_json,sql_text,visualization,author,created_at FROM reports WHERE id=$id AND lower(author)=$author";
        cmd.Parameters.AddWithValue("$id", id);
        if (!isAdmin)
        {
            cmd.Parameters.AddWithValue("$author", NormalizeAuthor(author));
        }

        return ReadSingle(cmd);
    }

    public void Delete(int id)
    {
        using var c = OpenConnection();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "DELETE FROM reports WHERE id=$id";
        cmd.Parameters.AddWithValue("$id", id);
        cmd.ExecuteNonQuery();
    }

    public void DeleteForAuthor(int id, string author, bool isAdmin)
    {
        using var c = OpenConnection();
        using var cmd = c.CreateCommand();
        cmd.CommandText = isAdmin
            ? "DELETE FROM reports WHERE id=$id"
            : "DELETE FROM reports WHERE id=$id AND lower(author)=$author";
        cmd.Parameters.AddWithValue("$id", id);
        if (!isAdmin)
        {
            cmd.Parameters.AddWithValue("$author", NormalizeAuthor(author));
        }

        cmd.ExecuteNonQuery();
    }

    private static List<Report> ReadReports(SqliteCommand cmd)
    {
        var list = new List<Report>();
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            list.Add(ReadReport(r));
        }

        return list;
    }

    private static Report? ReadSingle(SqliteCommand cmd)
    {
        using var r = cmd.ExecuteReader();
        return r.Read() ? ReadReport(r) : null;
    }

    private static Report ReadReport(SqliteDataReader r) =>
        new()
        {
            Id = r.GetInt32(0),
            Name = r.GetString(1),
            UserQuery = r.GetString(2),
            IntentJson = r.GetString(3),
            Sql = r.GetString(4),
            Visualization = r.GetString(5),
            Author = r.GetString(6),
            CreatedAt = DateTime.Parse(r.GetString(7))
        };

    private static string NormalizeAuthor(string author) =>
        author.Trim().ToLowerInvariant();

    private SqliteConnection OpenConnection()
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = _db,
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
}
