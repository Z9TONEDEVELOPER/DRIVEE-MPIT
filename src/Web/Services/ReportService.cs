using DriveeDataSpace.Web.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Web.Services;

public class ReportService
{
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
        using var c = new SqliteConnection($"Data Source={_db}");
        c.Open();
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
        using var c = new SqliteConnection($"Data Source={_db}");
        c.Open();
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
        var list = new List<Report>();
        using var c = new SqliteConnection($"Data Source={_db}");
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT id,name,user_query,intent_json,sql_text,visualization,author,created_at FROM reports ORDER BY id DESC";
        using var r = cmd.ExecuteReader();
        while (r.Read())
        {
            list.Add(new Report
            {
                Id = r.GetInt32(0),
                Name = r.GetString(1),
                UserQuery = r.GetString(2),
                IntentJson = r.GetString(3),
                Sql = r.GetString(4),
                Visualization = r.GetString(5),
                Author = r.GetString(6),
                CreatedAt = DateTime.Parse(r.GetString(7))
            });
        }
        return list;
    }

    public Report? Get(int id)
    {
        using var c = new SqliteConnection($"Data Source={_db}");
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT id,name,user_query,intent_json,sql_text,visualization,author,created_at FROM reports WHERE id=$id";
        cmd.Parameters.AddWithValue("$id", id);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return new Report
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
    }

    public void Delete(int id)
    {
        using var c = new SqliteConnection($"Data Source={_db}");
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "DELETE FROM reports WHERE id=$id";
        cmd.Parameters.AddWithValue("$id", id);
        cmd.ExecuteNonQuery();
    }
}
