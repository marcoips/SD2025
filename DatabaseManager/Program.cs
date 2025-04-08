using Microsoft.Data.Sqlite;
using System;
using System.Data.SQLite;

class DatabaseManager
{
    private string connectionString;

    public DatabaseManager(string dbFilePath)
    {
        connectionString = $"Data Source={dbFilePath};Version=3;";
        InitializeDatabase();
    }

    private void InitializeDatabase()
    {
        using (var connection = new SqliteConnection(connectionString))
        {
            connection.Open();
            string createTableQuery = @"
                CREATE TABLE IF NOT EXISTS WavyData (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    WavyId TEXT NOT NULL,
                    Data TEXT NOT NULL,
                    Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )";
            using (var command = new SqliteCommand(createTableQuery, connection))
            {
                command.ExecuteNonQuery();
            }
        }
    }

    public void InsertData(string wavyId, string data)
    {
        using (var connection = new SqliteConnection(connectionString))
        {
            connection.Open();
            string insertQuery = "INSERT INTO WavyData (WavyId, Data) VALUES (@wavyId, @data)";
            using (var command = new SqliteCommand(insertQuery, connection))
            {
                command.Parameters.AddWithValue("@wavyId", wavyId);
                command.Parameters.AddWithValue("@data", data);
                command.ExecuteNonQuery();
            }
        }
    }
}