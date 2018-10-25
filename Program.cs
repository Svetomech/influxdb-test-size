using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using InfluxData.Net.Common.Enums;
using InfluxData.Net.Common.Helpers;
using InfluxData.Net.InfluxDb;
using InfluxData.Net.InfluxDb.Models;

namespace InfluxDb._1secBench
{
  class Program
  {
    const int TanksCount = 30;
    const int Days = 365;
    const int BatchSize = 50_000;

    static readonly Guid[] _tanksIds = GetTanksIds();
    static readonly Random _rnd = new Random();
    static int _ticks = 0;

    static async Task Main(string[] args)
    {
      var client = Setup();
      await client.Database.CreateDatabaseAsync("CIPTanks");

      var readingCount = TanksCount * (long)TimeSpan.FromDays(Days).TotalSeconds;
      var batchSteps = readingCount / BatchSize;

      using (var progress = new ProgressBar())
      {
        for (var i = 0L; i < batchSteps; ++i)
        {
          progress.Report((double)i / batchSteps);
          await WriteBatch(client, BatchSize);
        }
      }

      var lastBatchSize = readingCount - batchSteps * BatchSize;
      await WriteBatch(client, lastBatchSize);

      Console.WriteLine($"{readingCount} series successfully written!");
    }

    static async Task WriteBatch(InfluxDbClient client, long batchSize)
    {
      var readings = new List<Point>();
      for (var j = 0L; j < batchSize; ++j)
      {
        readings.Add(GenerateReading());
      }
      await client.Client.WriteAsync(readings, "CIPTanks");
    }

    static Point GenerateReading()
    {
      var reading = new Point()
      {
        Name = "reading",
        Timestamp = DateTime.Now.AddDays(-Days).AddSeconds(-_ticks++),
        Tags = new Dictionary<string, object>()
        {
          { "CIPTankId", _tanksIds[_rnd.Next(0, TanksCount)] }
        },
        Fields = new Dictionary<string, object>()
        {
          { "Humidity", _rnd.Next(40, 50+1) },
          { "Humidity2", _rnd.Next(40, 50+1) },
          { "Temperature", _rnd.Next(75, 115+1) },
          { "Temperature2", _rnd.Next(75, 115+1) },
          { "FlipCount", _rnd.Next(1, 3+1) }
        }
      };
      return reading;
    }

    static InfluxDbClient Setup()
    {
      var client = new InfluxDbClient("http://localhost:8086/", "", "",
        InfluxDbVersion.Latest);
      return client;
    }

    static Guid[] GetTanksIds()
    {
      var ids = new Guid[TanksCount];
      for (int i = 0; i < ids.Length; ++i)
      {
        ids[i] = Guid.NewGuid();
      }
      return ids;
    }
  }
}
