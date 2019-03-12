using System;
using System.Security.Permissions;
using System.Collections.Concurrent;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using System.IO;
using CsvHelper;
using McMaster.Extensions.CommandLineUtils;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;

namespace CacheBench
{

    [Command(Name = "CacheBench", Description = "A simple set of benchmarks for relationship data caches.")]
    [HelpOption("-?")]
    public class Program
    {
        [Option(Description = "Run Benchmarks (run if -b is used)")]
        public Boolean Benchmark { get; }

        [Option("-t|--type", Description = "[ParallelDictionary|FASTER]")]
        public string CacheType { get; }

        [Option("-d|--datafile", Description = "csv data file for load")]
        public string DataFile { get; } = "./relationships_small.csv";

        public static void Main(string[] args)
            => CommandLineApplication.Execute<Program>(args);

        private void OnExecute(CommandLineApplication app)
        {
            if (string.IsNullOrEmpty(CacheType))
            {
                app.ShowHelp();
                return;
            }

            if (Benchmark)
            {
                Console.WriteLine($"Running Benchmarks for {CacheType}!");
                BenchmarkRunner.Run<ConcurrentDictionaryCache>();
            }
            else
            {
                if (CacheType.Equals("ParallelDictionary"))
                {
                    Console.WriteLine($"Starting paraellel dictionary workload.");
                    ConcurrentDictionaryCache pdc = new ConcurrentDictionaryCache();
                    pdc.BuildCache(DataFile);
                }
                else if (CacheType.Equals("FASTER"))
                {
                    Console.WriteLine($"Starting FASTER cache workload.");
                    throw new NotImplementedException();
                }
                else
                {
                    Console.WriteLine($"Undefined CacheType {CacheType}!");
                }
            }
        }
    }

    [ClrJob(baseline: true), CoreJob]
    [RPlotExporter, RankColumn]
    public class ConcurrentDictionaryCache
    {
        int initialCapacity = 100000;
        static int numProcs = Environment.ProcessorCount;
        int concurrencyLevel = numProcs * 4;

        ConcurrentDictionary<Guid, ConcurrentBag<Guid>> cd;

        [Benchmark]
        public void BuildCache(string DataFile)
        {

            Console.WriteLine($"Starting paraellel dictionary workload.");
            cd = new ConcurrentDictionary<Guid, ConcurrentBag<Guid>>(concurrencyLevel, initialCapacity);

            FileInfo fInfo = new FileInfo(DataFile);

            if (!fInfo.Exists)
            {
                Console.Out.WriteLine("DataFile does not exist.");
                return;
            }

            using (var reader = new StreamReader("./relationships_small.csv"))
            using (var csv = new CsvReader(reader))
            {
                csv.Configuration.HasHeaderRecord = false;
                var records = csv.GetRecords<RelationshipRecord>();

                Func<Guid, ConcurrentBag<Guid>, Guid, ConcurrentBag<Guid>> addUpdateListFunc = ((g, k, l) =>
                {
                    k.Add(l);
                    return k;
                });

                Func<Guid, Guid, ConcurrentBag<Guid>> addListFunc = ((k, v) =>
                {
                    var nb = new ConcurrentBag<Guid>();
                    nb.Add(v);
                    return nb;
                });

                int x = 0;

                System.Threading.Tasks.Parallel.ForEach(records, (currentRecord) =>
                {
                    x++;

                    if (x%10 == 0)
                    {
                        Console.WriteLine("A Processing Record {0} ", x);
                    }

                    cd.AddOrUpdate<Guid>(currentRecord.FromGuid, addListFunc, addUpdateListFunc, currentRecord.ToGuid);

                });
            }
        }

        [Benchmark]
        public void QueryCache()
        {

            System.Threading.Tasks.Parallel.ForEach(cd, y =>
            {
                ConcurrentBag<Guid> toGuids;
                var lookupGuid = y.Key;

                var existants = cd.TryGetValue(lookupGuid, out toGuids);

                if (existants && toGuids.Count > 0)
                {
                    foreach (var h in toGuids)
                    {
                        Console.WriteLine("Guid {0}, has {1} related guids", lookupGuid, h);
                    }
                }
            });
        }
    }


    public class RelationshipRecord
    {
        public Guid RelationshipGuid { get; set; }
        public Guid FromGuid { get; set; }
        public Guid ToGuid { get; set; }
        public int RelationshipSubType { get; set; }
    }
}
