using System;
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

    enum SCENARIO { ParallelDictionary, FASTER };


    [Command(Name = "CacheBench", Description = "A simple set of benchmarks for relationship data caches.")]
    [HelpOption("-?")]
    public class Program
    {

        [Option(Description = "Run Benchmarks (run if -b is used)")]
        public Boolean Benchmark { get; }

        [Option("-t|--type", Description = "[ParallelDictionary|FASTER]")]
        public string CacheType { get; }

        public static Task<int> Main(string[] args)
            => CommandLineApplication.ExecuteAsync<Program>(args);

        private async Task<int> OnExecuteAsync(CommandLineApplication app)
        {

            if (string.IsNullOrEmpty(CacheType))
            {
                app.ShowHelp();
                return 0;
            }

            if (Benchmark)
            {
                Console.WriteLine($"Running Benchmarks for {CacheType}!");
                BenchmarkRunner.Run<ParallelDictionaryCache>();
            }
            else
            {
                if (CacheType.Equals("ParallelDictionary"))
                {
                    Console.WriteLine($"Starting paraellel dictionary workload.");

                    ParallelDictionaryCache pdc = new ParallelDictionaryCache();

                    await Task.Run(async () =>
                    {
                        await pdc.BuildCache();
                    });
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
            return 1;
        }
    }

    [ClrJob(baseline: true), CoreJob]
    [RPlotExporter, RankColumn]
    public class ParallelDictionaryCache
    {
        public ParallelDictionaryCache()
        {
            //BuildCache();
        }

        public ParallelDictionaryCache(string[] args)
        {
            //BuildCache();
        }

        [Benchmark]
        public async Task BuildCache()
        {
            int initialCapacity = 6000000;
            int numProcs = Environment.ProcessorCount;
            int concurrencyLevel = numProcs * 4;

            ConcurrentDictionary<Guid, ConcurrentBag<Guid>> cd = new ConcurrentDictionary<Guid, ConcurrentBag<Guid>>(concurrencyLevel, initialCapacity);


            using (var reader = new StreamReader("./relationships_eleven.csv"))
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

                        if (x % 1 == 0)
                        {
                            Console.WriteLine("Processing Record {0} ", x);
                        }

                        cd.AddOrUpdate<Guid>(currentRecord.FromGuid, addListFunc, addUpdateListFunc, currentRecord.ToGuid);

                    });
            }
        }

        [Benchmark]
        public async Task QueryCache(ConcurrentDictionary<Guid, ConcurrentBag<Guid>> pdc)
        {

            System.Threading.Tasks.Parallel.ForEach(pdc, y =>
            {
                ConcurrentBag<Guid> toGuids;
                var lookupGuid = y.Key;

                var existants = pdc.TryGetValue(lookupGuid, out toGuids);

                if (toGuids.Count > 10)
                {
                    Console.WriteLine("Guid {0}, has {1} related guids");
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
