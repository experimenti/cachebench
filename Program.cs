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
                BenchmarkSwitcher.FromAssembly(typeof(ConcurrentDictionaryCache).Assembly).Run(new string[] { DataFile });
            }
            else
            {
                if (CacheType.Equals("ParallelDictionary"))
                {
                    ConcurrentDictionaryCache pdc = new ConcurrentDictionaryCache(DataFile);
                    pdc.QueryCache();
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

        [Params(1000, 100000)]
        public int LOOKUP_COUNT;

        int initialCapacity = 100000;
        static int numProcs = Environment.ProcessorCount;
        int concurrencyLevel = numProcs * 6;

        ConcurrentDictionary<Guid, ConcurrentBag<Guid>> cd;

        public string DataFile { get; set; } = "./relationships_small.csv";

        public ConcurrentDictionaryCache()
        {
        }


        public ConcurrentDictionaryCache(string datafile)
        {
            DataFile = datafile;

        }


        [Benchmark]
        public ConcurrentDictionary<Guid, ConcurrentBag<Guid>> BuildCache()
        {

            cd = new ConcurrentDictionary<Guid, ConcurrentBag<Guid>>(concurrencyLevel, initialCapacity);

            FileInfo fInfo = new FileInfo(DataFile);

            if (!fInfo.Exists)
            {
                throw new Exception("Datafile does not exist");
            }

            using (var reader = new StreamReader(DataFile))
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

                    //generate benchmark range
                    if (x < LOOKUP_COUNT)
                    {
                        cd.AddOrUpdate<Guid>(currentRecord.FromGuid, addListFunc, addUpdateListFunc, currentRecord.ToGuid);
                    }

                });
            }

            return cd;
        }

        [Benchmark]
        public void QueryCache()
        {

            var cd = BuildCache();




            System.Threading.Tasks.Parallel.ForEach(cd, (y) =>
            {
                var existants = cd.TryGetValue(y.Key, out ConcurrentBag<Guid> toGuids);

                if (existants && toGuids.Count > 1)
                {
                    //Do just a bit of work here
                    if (toGuids.Count > 5)
                    {
                        Console.WriteLine("Large(ish) relationship. Guid {0}, has {1} related guids", y.Key, toGuids.Count);

                        foreach (var h in toGuids)
                        {
                            Console.WriteLine("Guid {0}, has to guid {1} ", y.Key, h);
                        }
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
