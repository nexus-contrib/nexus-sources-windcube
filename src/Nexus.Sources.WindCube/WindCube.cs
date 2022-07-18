using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Nexus.Sources
{
    [ExtensionDescription(
        "Provides access to databases with WindCube files.",
        "https://github.com/Apollo3zehn/nexus-sources-windcube",
        "https://github.com/Apollo3zehn/nexus-sources-windcube")]
    public class WindCube : StructuredFileDataSource
    {
        #region Fields

        private string _inFileDateFormat = "yyyy/MM/dd HH:mm";
        private Encoding _encoding;
        private Dictionary<string, CatalogDescription> _config = default!;
        private NumberFormatInfo _nfi;

        #endregion

        #region Properties

        private DataSourceContext Context { get; set; } = default!;

        private ILogger Logger { get; set; } = default!;

        #endregion

        #region Constructors

        public WindCube()
        {
            _nfi = new NumberFormatInfo()
            {
                NumberDecimalSeparator = ".",
                NumberGroupSeparator = string.Empty
            };

            _encoding = CodePagesEncodingProvider.Instance.GetEncoding(1252) ?? throw new Exception("encoding is null");
        }

        #endregion

        #region Methods

        protected override async Task SetContextAsync(DataSourceContext context, ILogger logger, CancellationToken cancellationToken)
        {
            this.Context = context;
            this.Logger = logger;

            var configFilePath = Path.Combine(this.Root, "config.json");

            if (!File.Exists(configFilePath))
                throw new Exception($"Configuration file {configFilePath} not found.");

            var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
            _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");            
        }

        protected override Task<FileSourceProvider> GetFileSourceProviderAsync(CancellationToken cancellationToken)
        {
            var allFileSources = _config.ToDictionary(
                config => config.Key,
                config => config.Value.FileSources.Cast<FileSource>().ToArray());

            var fileSourceProvider = new FileSourceProvider(
                All: allFileSources,
                Single: catalogItem =>
                {
                    var properties = catalogItem.Resource.Properties;

                    if (properties is null)
                        throw new ArgumentNullException(nameof(properties));

                    var fileSourceName = properties.Value.GetProperty("FileSource").GetString();

                    return allFileSources[catalogItem.Catalog.Id]
                        .First(fileSource => ((ExtendedFileSource)fileSource).Name == fileSourceName);
                });

            return Task.FromResult(fileSourceProvider);
        }

        protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
        {
            if (path == "/")
                return Task.FromResult(_config.Select(entry => new CatalogRegistration(entry.Key, entry.Value.Title)).ToArray());

            else
                return Task.FromResult(new CatalogRegistration[0]);
        }

        protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var catalogDescription = _config[catalogId];
            var catalog = new ResourceCatalog(id: catalogId);

            foreach (var fileSource in catalogDescription.FileSources)
            {
                var filePaths = default(string[]);

                if (fileSource.CatalogSourceFiles is not null)
                {
                    filePaths = fileSource.CatalogSourceFiles
                        .Select(filePath => Path.Combine(this.Root, filePath))
                        .ToArray();
                }
                else
                {
                    if (!this.TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = new[] { filePath };
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    using var wcFile = new StreamReader(File.OpenRead(filePath), _encoding);
                    this.ReadHeader(wcFile);

                    var resources = this.GetResources(wcFile, fileSource);

                    var newCatalog = new ResourceCatalogBuilder(id: catalogId)
                        .AddResources(resources)
                        .Build();

                    catalog = catalog.Merge(newCatalog);
                }
            }

            return Task.FromResult(catalog);
        }

        protected override Task<double> GetFileAvailabilityAsync(string filePath, CancellationToken cancellationToken)
        {
            var rowCount = 0;
            var lines = File.ReadAllLines(filePath);
            var headerSize = int.Parse(Regex.Match(lines.First(), "[0-9]+").Value);

            foreach (var line in lines.Skip(headerSize + 2))
            {
                if (DateTime.TryParseExact(line.Substring(0, 16), _inFileDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dateTime))
                    rowCount++;
            }

            return Task.FromResult(rowCount / 144.0);
        }

        protected override Task ReadSingleAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                // read data
                var baseUnit = TimeSpan.FromMinutes(10);
                var lines = File.ReadAllLines(info.FilePath);
                var headerSize = int.Parse(Regex.Match(lines.First(), "[0-9]+").Value);

                if (lines.Length <= headerSize + 1)
                    continue;

                var headline = lines[headerSize + 1];

                var column = headline
                    .Split('\t')
                    .Select(rawName => Utilities.EnforceNamingConvention("WC_" + rawName))
                    .ToList()
                    .FindIndex(value => value.StartsWith(info.CatalogItem.Resource.Id));

                if (column > -1)
                {
                    foreach (var line in lines.Skip(headerSize + 2))
                    {
                        if (DateTime.TryParseExact(line.Substring(0, 16), _inFileDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var rowTimeStamp))
                        {
                            rowTimeStamp = rowTimeStamp.AddMinutes(-10).ToUniversalTime();

                            var index = (int)Math.Floor((rowTimeStamp - info.FileBegin).Ticks / (double)baseUnit.Ticks);

                            if (index < 0 || index >= info.Data.Length)
                                continue;

                            var rawValue = line.Split('\t')[column];
                            var value = double.Parse(rawValue, _nfi);
                            var destination = info.Data.Slice(index * info.CatalogItem.Representation.ElementSize);

                            BitConverter.GetBytes(value)
                                .CopyTo(destination);

                            info.Status.Span[index] = 1;
                        }
                    }
                }
                else
                {
                    this.Logger.LogDebug("Could not find representation {ResourcePath}", info.CatalogItem.ToPath());
                }
            });
        }

        private void ReadHeader(StreamReader wcFile)
        {
            var firstLine = wcFile.ReadLine();

            if (firstLine is null)
                throw new Exception("first line is null");

            var headerSize = int.Parse(Regex.Match(firstLine, "[0-9]+").Value);

            for (int i = 0; i < headerSize; i++)
            {
                wcFile.ReadLine();
            }
        }

        private List<Resource> GetResources(StreamReader wcFile, ExtendedFileSource fileSource)
        {
            var line = wcFile.ReadLine();

            if (line is null)
                throw new Exception("line is null");

            var resources = line.Split('\t')
                .Skip(1)
                .Where(value => !string.IsNullOrWhiteSpace(value))
                .Select(value =>
                {
                    var samplePeriod = TimeSpan.FromMinutes(10);

                    var representation = new Representation(
                        dataType: NexusDataType.FLOAT64,
                        samplePeriod: samplePeriod);

                    var match = Regex.Match(value, @"(.*)\s\((.*)\)");
                    var rawName = match.Success ? match.Groups[1].Value : value;
                    var id = Utilities.EnforceNamingConvention("WC_" + rawName);
                    var unit = match.Success ? match.Groups[2].Value : string.Empty;

                    var groupMatch = Regex.Match(value, "([0-9]+m)");
                    var group = default(string);

                    if (groupMatch.Success)
                        group = groupMatch.Groups[0].Value;

                    else
                        group = "Environment";

                    var resource = new ResourceBuilder(id: id)
                        .WithUnit(unit)
                        .WithGroups(group)
                        .WithProperty("FileSource", fileSource.Name)
                        .AddRepresentation(representation)
                        .Build();

                    return resource;
                }).ToList();

            return resources;
        }

        #endregion
    }
}
