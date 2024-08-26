using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

[ExtensionDescription(
    "Provides access to databases with WindCube files.",
    "https://github.com/Apollo3zehn/nexus-sources-windcube",
    "https://github.com/Apollo3zehn/nexus-sources-windcube")]
public partial class WindCube : StructuredFileDataSource
{
    record CatalogDescription(
        string Title,
        Dictionary<string, IReadOnlyList<FileSource>> FileSourceGroups,
        JsonElement? AdditionalProperties);

    #region Fields

    private readonly string _inFileDateFormat = "yyyy/MM/dd HH:mm";
    private readonly Encoding _encoding;
    private readonly NumberFormatInfo _nfi;
    private Dictionary<string, CatalogDescription> _config = default!;

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

    protected override async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var configFilePath = Path.Combine(Root, "config.json");

        if (!File.Exists(configFilePath))
            throw new Exception($"Configuration file {configFilePath} not found.");

        var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
        _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
    }

    protected override Task<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>> GetFileSourceProviderAsync(
        CancellationToken cancellationToken)
    {
        return Task.FromResult<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>>(
            catalogId => _config[catalogId].FileSourceGroups);
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        if (path == "/")
            return Task.FromResult(_config.Select(entry => new CatalogRegistration(entry.Key, entry.Value.Title)).ToArray());

        else
            return Task.FromResult(Array.Empty<CatalogRegistration>());
    }

    protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
    {
        var catalogDescription = _config[catalogId];
        var catalog = new ResourceCatalog(id: catalogId);

        foreach (var (fileSourceId, fileSourceGroup) in catalogDescription.FileSourceGroups)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var filePaths = default(string[]);
                var catalogSourceFiles = fileSource.AdditionalProperties?.GetStringArray("CatalogSourceFiles");

                if (catalogSourceFiles is not null)
                {
                    filePaths = catalogSourceFiles
                        .Where(filePath => filePath is not null)
                        .Select(filePath => Path.Combine(Root, filePath!))
                        .ToArray();
                }

                else
                {
                    if (!TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = [filePath];
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    using var wcFile = new StreamReader(File.OpenRead(filePath), _encoding);
                    ReadHeader(wcFile);

                    var resources = GetResources(wcFile, fileSourceId);

                    var newCatalog = new ResourceCatalogBuilder(id: catalogId)
                        .AddResources(resources)
                        .Build();

                    catalog = catalog.Merge(newCatalog);
                }
            }
        }

        return Task.FromResult(catalog);
    }

    protected override Task<double> GetFileAvailabilityAsync(string filePath, CancellationToken cancellationToken)
    {
        var rowCount = 0;
        var lines = File.ReadAllLines(filePath);
        var headerSize = int.Parse(HeaderSizeRegex().Match(lines.First()).Value);

        foreach (var line in lines.Skip(headerSize + 2))
        {
            if (DateTime.TryParseExact(line[..16], _inFileDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dateTime))
                rowCount++;
        }

        return Task.FromResult(rowCount / 144.0);
    }

    protected override Task ReadAsync(ReadInfo info, StructuredFileReadRequest[] readRequests, CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            foreach (var readRequest in readRequests)
            {
                // read data
                var baseUnit = TimeSpan.FromMinutes(10);
                var lines = File.ReadAllLines(info.FilePath, _encoding);
                var headerSize = int.Parse(HeaderSizeRegex().Match(lines.First()).Value);

                if (lines.Length <= headerSize + 1)
                {
                    Logger.LogDebug("The content of file {FilePath} is invalid", info.FilePath);
                    return;
                }

                var headline = lines[headerSize + 1];

                var column = headline
                    .Split('\t')
                    .ToList()
                    .FindIndex(value => value == readRequest.OriginalName);

                if (column > -1)
                {
                    foreach (var line in lines.Skip(headerSize + 2))
                    {
                        if (DateTime.TryParseExact(line[..16], _inFileDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var rowTimeStamp))
                        {
                            rowTimeStamp = rowTimeStamp.AddMinutes(-10).ToUniversalTime();

                            var index = (int)Math.Floor((rowTimeStamp - info.RegularFileBegin).Ticks / (double)baseUnit.Ticks);

                            if (index < 0 || index >= readRequest.Data.Length)
                                continue;

                            var rawValue = line.Split('\t')[column];
                            var value = double.Parse(rawValue, _nfi);
                            var destination = readRequest.Data[(index * readRequest.CatalogItem.Representation.ElementSize)..];

                            BitConverter.GetBytes(value)
                                .CopyTo(destination);

                            readRequest.Status.Span[index] = 1;
                        }
                    }
                }
                else
                {
                    Logger.LogDebug("Could not find representation {ResourcePath}", readRequest.CatalogItem.ToPath());
                }
            }
        }, cancellationToken);
    }

    private static void ReadHeader(StreamReader wcFile)
    {
        var firstLine = wcFile.ReadLine() ?? throw new Exception("first line is null");
        var headerSize = int.Parse(HeaderSizeRegex().Match(firstLine).Value);

        for (int i = 0; i < headerSize; i++)
        {
            wcFile.ReadLine();
        }
    }

    private static List<Resource> GetResources(StreamReader wcFile, string fileSourceId)
    {
        var line = wcFile.ReadLine() ?? throw new Exception("line is null");
        var resources = line.Split('\t')
            .Skip(1)
            .Where(originalName => !string.IsNullOrWhiteSpace(originalName))
            .Select(originalName =>
            {
                var samplePeriod = TimeSpan.FromMinutes(10);

                var representation = new Representation(
                    dataType: NexusDataType.FLOAT64,
                    samplePeriod: samplePeriod);

                var match = MyRegex().Match(originalName);
                var rawName = match.Success ? match.Groups[1].Value : originalName;

                if (!TryEnforceNamingConvention("WC_" + rawName, out var resourceId))
                    throw new Exception($"The name {"WC_" + rawName} is not a valid resource id.");

                var unit = match.Success ? match.Groups[2].Value : string.Empty;

                var groupMatch = Regex.Match(originalName, "([0-9]+m)");
                var group = default(string);

                if (groupMatch.Success)
                    group = groupMatch.Groups[0].Value;

                else
                    group = "Environment";

                var resource = new ResourceBuilder(id: resourceId)
                    .WithUnit(unit)
                    .WithGroups(group)
                    .WithFileSourceId(fileSourceId)
                    .WithOriginalName(originalName)
                    .AddRepresentation(representation)
                    .Build();

                return resource;
            }).ToList();

        return resources;
    }

    private static bool TryEnforceNamingConvention(string resourceId, [NotNullWhen(returnValue: true)] out string newResourceId)
    {
        newResourceId = resourceId;
        newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "_");
        newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "_");

        return Resource.ValidIdExpression.IsMatch(newResourceId);
    }

    [GeneratedRegex("[0-9]+")]
    private static partial Regex HeaderSizeRegex();
    [GeneratedRegex(@"(.*)\s\((.*)\)")]
    private static partial Regex MyRegex();

    #endregion
}
