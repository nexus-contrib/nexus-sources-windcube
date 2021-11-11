using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Nexus.Sources.WindCube.Tests
{
    public class WindCubeDataSourceTests
    {
        [Fact]
        public async Task ProvidesCatalog()
        {
            // arrange
            var dataSource = new WindCubeDataSource() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                Configuration: new Dictionary<string, string>(),
                Logger: NullLogger.Instance);

            await dataSource.SetContextAsync(context, CancellationToken.None);

            // act
            var actual = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var actualIds = actual.Resources.Skip(2).Take(2).Select(resource => resource.Id).ToList();
            var actualUnits = actual.Resources.Skip(2).Take(2).Select(resource => resource.Properties["Unit"]).ToList();
            var actualGroups = actual.Resources.Skip(2).Take(2)
                .SelectMany(resource => resource.Properties.Where(entry => entry.Key.StartsWith("Groups")).Select(entry => entry.Value)).ToList();
            var actualTimeRange = await dataSource.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

            // assert
            var expectedIds = new List<string>() { "WC_Pressure", "WC_Rel_Humidity" };
            var expectedUnits = new List<string>() { "hPa", "%" };
            var expectedGroups = new List<string>() { "Environment", "Environment" };
            var expectedStartDate = new DateTime(2020, 10, 07, 00, 00, 00, DateTimeKind.Utc);
            var expectedEndDate = new DateTime(2020, 10, 09, 00, 00, 00, DateTimeKind.Utc);

            Assert.True(expectedIds.SequenceEqual(actualIds));
            Assert.True(expectedUnits.SequenceEqual(actualUnits));
            Assert.True(expectedGroups.SequenceEqual(actualGroups));
            Assert.Equal(expectedStartDate, actualTimeRange.Begin);
            Assert.Equal(expectedEndDate, actualTimeRange.End);
        }

        [Fact]
        public async Task ProvidesDataAvailability()
        {
            // arrange
            var dataSource = new WindCubeDataSource() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                Configuration: new Dictionary<string, string>(),
                Logger: NullLogger.Instance);

            await dataSource.SetContextAsync(context, CancellationToken.None);

            // act
            var actual = new Dictionary<DateTime, double>();
            var begin = new DateTime(2020, 10, 06, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2020, 10, 09, 0, 0, 0, DateTimeKind.Utc);

            var currentBegin = begin;

            while (currentBegin < end)
            {
                actual[currentBegin] = await dataSource.GetAvailabilityAsync("/A/B/C", currentBegin, currentBegin.AddDays(1), CancellationToken.None);
                currentBegin += TimeSpan.FromDays(1);
            }

            // assert
            var expected = new SortedDictionary<DateTime, double>(Enumerable.Range(0, 2).ToDictionary(
                    i => begin.AddDays(i),
                    i => 0.0));

            expected[begin.AddDays(1)] = 1.0;
            expected[begin.AddDays(2)] = 54.0 / 144.0;

            Assert.True(expected.SequenceEqual(new SortedDictionary<DateTime, double>(actual)));
        }

        [Fact]
        public async Task CanReadFullDay()
        {
            // arrange
            var dataSource = new WindCubeDataSource() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                Configuration: new Dictionary<string, string>(),
                Logger: NullLogger.Instance);

            await dataSource.SetContextAsync(context, CancellationToken.None);

            // act
            var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var resource = catalog.Resources.First();
            var representation = resource.Representations.First();
            var catalogItem = new CatalogItem(catalog, resource, representation);

            var begin = new DateTime(2020, 10, 08, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2020, 10, 09, 0, 0, 0, DateTimeKind.Utc);
            var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

            var result = new ReadRequest(catalogItem, data, status);
            await dataSource.ReadAsync(begin, end, new ReadRequest[] { result }, new Progress<double>(), CancellationToken.None);

            // assert
            void DoAssert()
            {
                var data = MemoryMarshal.Cast<byte, double>(result.Data.Span);

                Assert.Equal(10.0, data[0]);
                Assert.Equal(10.1, data[1]);
                Assert.Equal(11.9, data[53]);
                Assert.Equal(0, data[54]);

                Assert.Equal(1, result.Status.Span[0]);
                Assert.Equal(1, result.Status.Span[1]);
                Assert.Equal(1, result.Status.Span[53]);
                Assert.Equal(0, result.Status.Span[54]);
            }

            DoAssert();
        }
    }
}