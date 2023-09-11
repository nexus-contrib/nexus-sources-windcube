namespace Nexus.Sources;

internal static class DateTimeExtensions
{
    public static DateTime RoundDown(this DateTime dateTime, TimeSpan timeSpan)
    {
        return new DateTime(dateTime.Ticks - (dateTime.Ticks % timeSpan.Ticks), dateTime.Kind);
    }
}