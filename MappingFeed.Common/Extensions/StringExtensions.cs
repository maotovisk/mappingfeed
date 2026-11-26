namespace MappingFeed.Common.Extensions;

public static class StringExtensions
{
    extension(string? value)
    {
        public string OrDefault(string fallback) =>
            string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
    }
}
