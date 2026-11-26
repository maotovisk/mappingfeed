namespace MappingFeed.Common.Extensions;

public static class StringExtensions
{
    extension(string? value)
    {
        public string? NullIfWhiteSpace() =>
            string.IsNullOrWhiteSpace(value) ? null : value;

        public string OrDefault(string fallback) =>
            value.NullIfWhiteSpace()?.Trim() ?? fallback;

        public bool ContainsIgnoreCase(string marker) =>
            value?.Contains(marker, StringComparison.OrdinalIgnoreCase) ?? false;

        /// <summary>Normalizes "#abc", "abc" or "#aabbcc" to "#AABBCC"; null if not a hex color.</summary>
        public string? NormalizeHexColor()
        {
            var hex = value?.Trim().TrimStart('#');
            if (hex is null || !hex.All(Uri.IsHexDigit))
                return null;

            if (hex.Length == 3)
                hex = string.Concat(hex.Select(x => $"{x}{x}"));

            return hex.Length == 6 ? $"#{hex.ToUpperInvariant()}" : null;
        }
    }

    extension(string value)
    {
        public string Truncate(int maxLength) =>
            value.Length <= maxLength ? value : value[..Math.Max(0, maxLength - 3)] + "...";
    }
}
