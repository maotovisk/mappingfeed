using System.Globalization;
using System.Text.Json.Nodes;

namespace MappingFeed.Osu;

internal static class JsonNodeExtensions
{
    extension(JsonObject jsonObject)
    {
        public JsonArray? TryGetArray(params string[] propertyNames)
        {
            foreach (var propertyName in propertyNames)
            {
                if (jsonObject[propertyName] is JsonArray array)
                    return array;
            }

            return null;
        }

        public string? TryGetString(params string[] propertyNames)
        {
            foreach (var propertyName in propertyNames)
            {
                if (!TryGetValue<string?>(jsonObject[propertyName], out var value))
                    continue;

                if (!string.IsNullOrWhiteSpace(value))
                    return value;
            }

            return null;
        }

        public long? TryGetInt64(params string[] propertyNames)
        {
            foreach (var propertyName in propertyNames)
            {
                if (jsonObject[propertyName] is null)
                    continue;

                if (TryGetValue(jsonObject[propertyName], out long longValue))
                    return longValue;

                if (TryGetValue(jsonObject[propertyName], out int intValue))
                    return intValue;

                if (long.TryParse(jsonObject[propertyName]!.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedValue))
                    return parsedValue;
            }

            return null;
        }

        public string? TryGetNestedString(params string[] path)
        {
            JsonNode? current = jsonObject;

            foreach (var segment in path)
            {
                current = current[segment];
                if (current is null)
                    return null;
            }

            return TryGetValue<string?>(current, out var value) ? value : null;
        }

        public long? TryGetNestedInt64(params string[] path)
        {
            JsonNode? current = jsonObject;

            foreach (var segment in path)
            {
                current = current[segment];
                if (current is null)
                    return null;
            }

            if (TryGetValue(current, out long longValue))
                return longValue;

            if (TryGetValue(current, out int intValue))
                return intValue;

            return long.TryParse(current.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedValue)
                ? parsedValue
                : null;
        }
    }

    public static IEnumerable<string> CollectStrings(this JsonNode? jsonNode)
    {
        if (jsonNode is null)
            yield break;

        switch (jsonNode)
        {
            case JsonValue value:
                if (value.TryGetValue<string>(out var stringValue) && !string.IsNullOrWhiteSpace(stringValue))
                    yield return stringValue;
                break;

            case JsonObject objectValue:
                foreach (var property in objectValue)
                {
                    foreach (var nestedValue in property.Value.CollectStrings())
                        yield return nestedValue;
                }
                break;

            case JsonArray array:
                foreach (var nestedNode in array)
                {
                    foreach (var nestedValue in nestedNode.CollectStrings())
                        yield return nestedValue;
                }
                break;
        }
    }

    private static bool TryGetValue<T>(JsonNode? node, out T value)
    {
        try
        {
            value = node!.GetValue<T>();
            return true;
        }
        catch
        {
            value = default!;
            return false;
        }
    }
}
