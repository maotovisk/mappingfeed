using System.Text.Json.Nodes;
using MappingFeed.Config;
using Microsoft.Extensions.Options;

namespace MappingFeed.Osu;

public sealed class OsuAuthClient(
    HttpClient httpClient,
    IOptions<OsuOptions> options)
{
    private readonly OsuOptions _options = options.Value;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private string? _accessToken;
    private DateTimeOffset _expiresAt;

    public async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_accessToken) && _expiresAt > DateTimeOffset.UtcNow.AddMinutes(1))
            return _accessToken;

        await _lock.WaitAsync(cancellationToken);
        try
        {
            if (!string.IsNullOrWhiteSpace(_accessToken) && _expiresAt > DateTimeOffset.UtcNow.AddMinutes(1))
                return _accessToken;

            using var request = new HttpRequestMessage(HttpMethod.Post, "/oauth/token")
            {
                Content = new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["client_id"] = _options.ClientId.ToString(),
                    ["client_secret"] = _options.ClientSecret,
                    ["grant_type"] = "client_credentials",
                    ["scope"] = "public",
                }),
            };

            var response = await httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();

            await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
            var jsonObject = await JsonNode.ParseAsync(responseStream, cancellationToken: cancellationToken) as JsonObject
                ?? throw new InvalidOperationException("Failed to parse osu! OAuth response.");

            _accessToken = jsonObject.TryGetString("access_token")
                ?? throw new InvalidOperationException("osu! OAuth response did not contain an access token.");

            var expiresIn = jsonObject.TryGetInt64("expires_in") ?? 3600;
            _expiresAt = DateTimeOffset.UtcNow.AddSeconds(expiresIn);

            return _accessToken;
        }
        finally
        {
            _lock.Release();
        }
    }
}
