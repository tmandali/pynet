using CSnakes.Runtime;
using CSnakes.Runtime.Locators;
using System.Text.Json;
using CSnakes.Runtime.Python;
using pyapi.Models;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddProblemDetails();
builder.Services.AddLogging();
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase;
});

var home = Path.Join(Environment.CurrentDirectory, "..", "python");
builder.Services.WithPython()
    .WithHome(home)
    .FromRedistributable(RedistributablePythonVersion.Python3_13)
    .WithUvInstaller()
    .WithVirtualEnvironment(Path.Combine(home, ".venv"));

builder.Services.AddSingleton(sp => sp.GetRequiredService<IPythonEnvironment>().Demo());
builder.Services.AddSingleton(sp => sp.GetRequiredService<IPythonEnvironment>().CodeExec());
builder.Services.AddSingleton(sp => sp.GetRequiredService<IPythonEnvironment>().DataExporter());

var app = builder.Build();


app.MapGet("/", () => "Hello World!");
app.MapGet("/demo", (IDemo demo) => demo.Greetings("timur"));
app.MapGet("/dict", (IDemo demos) =>
{
    var dict = demos.GetInfo();
    return JsonSerializer.Serialize(dict);
});

app.MapPost("/dataexporter/parquet", (
    IDataExporter dx, 
    IConfiguration config,
    DataExporterRequest request, 
    string? filename = null) =>
{
        var datasets = request.DataSources.SelectMany(s=> 
        s.Value.Select(v=> new {
            DbUri = request.ConnectionStrings.TryGetValue(s.Key, out var connStr) && !string.IsNullOrEmpty(connStr) 
                ? connStr 
                : config.GetConnectionString(s.Key) ?? throw new Exception($"Connection string not found: {s.Key}"),
            Name = v.Key,
            Query = v.Value
        })
    )
    .ToDictionary(x => x.Name, x => dx.ToPolar(x.DbUri, x.Query));
    
    return Results.Bytes(
        dx.ToParquetDataset(request.Query, datasets).AsReadOnlySpan<byte>().ToArray(), "application/octet-stream", filename ?? $"export_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}.parquet");
});

app.MapPost("/dataexporter/{cnnName}/parquet", async (
    IDataExporter dx, 
    IConfiguration config,
    HttpRequest request,
    string cnnName,
    string? filename = null) =>
{

    
    using var reader = new StreamReader(request.Body);
    var meta = new {
        Query = await reader.ReadToEndAsync(),
        DbUri = request.Headers.TryGetValue(cnnName, out var headerValue) ? headerValue.ToString() : config.GetConnectionString(cnnName) ?? throw new Exception("Connection string not found"),
        FileName = filename ?? $"export_{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}.parquet"
    };

    return Results.Bytes(
        dx.ToParquet(meta.DbUri, meta.Query).AsReadOnlySpan<byte>().ToArray(),
        "application/octet-stream",
        meta.FileName);

    // var kullanici = "sa";
    // var parola = HttpUtility.UrlEncode("Passw@rd");
    // var host = "localhost";
    // var port = 1433;
    // var veritabani = "TestDb";

    // var DB_URI =
    //     $"mssql+pyodbc://{kullanici}:{parola}@{host}:{port}/{veritabani}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes";

    //var users = ep.WriteParquet(dbUri, query);
});

app.MapGet("/obj", (IDemo demos) =>
{
    var obj = demos.GetObj().As<IReadOnlyDictionary<string, string>>();
    return Results.Ok(obj);
});

app.MapPost("/run", async (IPythonEnvironment env, HttpRequest request) =>
{
    using var reader = new StreamReader(request.Body);
    using var module = Import.ImportModule("test", await reader.ReadToEndAsync(), home);
    var x = module.ToString();
    return Results.Ok(x); 
});

await app.RunAsync();

// static IResult HandleException(Exception ex, int statusCode = 500, string? title = null)
// {
//     var errorDetail = ex.Message;
//     if (ex.InnerException != null)
//     {
//         errorDetail += $" Inner Exception: {ex.InnerException.Message}";
//         if (ex.InnerException.StackTrace != null)
//         {
//             errorDetail += $" StackTrace: {ex.InnerException.StackTrace}";
//         }
//     }
//     if (ex.StackTrace != null)
//     {
//         errorDetail += $" StackTrace: {ex.StackTrace}";
//     }
    
//     return Results.Problem(
//         detail: errorDetail,
//         statusCode: statusCode,
//         title: title ?? "An error occurred while processing the request");
// }
