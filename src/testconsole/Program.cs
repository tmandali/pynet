using Parquet;

var url = args.Length > 0 ? args[0] : "http://localhost:5172/testdb/parquet";

//var destinationPath = "downloaded.parquet";

try
{
    using var httpClient = new HttpClient();
    var result = await httpClient.PostAsync(url, new StringContent("SELECT * FROM users"));
    result.EnsureSuccessStatusCode();
    
    var bytes = await result.Content.ReadAsByteArrayAsync();
    using var stream = new MemoryStream(bytes);
    // var data = await ParquetSerializer.DeserializeAsync(stream);
    // var xx = await ParquetSerializer.DeserializeAsync<User>(stream);
    var df = await stream.ReadParquetAsDataFrameAsync();
    Console.WriteLine($"Toplam {df.Rows.Count} satır okundu.");
    Console.WriteLine($"Kolon sayısı: {df.Columns.Count}");

    // var dt = df.ToTable();
}
catch (HttpRequestException ex)
{
    Console.WriteLine($"HTTP hatası: {ex.Message}");
    return 1;
}
catch (Exception ex)
{
    Console.WriteLine($"Beklenmeyen hata: {ex.Message}");
    Console.WriteLine($"Stack trace: {ex.StackTrace}");
    return 1;
}

return 0;