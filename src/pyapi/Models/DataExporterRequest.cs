namespace pyapi.Models;

public class DataExporterRequest
{
    public Dictionary<string, DatasetInfo> Datasets { get; set; } = new();
    public string Query { get; set; } = string.Empty;
}

public class DatasetInfo
{
    public string Query { get; set; } = string.Empty;
    public string DbUri { get; set; } = string.Empty;
    public string DbName { get; set; } = string.Empty;
}

