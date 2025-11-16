namespace pyapi.Models;

public class DataExporterRequest
{
    public Dictionary<string, string> ConnectionStrings { get; set; } = [];
    public Dictionary<string, Dictionary<string, string>> DataSources { get; set; } = [];
    public string Query { get; set; } = string.Empty;
}