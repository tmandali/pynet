using System.Diagnostics;
using System.IO;
using CSnakes.Runtime;
using CSnakes.Runtime.CPython;
using CSnakes.Runtime.Python;
using CSnakes.Runtime.EnvironmentManagement;
using CSnakes.Runtime.Locators;
using System.Runtime.InteropServices;
using Microsoft.VisualBasic;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddProblemDetails();

var home = Path.Join(Environment.CurrentDirectory, "..", "python");
builder.Services.WithPython()
    .WithHome(home)
    .FromRedistributable(RedistributablePythonVersion.Python3_13)
    .WithUvInstaller()
    .WithVirtualEnvironment(Path.Combine(home, ".venv"));

builder.Services.AddSingleton(sp => sp.GetRequiredService<IPythonEnvironment>().Demo());
builder.Services.AddSingleton(sp => sp.GetRequiredService<IPythonEnvironment>().CodeExec());
builder.Services.AddSingleton(sp => sp.GetRequiredService<IPythonEnvironment>().DovizKurlari());

var app = builder.Build();
app.MapGet("/", () => "Hello World!");
app.MapGet("/demo", (IDemo demo) => demo.Greetings("timur"));
// app.MapPost("/codeExec", async (ICodeExec codeExec, HttpContext context, IPythonEnvironment pythonEnvironment) => 
// {
//     // using (GIL.Acquire())
//     // {
//     //     var module = CSnakes.Runtime.Python.Import.ImportModule("demo");
//     //     var func = module.GetAttr("greetings");
//     // }

//     using var reader = new StreamReader(context.Request.Body);
//     var body = await reader.ReadToEndAsync();
//     return ExecutePythonCode(body, Path.Combine(home, ".venv")); //codeExec.Run(body, Path.Combine(home, ".venv"));
// });


app.MapPost("/run-python", async (HttpContext context) =>
{

    var venvPath = Path.Combine(home, ".venv");
    var pythonExec = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? 
            Path.Combine(venvPath, "Scripts", "python.exe") : 
            Path.Combine(venvPath, "bin", "python");

    using var reader = new StreamReader(context.Request.Body);
    var code = await reader.ReadToEndAsync();

    var startInfo = new ProcessStartInfo
    {
        FileName = pythonExec,
        Arguments = "-u", 
        RedirectStandardInput = true,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true,
        WorkingDirectory = home 
    };

    using var process = new Process { StartInfo = startInfo };

    context.RequestAborted.Register(() =>
    {
        try { if (!process.HasExited) process.Kill(); } catch { }
    });

    process.Start();

    // Process'i başlat ve çıktıları topla
    var outputLines = new List<string>();
    var errorLines = new List<string>();

    var outputTask = Task.Run(async () =>
    {
        string? line;
        while ((line = await process.StandardOutput.ReadLineAsync()) != null)
        {
            outputLines.Add(line);
        }
    });

    var errorTask = Task.Run(async () =>
    {
        string? line;
        while ((line = await process.StandardError.ReadLineAsync()) != null)
        {
            errorLines.Add(line);
        }
    });

    await process.StandardInput.WriteAsync(code);
    await process.StandardInput.DisposeAsync();

    await Task.WhenAll(outputTask, errorTask, process.WaitForExitAsync());

    if (process.ExitCode != 0)
    {
        context.Response.StatusCode = 400;
        context.Response.ContentType = "application/json";
        var errorDetails = string.Join("\\n", errorLines);
        await context.Response.WriteAsync($"{{\"error\": \"Python execution failed\", \"details\": \"{errorDetails}\", \"exitCode\": {process.ExitCode}}}");
        return;
    }

    context.Response.ContentType = "text/event-stream";
    context.Response.Headers.CacheControl = "no-cache";
    context.Response.Headers.Connection = "keep-alive";

    foreach (var line in outputLines)
    {
        await context.Response.WriteAsync($"data: {line}\n\n");
    }
    await context.Response.Body.FlushAsync();
    
});

await app.RunAsync();


// static string ExecutePythonCode(string code, string? venvPath = null)
// {
//     // 1. Python Yorumlayıcı Yolunu Belirleme
//     string pythonExec;
    
//     if (!string.IsNullOrEmpty(venvPath))
//     {
//         // Sanal ortam yolu varsa, işletim sistemine göre yolu ayarla.
//         if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
//         {
//             // Windows: venv_path/Scripts/python.exe
//             pythonExec = Path.Combine(venvPath, "Scripts", "python.exe");
//         }
//         else
//         {
//             // Linux/macOS/Diğer: venv_path/bin/python
//             pythonExec = Path.Combine(venvPath, "bin", "python");
//         }
//     }
//     else
//     {
//         // Sanal ortam yoksa, PATH'te aranan standart 'python' veya 'python3' kullanılır.
//         // Bu, ortam değişkenlerine bağlıdır.
//         pythonExec = "python"; 
//     }

//     // 2. Süreç Bilgilerini Yapılandırma
//     var startInfo = new ProcessStartInfo
//     {
//         FileName = pythonExec,
//         // Argümanlar: Python'a komutları stdin'den okumasını söyleyen '-' argümanı kullanılır.
//         //Arguments = "-", 
//         RedirectStandardInput = true,
//         RedirectStandardOutput = true,
//         RedirectStandardError = true,
//         UseShellExecute = false, // Kabuk kullanmamak, girdiyi/çıktıyı yeniden yönlendirmek için önemlidir.
//         CreateNoWindow = true    // Konsol penceresi açılmaz.
//     };

//     using Process process = new() { StartInfo = startInfo };
    
//     process.OutputDataReceived += (sender, e) =>
//     {
//         if (!string.IsNullOrEmpty(e.Data))
//         {
//         }
//     };

//     // 3. Süreci Başlatma
//     process.Start();

//     // 4. Python Kodunu Alt Sürece Gönderme (stdin)
//     // 'code' string'ini alt sürecin standart girdisine yaz.
//     process.StandardInput.Write(code);
//     process.StandardInput.Close(); // Girdiyi kapatmak, alt sürecin okumayı bitirmesi için önemlidir.

//     // 5. Çıktı ve Hataları Okuma
//     // Çıktıları ve hataları alt süreç bitmeden önce bile asenkron olarak okumak daha iyidir,
//     // ancak basitlik için burada senkron okuma kullanıyoruz.
//     string stdout = process.StandardOutput.ReadToEnd();
//     string stderr = process.StandardError.ReadToEnd();

//     // Sürecin bitmesini bekle
//     process.WaitForExit();

//     // 6. Hata Kontrolü
//     if (process.ExitCode != 0)
//     {
//         // Python'daki Exception fırlatma ile eşdeğer.
//         throw new Exception($"Python alt süreci hata ile bitti (Kod: {process.ExitCode}). Hata Çıktısı:\n{stderr}");
//     }

//     return stdout;
// }
// static void ExecuteProcess(string fileName, IEnumerable<string> arguments, string workingDirectory, string path, TextWriter? stdOut, TextWriter? stdErr, IReadOnlyDictionary<string, string?>? extraEnv = null)
// {
//     ProcessStartInfo startInfo = new(fileName, arguments)
//     {
//         WorkingDirectory = workingDirectory,
//         CreateNoWindow = true,
//         UseShellExecute = false,
//         RedirectStandardOutput = true,
//         RedirectStandardError = true,
//     };

//     if (!string.IsNullOrEmpty(path))
//         startInfo.EnvironmentVariables["PATH"] = path;

//     if (extraEnv is not null)
//     {
//         foreach (var kvp in extraEnv)
//         {
//             if (kvp.Value is not null)
//                 startInfo.EnvironmentVariables[kvp.Key] = kvp.Value;
//         }
//     }
//     startInfo.RedirectStandardOutput = true;
//     startInfo.RedirectStandardError = true;

//     using Process process = new() { StartInfo = startInfo };
//     process.OutputDataReceived += (sender, e) =>
//     {
//         if (!string.IsNullOrEmpty(e.Data))
//         {
//             stdOut?.WriteLine(e.Data);
//         }
//     };

//     process.ErrorDataReceived += (sender, e) =>
//     {
//         if (!string.IsNullOrEmpty(e.Data))
//         {
//             stdErr?.WriteLine(e.Data);
//         }
//     };

//     process.Start();
//     process.BeginErrorReadLine();
//     process.BeginOutputReadLine();
//     process.WaitForExit();

//     if (process.ExitCode != 0)
//     {
//         throw new InvalidOperationException("Failed to install packages");
//     }
// }