using CSnakes.Runtime;
using CSnakes.Runtime.Locators;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddProblemDetails();

var home = Path.Join(Environment.CurrentDirectory, "..", "python");
builder.Services.WithPython()
    .WithHome(home)
    .FromRedistributable(RedistributablePythonVersion.Python3_13)
    .WithUvInstaller()
    .WithVirtualEnvironment(Path.Combine(home, ".venv"));

builder.Services.AddSingleton(sp => sp.GetRequiredService<IPythonEnvironment>().Demo());

var app = builder.Build();
app.MapGet("/", () => "Hello World!");
app.MapGet("/demo", (IDemo demo) => demo.Greetings("timur"));
app.Run();