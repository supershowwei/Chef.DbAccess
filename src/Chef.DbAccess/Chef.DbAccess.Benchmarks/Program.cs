using BenchmarkDotNet.Running;

namespace Chef.DbAccess.Benchmarks
{
    // 用法：dotnet run -c Release -f net10.0 -- --filter * --runtimes net10.0 net48
    public static class Program
    {
        public static void Main(string[] args)
        {
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
