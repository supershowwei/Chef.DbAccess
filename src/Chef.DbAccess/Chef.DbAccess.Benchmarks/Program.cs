using System.Linq;
using BenchmarkDotNet.Running;

namespace Chef.DbAccess.Benchmarks
{
    // 用法：dotnet run -c Release -f net10.0 -- --filter * --runtimes net10.0 net48
    public static class Program
    {
        public static int Main(string[] args)
        {
            var summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

            // 情境執行失敗時（例如 GlobalSetup 的結果檢查丟出例外），BenchmarkDotNet 仍然正常結束，改以結束代碼回報
            return summaries.Any(s => s.HasCriticalValidationErrors || s.Reports.Any(r => !r.Success)) ? 1 : 0;
        }
    }
}
