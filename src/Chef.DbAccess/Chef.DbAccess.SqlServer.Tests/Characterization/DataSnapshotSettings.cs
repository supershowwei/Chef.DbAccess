using Argon;
using VerifyTests;

namespace Chef.DbAccess.SqlServer.Tests.Characterization
{
    internal static class DataSnapshotSettings
    {
        /// <summary>
        /// 資料快照要原樣記錄每個欄位：Verify 預設會略過 null、預設值與空集合，
        /// 也會把 Guid 與日期換成佔位符，這些都會讓 5.0 改寫後的比對漏掉差異。
        /// </summary>
        public static SettingsTask AsDataSnapshot(this SettingsTask settings)
        {
            return settings.DontScrubGuids()
                .DontScrubDateTimes()
                .DontIgnoreEmptyCollections()
                .AddExtraSettings(
                    x =>
                        {
                            x.DefaultValueHandling = DefaultValueHandling.Include;
                            x.NullValueHandling = NullValueHandling.Include;
                        });
        }
    }
}
