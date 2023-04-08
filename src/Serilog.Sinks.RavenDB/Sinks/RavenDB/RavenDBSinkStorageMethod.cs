namespace Serilog.Sinks.RavenDB
{
    /// <summary>
    /// Indicated witch method top use when th log event is stored
    /// </summary>
    public enum RavenDBSinkStorageMethod
    {
        /// <summary> a DB Session is used to store each batch</summary>
        Session,
        /// <summary> BulkInsert is used to store each batch</summary>
        BulkInsert
    }
}