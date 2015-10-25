namespace Simple.Data.Firebird.BulkInsert
{
    internal class FbInsertSqlContext
    {
        public string TableName { get; set; }
        public string ReturnsExecuteBlockSql { get; set; }
        public string ReturnsColumnSql { get; set; }
        public string ReturnsVariablesSql { get; set; }
        public int MaxParameterId { get; set; }
        public int LastParameterId { get; set; }
        public bool SkipCommandParameters { get; set; }
    }
}