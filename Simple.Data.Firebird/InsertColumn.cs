using Simple.Data.Ado.Schema;

namespace Simple.Data.Firebird
{
    internal class InsertColumn
    {
        public string Name { get; set; }
        public object Value { get; set; }
        public string ParameterName { get; set; }
        public Column Column { get; set; }
    }
}