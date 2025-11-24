using Linq2db.Ydb;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Interceptors;
using LinqToDB.Mapping;
using NUnit.Framework;
using Ydb.Sdk.Ado;

namespace test
{
    /// <summary>
    /// Base class for YDB-specific type tests.
    /// Takes into account YDB limitations:
    /// - no temporary tables;
    /// - YdbParameter cannot handle null without an explicit type;
    /// - ProviderSpecific BulkCopy (BulkUpsert) is not used here.
    /// </summary>
    public abstract class TypeTestsBase
    {
        // ===================== TABLE MODEL =====================

        private sealed class TypeTable<TType, TNullableType>
        {
            [PrimaryKey]
            public int Id { get; set; }

            public TType          Column         { get; set; } = default!;
            public TNullableType? ColumnNullable { get; set; }
        }

        // ===================== YDB CONNECTION =====================

        private static int TryInt(string? s, int fallback) =>
            int.TryParse(s, out var v) ? v : fallback;

        private static bool TryBool(string? s, bool fallback) =>
            bool.TryParse(s, out var v) ? v : fallback;

        private static readonly string Host    = Environment.GetEnvironmentVariable("YDB_HOST")      ?? "localhost";
        private static readonly int    Port    = TryInt(Environment.GetEnvironmentVariable("YDB_PORT"), 2136);
        private static readonly string DbPath  = Environment.GetEnvironmentVariable("YDB_DB")        ?? "/local";
        private static readonly bool   UseTls  = TryBool(Environment.GetEnvironmentVariable("YDB_USE_TLS"), false);
        private static readonly int    TlsPort = TryInt(Environment.GetEnvironmentVariable("YDB_TLS_PORT"), 2135);

        private static async Task<DataConnection> CreateYdbConnection()
        {
            var builder = new YdbConnectionStringBuilder
            {
                Host     = Host,
                Port     = UseTls ? TlsPort : Port,
                Database = DbPath,
                UseTls   = UseTls
            };

            await using var ydbConnection = new YdbConnection(builder);
            return YdbTools.CreateDataConnection(ydbConnection);
        }

        /// <summary>
        /// By default we test parameterized queries.
        /// </summary>
        protected virtual bool TestParameters => true;

        // ===================== CORE TYPE TEST METHOD =====================

        protected async ValueTask TestType<TType, TNullableType>(
            DbDataType dbType,
            TType value,
            TNullableType? nullableValue,
            bool? testParameters = null,
            bool skipNullable = false,
            bool filterByValue = true,
            bool filterByNullableValue = true,
            Func<TType, TType>? getExpectedValue = null,
            Func<TNullableType?, TNullableType?>? getExpectedNullableValue = null,
            Func<TType, bool>? isExpectedValue = null,
            Func<TNullableType?, bool>? isExpectedNullableValue = null,
            Func<BulkCopyType, bool>? testBulkCopyType = null)
        {
            testParameters ??= TestParameters;

            // ---------- MappingSchema and FluentMapping ----------

            var ms  = new MappingSchema();
            var ent = new FluentMappingBuilder(ms).Entity<TypeTable<TType, TNullableType>>();

            ent.Property(e => e.Id).IsPrimaryKey();

            var prop = ent.Property(e => e.Column).IsNullable(false);

            if (dbType.DataType  != DataType.Undefined) prop.HasDataType(dbType.DataType);
            if (dbType.DbType    != null)              prop.HasDbType(dbType.DbType);
            if (dbType.Precision != null)              prop.HasPrecision(dbType.Precision.Value);
            if (dbType.Scale     != null)              prop.HasScale(dbType.Scale.Value);
            if (dbType.Length    != null)              prop.HasLength(dbType.Length.Value);

            if (!skipNullable)
            {
                var propN = ent.Property(e => e.ColumnNullable).IsNullable(true);

                if (dbType.DataType  != DataType.Undefined) propN.HasDataType(dbType.DataType);
                if (dbType.DbType    != null)              propN.HasDbType($"Nullable({dbType.DbType})");
                if (dbType.Precision != null)              propN.HasPrecision(dbType.Precision.Value);
                if (dbType.Scale     != null)              propN.HasScale(dbType.Scale.Value);
                if (dbType.Length    != null)              propN.HasLength(dbType.Length.Value);
            }
            else
            {
                ent.Property(e => e.ColumnNullable).IsNotColumn();
            }

            ent.Build();

            var row = new TypeTable<TType, TNullableType>
            {
                Id             = 1,
                Column         = value,
                ColumnNullable = nullableValue
            };

            DataConnection? db = null;

            try
            {
                // ---------- Connection and table creation ----------
                db = await CreateYdbConnection();
                db.AddMappingSchema(ms);

                // Table name will be TypeTable`2, which is what the provider expects.
                db.CreateTable<TypeTable<TType, TNullableType>>();

                var table = db.GetTable<TypeTable<TType, TNullableType>>();

                // ---------- Initial single-row insert WITHOUT parameters ----------
                db.InlineParameters = true;
                table.Insert(() => new TypeTable<TType, TNullableType>
                {
                    Id             = row.Id,
                    Column         = row.Column,
                    ColumnNullable = row.ColumnNullable
                });
                db.InlineParameters = false;

                // ---------- Parameterized query tests ----------
                if (testParameters == true)
                {
                    // Filter by nullable column only if it is not null
                    // (otherwise YdbParameter would be created with null).
                    var filterByNullableForParams =
                        filterByNullableValue && !skipNullable && nullableValue is not null;

                    db.InlineParameters = false;

                    var expectedParamCount =
                        (filterByValue ? 1 : 0) +
                        (filterByNullableForParams ? 1 : 0);

                    db.OnNextCommandInitialized((_, cmd) =>
                    {
                        Assert.That(cmd.Parameters, Has.Count.EqualTo(expectedParamCount));
                        return cmd;
                    });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableForParams,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);
                }

                // ---------- Literal tests (InlineParameters = true) ----------
                {
                    db.InlineParameters = true;

                    db.OnNextCommandInitialized((_, cmd) =>
                    {
                        Assert.That(cmd.Parameters, Is.Empty);
                        return cmd;
                    });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableValue,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);

                    db.InlineParameters = false;
                }

                // If parameters are disabled — always inline them.
                if (testParameters == false)
                    db.InlineParameters = true;

                // For cases with nullableValue == null we skip BulkCopy
                // (to avoid creating a parameter with a null value).
                var canBulkCopy = nullableValue is not null;

                // ---------- BulkCopy: RowByRow ----------
                if (canBulkCopy && (testBulkCopyType == null || testBulkCopyType(BulkCopyType.RowByRow)))
                {
                    var options = new BulkCopyOptions
                    {
                        BulkCopyType  = BulkCopyType.RowByRow,
                        UseParameters = testParameters != false
                    };

                    table.Delete();
                    db.BulkCopy(options, new[] { row });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableValue,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);
                }

                // ---------- BulkCopy: MultipleRows ----------
                if (canBulkCopy && (testBulkCopyType == null || testBulkCopyType(BulkCopyType.MultipleRows)))
                {
                    var options = new BulkCopyOptions
                    {
                        BulkCopyType  = BulkCopyType.MultipleRows,
                        UseParameters = testParameters != false
                    };

                    table.Delete();
                    db.BulkCopy(options, new[] { row });

                    AssertData(
                        table,
                        value,
                        nullableValue,
                        skipNullable,
                        filterByValue,
                        filterByNullableValue,
                        getExpectedValue,
                        getExpectedNullableValue,
                        isExpectedValue,
                        isExpectedNullableValue);
                }

                // NOTE:
                // ProviderSpecific (BulkUpsert) is intentionally NOT used here
                // to avoid typical type conflicts on cross-type mappings.
            }
            finally
            {
                if (db != null)
                {
                    try
                    {
                        db.DropTable<TypeTable<TType, TNullableType>>();
                    }
                    catch
                    {
                        // Table might already be removed — ignore.
                    }

                    db.Dispose();
                }
            }
        }

        // ===================== DATA ASSERTION =====================

        private static void AssertData<TType, TNullableType>(
            ITable<TypeTable<TType, TNullableType>> table,
            TType value,
            TNullableType? nullableValue,
            bool skipNullable,
            bool filterByValue,
            bool filterByNullableValue,
            Func<TType, TType>? getExpectedValue,
            Func<TNullableType?, TNullableType?>? getExpectedNullableValue,
            Func<TType, bool>? isExpectedValue,
            Func<TNullableType?, bool>? isExpectedNullableValue)
        {
            filterByNullableValue = filterByNullableValue && !skipNullable;

            var records =
                filterByValue && filterByNullableValue
                    ? table.Where(r => (object)r.Column! == (object)value! && (object?)r.ColumnNullable == (object?)nullableValue).ToArray()
                    : filterByValue
                        ? table.Where(r => (object)r.Column! == (object)value!).ToArray()
                        : filterByNullableValue
                            ? table.Where(r => (object?)r.ColumnNullable == (object?)nullableValue).ToArray()
                            : table.ToArray();

            Assert.That(records, Has.Length.EqualTo(1));

            var record = records[0];

            if (isExpectedValue != null)
            {
                Assert.That(isExpectedValue(record.Column), Is.True);
            }
            else
            {
                Assert.That(
                    record.Column,
                    Is.EqualTo(
                        getExpectedValue != null
                            ? getExpectedValue(value)
                            : value));
            }

            if (!skipNullable)
            {
                if (isExpectedNullableValue != null)
                {
                    Assert.That(isExpectedNullableValue(record.ColumnNullable), Is.True);
                }
                else
                {
                    Assert.That(
                        record.ColumnNullable,
                        Is.EqualTo(
                            getExpectedNullableValue != null
                                ? getExpectedNullableValue(nullableValue)
                                : nullableValue));
                }
            }
        }
    }
}
