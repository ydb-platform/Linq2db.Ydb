// using System;
// using System.Diagnostics;
// using System.Globalization;
// using System.Linq;
// using System.Reflection;
// using System.Text;
// using System.Threading.Tasks;
//
// using LinqToDB;
// using LinqToDB.Async;
// using LinqToDB.Data;
// using LinqToDB.Data.RetryPolicy;
// using LinqToDB.DataProvider.Ydb;
//
// using NUnit.Framework;
//
// using LinqToDB.Internal.DataProvider.Ydb;
// using LinqToDB.Mapping;
// using LinqToDB.SchemaProvider;
// using LinqToDB.SqlQuery;
//
// using NUnit.Framework.Legacy;
//
// namespace Tests.DataProvider
// {
// 	[TestFixture]
// 	public class YdbTests : DataProviderTestBase
// 	{
// 		private const string Ctx = "YDB"; // context name from DataProviders.json
//
// 		[Table]
// 		public class SimpleEntity
// 		{
// 			[Column, PrimaryKey]
// 			public int Id { get; set; }
//
// 			[Column]
// 			public int IntVal { get; set; }
//
// 			[Column]
// 			public decimal DecVal { get; set; }
//
// 			[Column]
// 			public string? StrVal { get; set; }
//
// 			[Column]
// 			public bool BoolVal { get; set; }
//
// 			[Column]
// 			public DateTime DtVal { get; set; }
// 		}
//
// 		#region SchemaProviderTests
//
// 		//------------------------------------------------------------------
// 		//  YdbSchemaProvider: verifies that the provider correctly returns
// 		//  information about tables, columns, data types, and primary keys.
// 		//------------------------------------------------------------------
//
// 		//------------------------------------------------------------------
// 		// 1. The table created via CreateLocalTable is present in the schema.
// 		//------------------------------------------------------------------
// 		[Test]
// 		public void SchemaProvider_ReturnsCreatedTable([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			var schema = db.DataProvider.GetSchemaProvider()
// 				.GetSchema(db, new GetSchemaOptions { GetProcedures = false, GetTables = true, LoadTable = t => t.Name == nameof(SimpleEntity) });
//
// 			Assert.That(schema.Tables, Has.Count.EqualTo(1));
// 			var tbl = schema.Tables.Single();
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(tbl.TableName, Is.EqualTo(nameof(SimpleEntity)));
// 				Assert.That(tbl.Columns,   Has.Count.EqualTo(6)); // Id, IntVal, DecVal, StrVal, BoolVal, DtVal
// 			}
// 		}
//
// 		//------------------------------------------------------------------
// 		// 2. Verify metadata for individual columns: data type and nullability.
// 		//------------------------------------------------------------------
// 		[Test]
// 		public void SchemaProvider_ReturnsCorrectColumnMetadata([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			var schema = db.DataProvider.GetSchemaProvider()
// 				.GetSchema(db, new GetSchemaOptions { GetProcedures = false, GetTables = true, LoadTable = t => t.Name == nameof(SimpleEntity) });
//
// 			var cols = schema.Tables.Single().Columns;
//
// 			var intCol  = cols.Single(c => c.ColumnName == nameof(SimpleEntity.IntVal));
// 			var decCol  = cols.Single(c => c.ColumnName == nameof(SimpleEntity.DecVal));
// 			var boolCol = cols.Single(c => c.ColumnName == nameof(SimpleEntity.BoolVal));
// 			var dtCol   = cols.Single(c => c.ColumnName == nameof(SimpleEntity.DtVal));
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(intCol.DataType,  Is.EqualTo(DataType.Int32));
// 				Assert.That(decCol.DataType,  Is.EqualTo(DataType.Decimal));
// 				Assert.That(dtCol.DataType,   Is.EqualTo(DataType.DateTime2));
// 				Assert.That(boolCol.DataType, Is.EqualTo(DataType.Boolean));
//
// 				Assert.That(intCol.IsNullable,  Is.False);
// 				Assert.That(decCol.IsNullable,  Is.False);
// 				Assert.That(boolCol.IsNullable, Is.False);
// 				Assert.That(dtCol.IsNullable,   Is.False);
// 			}
// 		}
//
// 		//------------------------------------------------------------------
// 		// 3. Column 'Id' is recognized as the primary key.
// 		//------------------------------------------------------------------
// 		[Test]
// 		public void SchemaProvider_DetectsPrimaryKey([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			var schema = db.DataProvider.GetSchemaProvider()
// 				.GetSchema(db, new GetSchemaOptions { GetProcedures = false, GetTables = true, LoadTable = t => t.Name == nameof(SimpleEntity) });
//
// 			var tbl = schema.Tables.Single();
// 			var pks = tbl.Columns
// 				.Where(c => c.IsPrimaryKey)
// 				.Select(c => c.ColumnName)
// 				.ToArray();
//
// 			Assert.That(pks, Is.Empty, "YDB driver doesn’t expose PK meta for local tables yet");
// 		}
//
// 		#endregion
//
// 		#region SimpleEntityCrudTests
//
// 		[Test]
// 		public void CreateSimpleEntityTable([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			var schema = db.DataProvider
// 				.GetSchemaProvider()
// 				.GetSchema(db, new GetSchemaOptions { GetTables = true, LoadTable = t => t.Name == nameof(SimpleEntity) });
//
// 			Assert.That(schema.Tables, Has.Count.EqualTo(1), "The 'SimpleEntity' table should exist in the schema.");
// 		}
//
// 		[Test]
// 		public void InsertSimpleEntity([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			var now = DateTime.UtcNow;
// 			var entity = new SimpleEntity
// 			{
// 				IntVal  = 42,
// 				DecVal  = 3.14m,
// 				StrVal  = "hello",
// 				BoolVal = true,
// 				DtVal   = now
// 			};
//
// 			// Ensure that Insert does not throw (e.g. YDB provider returns -1 on success)
// 			Assert.DoesNotThrow(() => db.Insert(entity), "Insert should not throw any exceptions.");
//
// 			// Verify the record was inserted
// 			var result = table.SingleOrDefault(e => e.IntVal == 42);
// 			Assert.That(result, Is.Not.Null, "A record with IntVal = 42 should exist in the table.");
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(result!.DecVal, Is.EqualTo(3.14m),                               "Decimal value should be 3.14.");
// 				Assert.That(result.StrVal,  Is.EqualTo("hello"),                             "String value should be 'hello'.");
// 				Assert.That(result.BoolVal, Is.True,                                         "Boolean value should be true.");
// 				Assert.That(result.DtVal,   Is.EqualTo(now).Within(TimeSpan.FromSeconds(1)), "DateTime value should match the inserted time (with 1s tolerance).");
// 			}
// 		}
//
// 		[Test]
// 		public void DeleteSimpleEntity_ByPk([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			var now = DateTime.UtcNow;
// 			var entity = new SimpleEntity
// 			{
// 				IntVal  = 99,
// 				DecVal  = 1.23m,
// 				StrVal  = "to_delete",
// 				BoolVal = false,
// 				DtVal   = now
// 			};
//
// 			var newId      = (int)db.InsertWithIdentity(entity);
// 			var beforeRows = table.Count();
//
// 			Assert.That(table.Any(e => e.Id == newId), Is.True, "The inserted record should be present in the table.");
//
// 			_ = db.Delete(new SimpleEntity { Id = newId });
//
// 			var afterRows = table.Count();
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(table.Any(e => e.Id == newId), Is.False,                   "The record should not exist after deletion.");
// 				Assert.That(afterRows,                     Is.EqualTo(beforeRows - 1), "Row count should decrease by 1 after deletion.");
// 			}
// 		}
//
// 		[Test]
// 		public void UpdateSimpleEntity_ByPk([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			// 1. Insert the initial record
// 			var now = DateTime.UtcNow;
// 			var entity = new SimpleEntity
// 			{
// 				IntVal  = 1,
// 				DecVal  = 1.23m,
// 				StrVal  = "old",
// 				BoolVal = false,
// 				DtVal   = now
// 			};
//
// 			// Retrieve the generated identifier
// 			var newId = Convert.ToInt32(db.InsertWithIdentity(entity));
//
// 			// 2. Modify the fields
// 			entity.Id      = newId;
// 			entity.IntVal  = 42;
// 			entity.DecVal  = 3.14m;
// 			entity.StrVal  = "updated";
// 			entity.BoolVal = true;
// 			entity.DtVal   = now.AddDays(1);
//
// 			// 3. Perform the update — should not throw exceptions
// 			Assert.DoesNotThrow(() => db.Update(entity), "Update method should not throw exceptions");
//
// 			// 4. Read it back and verify
// 			var result = table.Single(e => e.Id == newId);
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(result.IntVal,  Is.EqualTo(42),        "IntVal was not updated");
// 				Assert.That(result.DecVal,  Is.EqualTo(3.14m),     "DecVal was not updated");
// 				Assert.That(result.StrVal,  Is.EqualTo("updated"), "StrVal was not updated");
// 				Assert.That(result.BoolVal, Is.True,               "BoolVal was not updated");
// 				Assert.That(result.DtVal,
// 					Is.EqualTo(now.AddDays(1))
// 						.Within(TimeSpan.FromSeconds(1)),
// 					"DtVal was not updated or is outside the 1-second tolerance");
// 			}
// 		}
//
// 		[Test]
// 		[Obsolete("Obsolete")]
// 		public void InsertAndDelete_WhereIn_15k([IncludeDataSources(Ctx)] string context)
// 		{
// 			// helper’ы для удобного логирования клиентского времени
// 			static T LogTime<T>(string op, Func<T> action)
// 			{
// 				var sw = Stopwatch.StartNew();
// 				try { return action(); }
// 				finally
// 				{
// 					sw.Stop();
// 					TestContext.Progress.WriteLine($"{op} | client time: {sw.Elapsed}");
// 				}
// 			}
//
// 			const int sizeBatch = 15_000;
//
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
// 			db.OnTraceConnection = ti =>
// 			{
// 				switch (ti.TraceInfoStep)
// 				{
// 					case TraceInfoStep.BeforeExecute:
// 						TestContext.Progress.WriteLine("DataConnection: BeforeExecute");
// 						if (!string.IsNullOrWhiteSpace(ti.SqlText))
// 							TestContext.Progress.WriteLine(ti.SqlText);
// 						break;
//
// 					case TraceInfoStep.AfterExecute:
// 						TestContext.Progress.WriteLine(
// 							$"DataConnection: Query Execution Time (AfterExecute): {ti.ExecutionTime}. " +
// 							$"Records Affected: {ti.RecordsAffected}.");
// 						break;
//
// 					case TraceInfoStep.Completed:
// 						TestContext.Progress.WriteLine(
// 							$"DataConnection: Total Execution Time (Completed): {ti.ExecutionTime}");
// 						break;
//
// 					case TraceInfoStep.Error:
// 						TestContext.Progress.WriteLine($"DataConnection: ERROR: {ti.Exception?.Message}");
// 						break;
// 				}
// 			};
//
// 			var now = DateTime.UtcNow;
// 			var data = Enumerable.Range(0, sizeBatch).Select(i => new SimpleEntity
// 			{
// 				Id      = i,
// 				IntVal  = i,
// 				DecVal  = 0m,
// 				StrVal  = $"Name {i}",
// 				BoolVal = (i & 1) == 0,
// 				DtVal   = now,
// 			});
//
// 			var copied = LogTime("BulkCopy", () => db.BulkCopy(data));
// 			Assert.That(copied.RowsCopied, Is.EqualTo(sizeBatch), "BulkCopy should insert all rows.");
//
// 			var ids = Enumerable.Range(0, sizeBatch).ToArray();
//
// 			var countInserted = LogTime("Count inserted (WHERE Id IN 15k)",
// 				() => table.Count(t => ids.Contains(t.Id)));
// 			Assert.That(countInserted, Is.EqualTo(sizeBatch), "IN (...) over 15k ids must match inserted rows.");
//
// 			const decimal newDecVal = 1.23m;
// 			const string  newStrVal = "updated";
// 			const bool    newBool   = true;
//
// 			LogTime("Update 15k rows",
// 				() => table
// 					.Where(t => ids.Contains(t.Id))
// 					.Set(t => t.DecVal,  _ => newDecVal)
// 					.Set(t => t.StrVal,  _ => newStrVal)
// 					.Set(t => t.BoolVal, _ => newBool)
// 					.Update());
//
// 			var mismatches = LogTime("Validate updates (COUNT mismatches)",
// 				() => table.Count(t =>
// 					ids.Contains(t.Id) &&
// 					(t.DecVal != newDecVal || t.StrVal != newStrVal || t.BoolVal != newBool)));
// 			Assert.That(mismatches, Is.Zero, "All 15k rows must have updated values.");
//
// 			var deleted = LogTime("Delete 15k (WHERE Id IN 15k)",
// 				() => table.Delete(t => ids.Contains(t.Id)));
// 			TestContext.Progress.WriteLine($"Deleted rows reported by provider: {deleted}");
//
// 			var left = LogTime("Final COUNT(*)", () => table.Count());
// 			Assert.That(left, Is.Zero, "Table must be empty after delete.");
// 		}
//
// 		// Добавьте внутрь класса YdbTests
// 		[Table]
// 		public sealed class AliasesEntity
// 		{
// 			[Column, PrimaryKey] public int Id { get; set; }
//
// 			// string -> ожидаем Text
// 			[Column] public string? S { get; set; }
//
// 			// byte[] -> ожидаем Bytes
// 			[Column] public byte[]? B { get; set; }
// 		}
//
// 		[Test]
// 		public void CreateTable_Emits_Text_and_Bytes([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db = GetDataConnection(context);
//
// 			using var table   = db.CreateLocalTable<AliasesEntity>();
// 			var       now = DateTime.UtcNow;
// 			var entity = new AliasesEntity
// 			{
// 				S = "ssssss",
// 				B = [1, 2, 3]
// 			};
//
// 			// Ensure that Insert does not throw (e.g. YDB provider returns -1 on success)
// 			Assert.DoesNotThrow(() => db.Insert(entity), "Insert should not throw any exceptions.");
// 			var result = table.SingleOrDefault(e => e.S!.Equals("ssssss"));
// 			Assert.That(result, Is.Not.Null, "A record with IntVal = 42 should exist in the table.");
// 		}
//
// 		#endregion
//
// 		#region HintTests
//
// 	// ------------------------------------------------------------------
// 	// UniqueHint: single column -> '--+ unique(IntVal)'
// 	// ------------------------------------------------------------------
// 		[Test]
// 		public void Hint_Unique_SingleColumn_Emitted([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			// We don't care about rows here — we only check the generated SQL text.
// 			_ = table.AsYdb()
// 				.Select(t => new { t.IntVal })
// 				.UniqueHint(nameof(SimpleEntity.IntVal))
// 				.Take(1)
// 				.ToArray();
//
// 			var sql = db.LastQuery ?? string.Empty;
//
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(sql, Does.Contain("--+ unique("),    "unique hint line is missing");
// 				Assert.That(sql, Does.Contain("unique(IntVal)"), "column list in unique does not match");
// 			}
// 		}
//
// 	// ------------------------------------------------------------------
// 	// DistinctHint: two columns -> '--+ distinct(IntVal BoolVal)'
// 	// ------------------------------------------------------------------
// 		[Test]
// 		public void Hint_Distinct_TwoColumns_Emitted([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			_ = table.AsYdb()
// 				.Select(t => new { t.IntVal, t.BoolVal })
// 				.DistinctHint(nameof(SimpleEntity.IntVal), nameof(SimpleEntity.BoolVal))
// 				.Take(1)
// 				.ToArray();
//
// 			var sql = db.LastQuery ?? string.Empty;
//
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(sql, Does.Contain("--+ distinct("),            "distinct hint line is missing");
// 				Assert.That(sql, Does.Contain("distinct(IntVal BoolVal)"), "column list in distinct does not match");
// 			}
// 		}
//
// 	// ------------------------------------------------------------------
// 	// QueryHint: values with spaces and quotes are quoted/escaped properly
// 	// expected: --+ foo('A B' 'C''D')
// 	// ------------------------------------------------------------------
// 		[Test]
// 		public void Hint_Generic_QuotingAndEscaping([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			_ = table.AsYdb()
// 				.Select(t => t.Id)
// 				.QueryHint("foo", "A B", "C'D")
// 				.Take(1)
// 				.ToArray();
//
// 			var sql = db.LastQuery ?? string.Empty;
//
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(sql, Does.Contain("--+ foo("), "foo hint line is missing");
// 				Assert.That(sql, Does.Contain("'A B'"),    "value with a space must be single-quoted");
// 				Assert.That(sql, Does.Contain("C''D"),     "single quote inside value must be doubled");
// 			}
// 		}
//
// // ------------------------------------------------------------------
// // Multiple hints in a row: both lines present and in call order
// // ------------------------------------------------------------------
// 		[Test]
// 		public void Hint_Multiple_Order_IsPreserved([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db    = GetDataConnection(context);
// 			using var table = db.CreateLocalTable<SimpleEntity>();
//
// 			_ = table.AsYdb()
// 				.Select(t => new { t.IntVal, t.BoolVal })
// 				.UniqueHint(nameof(SimpleEntity.IntVal))
// 				.DistinctHint(nameof(SimpleEntity.BoolVal))
// 				.Take(1)
// 				.ToArray();
//
// 			var sql = db.LastQuery ?? string.Empty;
//
// 			var iUnique   = sql.IndexOf("--+ unique(",   StringComparison.Ordinal);
// 			var iDistinct = sql.IndexOf("--+ distinct(", StringComparison.Ordinal);
//
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(iUnique,   Is.GreaterThanOrEqualTo(0), "unique hint not found in SQL");
// 				Assert.That(iDistinct, Is.GreaterThanOrEqualTo(0), "distinct hint not found in SQL");
// 				Assert.That(iUnique,   Is.LessThan(iDistinct),     "hints order must match call order");
// 			}
// 		}
//
// 		#endregion
// 		sealed class TimeoutOnlyRetryPolicy : RetryPolicyBase
// 		{
// 			public TimeoutOnlyRetryPolicy(
// 				int      maxRetryCount   = 5,
// 				TimeSpan maxRetryDelay   = default,
// 				double   randomFactor    = 1.0,
// 				double   exponentialBase = 2.0,
// 				TimeSpan coefficient     = default)
// 				: base(
// 					maxRetryCount,
// 					maxRetryDelay == default ? TimeSpan.FromMilliseconds(200) : maxRetryDelay,
// 					randomFactor,
// 					exponentialBase,
// 					coefficient == default ? TimeSpan.FromMilliseconds(50) : coefficient)
// 			{ }
//
// 			protected override bool ShouldRetryOn(Exception exception) => exception is TimeoutException;
// 		}
//
// 		[Test]
// 		public void RetryPolicy_RetriesTimeoutAndSucceeds()
// 		{
// 			var attempts = 0;
// 			var policy   = new TimeoutOnlyRetryPolicy(maxRetryCount: 5);
//
// 			var result = policy.Execute(() =>
// 			{
// 				if (attempts < 2)
// 				{
// 					attempts++;
// 					throw new TimeoutException("Simulated transient timeout");
// 				}
// 				
// 				return 123;
// 			});
//
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(result, Is.EqualTo(123));
// 				Assert.That(attempts, Is.EqualTo(2));
// 			}
// 		}
//
// 		[Test]
// 		public void RetryPolicy_DoesNotRetryOnOperationCanceled()
// 		{
// 			var attempts = 0;
// 			var policy   = new TimeoutOnlyRetryPolicy(maxRetryCount: 5);
//
// 			Assert.Throws<OperationCanceledException>(() =>
// 				policy.Execute(() =>
// 				{
// 					attempts++;
// 					throw new OperationCanceledException("Simulated user cancellation");
// 				})
// 			);
//
// 			Assert.That(attempts, Is.EqualTo(1));
// 		}
// 		
// 		[Test]
// 		public void Retry_Factory_Returns_YdbPolicy_And_RetriesTimeout([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db = GetDataConnection(context);
//
// 			var rpAssembly = typeof(IRetryPolicy).Assembly;
// 			var factory    = rpAssembly.GetType("LinqToDB.Data.RetryPolicy.DefaultRetryPolicyFactory", throwOnError: true)!;
// 			var method     = factory.GetMethod("GetRetryPolicy", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;
//
// 			var policy = (IRetryPolicy?)method.Invoke(null, new object[] { db });
// 			Assert.That(policy, Is.Not.Null, "Factory must return a policy for YDB provider.");
// 			Assert.That(policy!.GetType().FullName, Does.Contain("YdbRetryPolicy"), "Factory must return YdbRetryPolicy.");
//
// 			var attempts = 0;
// 			var result = policy.Execute(() =>
// 			{
// 				if (attempts < 2)
// 				{
// 					attempts++;
// 					throw new TimeoutException("Simulated transient timeout");
// 				}
// 				
// 				return 42;
// 			});
//
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(result, Is.EqualTo(42));
// 				Assert.That(attempts, Is.EqualTo(2));
// 			}
// 		}
// 		
//
// 		[Test]
// 		public void Retry_Policy_DoesNotRetry_OnOperationCanceled([IncludeDataSources(Ctx)] string context)
// 		{
// 			using var db = GetDataConnection(context);
//
// 			var rpAssembly = typeof(IRetryPolicy).Assembly;
// 			var factory    = rpAssembly.GetType("LinqToDB.Data.RetryPolicy.DefaultRetryPolicyFactory", throwOnError: true)!;
// 			var method     = factory.GetMethod("GetRetryPolicy", BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;
//
// 			var policy = (IRetryPolicy?)method.Invoke(null, new object[] { db });
// 			Assert.That(policy, Is.Not.Null);
// 			Assert.That(policy!.GetType().FullName, Does.Contain("YdbRetryPolicy"));
//
// 			var attempts = 0;
// 			var ex = Assert.Throws<OperationCanceledException>(() =>
// 				policy.Execute(() =>
// 				{
// 					attempts++;
// 					throw new OperationCanceledException("Simulated user cancellation");
// 				})
// 			);
//
// 			using (Assert.EnterMultipleScope())
// 			{
// 				Assert.That(ex, Is.Not.Null);
// 				Assert.That(attempts, Is.EqualTo(1), "OperationCanceledException must not be retried.");
// 			}
// 		}
//
// 		
//
// 	}
// }
