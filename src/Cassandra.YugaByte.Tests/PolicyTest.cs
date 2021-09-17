// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

using System;
using System.Diagnostics;
using System.Net;
using System.Linq;
using System.Collections.Generic;
using NUnit.Framework;

namespace Cassandra.YugaByte.Tests
{
    [TestFixture]
    public class PolicyTest
    {
        const int TotalKeys = 100;

        delegate object CreateKey(int index);

        private ICluster _cluster;
        private ISession _session;

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            var ip = Environment.GetEnvironmentVariable("YB_CLUSTER_ADDRESS");
            _cluster = Cluster.Builder().AddContactPoint(ip).Build();
            _session = _cluster.Connect();
            _session.Execute("CREATE KEYSPACE IF NOT EXISTS test");
            _session.Execute("USE test");
        }

        [Test]
        public void PrimaryKeyTime()
        {
            TestPrimitive("time", i => new LocalTime(i * 57809));
        }

        //[Test]
        public void PrimaryKeyDate()
        {
            TestPrimitive("date", i => new LocalDate(1900 * i, 1 + i % 12, 1 + i % 28));
        }

       [Test]
        public void PrimaryKeyInet()
        {
            TestPrimitive("inet", i => IPAddress.Parse(string.Format("{0}.1.{0}.2", i)));
        }

        [Test]
        public void PrimaryKeyTimeUuid()
        {
            TestPrimitive("timeuuid", CreateTimeUuidByIndex);
        }

        [Test]
        public void PrimaryKeyUuid()
        {
            TestPrimitive("uuid", CreateGuidByIndex);
        }

        [Test]
        public void PrimaryKeyTimestamp()
        {
            TestPrimitive("timestamp", i => CreateTimestampByIndex(i));
        }

        [Test]
        public void PrimaryKeyFloat()
        {
            TestPrimitive("float", i => (float)i / 3);
        }

        [Test]
        public void PrimaryKeyDouble()
        {
            TestPrimitive("double", i => (double)i / 3);
        }

        [Test]
        public void PrimaryKeyBlob()
        {
            TestPrimitive("blob", i => BitConverter.GetBytes(i));
        }

        [Test]
        public void PrimaryKeyText()
        {
            TestPrimitive("text", i => "key_" + i);
        }

        [Test]
        public void PrimaryKeyTinyint()
        {
            TestPrimitive("tinyint", i => (sbyte)i);
        }

        [Test]
        public void PrimaryKeySmallint()
        {
            TestPrimitive("smallint", i => (short)i);
        }

        [Test]
        public void PrimaryKeyInt()
        {
            TestPrimitive("int", i => i);
        }

        [Test]
        public void PrimaryKeyBigint()
        {
            TestPrimitive("bigint", i => (long)i);
        }

        [Test]
        public void PrimaryKeyVarchar()
        {
            TestPrimitive("varchar", i => "key_" + i);
        }

        [Test]
        public void PrimaryKeyBoolean()
        {
            TestPrimitive("boolean", i => (i & 1) != 0, 2);
        }

        [Test]
        public void PrimaryKeyFrozenList()
        {
            TestPrimitive("frozen<list<int>>", i => new int[] { i, i * 2, i * 3 });
        }

        [Test]
        public void PrimaryKeyFrozenSet()
        {
            TestPrimitive("frozen<set<text>>", i => new string[] { "a_" + i, "b_" + i });
        }

        [Test]
        public void PrimaryKeyFrozenMap()
        {
            TestPrimitive("frozen<map<int, text>>", i => new Dictionary<int, string>() { { i, "a_" + i }, { i + 1, "b_" + i } });
        }

        [Test]
        public void PrimaryKeyFrozenMapList()
        {
            TestPrimitive(
                "frozen<map<int, frozen<list<text>>>>", 
                i => new Dictionary<int, string[]>() {
                    { i, new string[] { "a_" + i } },
                    { i + 1, new string[] {"b_" + i, "c_" + i } } });
        }

        [Test]
        public void TokenTextAndBlob()
        {
            TestToken(typeof(string), typeof(string), typeof(byte[]), typeof(string));
        }

        [Test]
        public void TokenIntAndText()
        {
            TestToken(typeof(sbyte), typeof(short), typeof(string), typeof(int), typeof(long));
        }

        [Test]
        public void TokenMixed()
        {
            TestToken(typeof(int), typeof(DateTimeOffset), typeof(IPAddress), typeof(Guid), typeof(TimeUuid));
        }

        [Test]
        public void TokenFloats()
        {
            TestTokenEx(100, typeof(float), typeof(double));
        }

        [Test]
        public void TokenDateTime()
        {
            TestToken(typeof(LocalDate), typeof(LocalTime));
        }

        [Test]
        public void TokenBool()
        {
            TestToken(typeof(bool));
        }

        //[Test]
        public void TokenFrozen()
        {
            _session.Execute("DROP TYPE IF EXISTS udt");
            _session.Execute("CREATE TYPE udt(a int, b text, c float)");
            _session.UserDefinedTypes.Define(
                UdtMap.For<UDT>("udt")
                    .Map(v => v.A, "a")
                    .Map(v => v.B, "b")
                    .Map(v => v.C, "c")
            );

            TestToken(typeof(IDictionary<int, string>), typeof(ISet<double>), typeof(IEnumerable<ISet<string>>), typeof(UDT));
        }

        private void TestToken(params Type[] types)
        {
            TestTokenEx(1, types);
        }

        private void TestTokenEx(int repeats, params Type[] types)
        {
            var tableName = TestUtils.TypeToTableName("token_", string.Join("_", types.Select(type => TestUtils.TypeToColumnType(type))));
            _session.Execute(string.Format("DROP TABLE IF EXISTS {0}", tableName));
            var columns = string.Join(", ", types.Select((type, idx) => string.Format("h{1} {0}", TestUtils.TypeToColumnType(type), idx)));
            var primaryKey = string.Join(", ", types.Select((type, idx) => "h" + idx));
            var questions = string.Join(", ", Enumerable.Repeat("?", types.Length));
            _session.Execute(string.Format("CREATE TABLE IF NOT EXISTS {0} ({1}, PRIMARY KEY (({2})));", tableName, columns, primaryKey));

            var rand = new Random();
            var values = new object[types.Length];
            for (var repeat = 0; repeat != repeats; ++repeat)
            {
                for (var i = 0; i != types.Length; ++i)
                {
                    values[i] = rand.RandomValue(types[i]);
                }
                var stmt = _session.Prepare(string.Format("INSERT INTO {0} ({1}) VALUES ({2})", tableName, primaryKey, questions)).Bind(values);
                _session.Execute(stmt);

                var token = PartitionAwarePolicy.YBHashCode(_cluster, stmt);
                Trace.TraceInformation("Token: {0}", token);
                var bs = _session.Prepare(string.Format("SELECT * FROM {0} WHERE TOKEN({1}) = ?", tableName, primaryKey)).Bind(token);
                var row = _session.Execute(bs).FirstOrDefault();
                Assert.NotNull(row);
                for (var i = 0; i != types.Length; ++i)
                {
                    Assert.AreEqual(values[i], row.GetValue(types[i], "h" + i));
                }
            }

            _session.Execute(string.Format("DROP TABLE {0}", tableName));
        }

        [Test]
        public void DML()
        {
            _session.Execute("CREATE TABLE IF NOT EXISTS test_lb (h1 INT, h2 TEXT, c INT, PRIMARY KEY ((h1, h2)));");
            _session.Cluster.Metadata.RefreshSchema();

            var preMetrics = _cluster.FetchMetrics();
            var ps = _session.Prepare("INSERT INTO test_lb (h1, h2, c) VALUES (?, ?, ?)");

            for (int i = 1; i <= TotalKeys; ++i)
            {
                _session.Execute(ps.Bind(i, "v" + i, i));
            }

            var deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Local inserts: {0}", deltaMetrics.localWrite);
            Assert.Greater(deltaMetrics.localWrite * 10, TotalKeys * 7);
            preMetrics += deltaMetrics;

            ps = _session.Prepare("UPDATE test_lb SET c = ? WHERE h1 = ? AND h2 = ?");
            for (int i = 1; i <= TotalKeys; ++i)
            {
                _session.Execute(ps.Bind(i*2, i, "v" + i));
            }

            deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Local updates: {0}", deltaMetrics.localWrite);
            Assert.Greater(deltaMetrics.localWrite * 10, TotalKeys * 7);
            preMetrics += deltaMetrics;

            ps = _session.Prepare("SELECT c FROM test_lb WHERE h1 = ? AND h2 = ?");
            for (int i = 1; i <= TotalKeys; ++i)
            {
                Row row = _session.Execute(ps.Bind(i, "v" + i)).FirstOrDefault();
                Assert.NotNull(row);
                Assert.AreEqual(i * 2, row.GetValue<int>("c"));
            }

            deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Local selects: {0}", deltaMetrics.localRead);
            Assert.Greater(deltaMetrics.localRead * 10, TotalKeys * 7);
            preMetrics += deltaMetrics;

            ps = _session.Prepare("DELETE FROM test_lb WHERE h1 = ? AND h2 = ?");
            for (int i = 1; i <= TotalKeys; ++i)
            {
                _session.Execute(ps.Bind(i, "v" + i));
            }

            deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Local deletes: {0}", deltaMetrics.localWrite);
            Assert.Greater(deltaMetrics.localWrite * 10, TotalKeys * 7);

            _session.Execute("DROP TABLE test_lb");
        }

        [Test]
        public void Index()
        {
            _session.Execute("DROP TABLE IF EXISTS test_lb_idx");
            _session.Execute("CREATE TABLE test_lb_idx (h1 INT, h2 TEXT, c INT, PRIMARY KEY ((h1, h2))) " +
                             "WITH TRANSACTIONS = { 'enabled' : true };");
            _session.Execute("CREATE INDEX test_lb_idx_1 ON test_lb_idx (h1) INCLUDE (c);");
            _session.Execute("CREATE INDEX test_lb_idx_2 ON test_lb_idx (c);");
            _session.Cluster.Metadata.RefreshSchema();

            var preMetrics = _cluster.FetchMetrics();
            var ps = _session.Prepare("INSERT INTO test_lb_idx (h1, h2, c) VALUES (?, ?, ?)");
            for (int i = 1; i <= TotalKeys; ++i)
            {
                _session.Execute(ps.Bind(i, "v" + i, i));
            }

            var deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Local inserts: {0}", deltaMetrics.localWrite);
            Assert.Greater(deltaMetrics.localWrite * 10, TotalKeys * 7);
            preMetrics += deltaMetrics;

            ps = _session.Prepare("SELECT c FROM test_lb_idx WHERE h1 = ?");
            for (int i = 1; i <= TotalKeys; ++i)
            {
                Row row = _session.Execute(ps.Bind(i)).FirstOrDefault();
                Assert.NotNull(row);
                Assert.AreEqual(i, row.GetValue<int>("c"));
            }

            deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Selects, local: {0}, remote: {1}", deltaMetrics.localRead, deltaMetrics.remoteRead);
            Assert.Greater(deltaMetrics.localRead * 10, TotalKeys * 7);
            preMetrics += deltaMetrics;

            ps = _session.Prepare("SELECT h1, h2 FROM test_lb_idx WHERE c = ?");
            for (int i = 1; i <= TotalKeys; ++i)
            {
                Row row = _session.Execute(ps.Bind(i)).FirstOrDefault();
                Assert.NotNull(row);
                Assert.AreEqual(i, row.GetValue<int>("h1"));
                Assert.AreEqual("v" + i, row.GetValue<string>("h2"));
            }

            deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Selects, local: {0}, remote: {1}", deltaMetrics.localRead, deltaMetrics.remoteRead);
            Assert.Greater(deltaMetrics.localRead * 10, TotalKeys * 7);

            _session.Execute("DROP TABLE test_lb_idx");
        }

        [Test]
        public void BatchStatement()
        {
            // Create test table.
            _session.Execute("CREATE TABLE IF NOT EXISTS test_lb (h INT, r TEXT, c INT, PRIMARY KEY ((h), r))");
            _session.Cluster.Metadata.RefreshSchema();

            var preMetrics = _cluster.FetchMetrics();
            var ps = _session.Prepare("INSERT INTO test_lb (h, r, c) VALUES (?, ?, ?)");

            for (int i = 1; i <= TotalKeys; ++i)
            {
                BatchStatement batch = new BatchStatement();
                for (int j = 1; j <= 5; j++)
                {
                 batch.Add(ps.Bind(i, "v" + j, i * j));
                }
                _session.Execute(batch);
            }

            var deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Inserts, local: {0}, remote: {1}", deltaMetrics.localWrite, deltaMetrics.remoteWrite);
            Assert.Greater(deltaMetrics.localWrite * 10, TotalKeys * 7);
            preMetrics += deltaMetrics;

            ps = _session.Prepare("SELECT c FROM test_lb WHERE h = ? and r = 'v3'");
            for (int i = 1; i <= TotalKeys; ++i)
            {
                Row row = _session.Execute(ps.Bind(i)).FirstOrDefault();
                Assert.NotNull(row);
                Assert.AreEqual(i * 3, row.GetValue<int>("c"));
            }

            deltaMetrics = _cluster.FetchMetrics() - preMetrics;
            Trace.TraceInformation("Selects, local: {0}, remote: {1}", deltaMetrics.localRead, deltaMetrics.remoteRead);
            Assert.Greater(deltaMetrics.localRead * 10, TotalKeys * 7);

            _session.Execute("DROP TABLE test_lb");
        }

        private void TestPrimitive(string type, CreateKey createKey, int totalKeys = TotalKeys)
        {
            Trace.TraceInformation("Testing {0}, keys: {1}", type, totalKeys);
            var preMetrics = _cluster.FetchMetrics();
            TestPartition(type, createKey, totalKeys);
            var postMetrics = _cluster.FetchMetrics();
            var localReads = postMetrics.localRead - preMetrics.localRead;
            var localWrites = postMetrics.localWrite - preMetrics.localWrite;
            var oldColor = Console.ForegroundColor;
            Trace.TraceInformation("Local reads = {0}, local writes = {1}", localReads, localWrites);
            Trace.TraceInformation("Remote reads = {0}, remote writes = {1}", postMetrics.remoteRead - preMetrics.remoteRead, postMetrics.remoteWrite - preMetrics.remoteWrite);
            Assert.Greater(localReads * 10, totalKeys * 7);
            Assert.Greater(localWrites * 10, totalKeys * 7);
        }

        private void TestPartition(string type, CreateKey createKey, int totalKeys)
        {
            var tableName = TestUtils.TypeToTableName("simple_", type);
            _session.Execute(string.Format("CREATE TABLE IF NOT EXISTS {0}(id {1} PRIMARY KEY, data varchar)", tableName, type));
            _session.Cluster.Metadata.RefreshSchema();
            Trace.TraceInformation("Created table {0}", tableName);
            var ps = _session.Prepare(string.Format("INSERT INTO {0} (id, data) VALUES (?, ?)", tableName));

            for (int i = 0; i != totalKeys; ++i)
            {
                _session.Execute(ps.Bind(createKey(i), CreateData(i)));
            }

            ps = _session.Prepare(string.Format("SELECT data FROM {0} WHERE id = ?", tableName));

            for (int i = 0; i != totalKeys; ++i)
            {
                var rs = _session.Execute(ps.Bind(createKey(i)));
                var count = 0;
                foreach (var row in rs)
                {
                    var data = row.GetValue<string>("data");
                    Assert.AreEqual(data, CreateData(i));
                    ++count;
                }
                Assert.AreEqual(count, 1);
            }
            _session.Execute(string.Format("DROP TABLE {0}", tableName));
        }

        private string CreateData(int index)
        {
            return "data_" + index;
        }

        private object CreateGuidByIndex(int i)
        {
            byte b = (byte)i;
            return new Guid(i, (short)i, (short)i, (byte)(i + 1), (byte)(i + 2), (byte)(i + 3), (byte)(i + 4), (byte)(i + 5), (byte)(i + 6), (byte)(i + 7), (byte)(i + 8));
        }

        private object CreateTimeUuidByIndex(int i)
        {
            var b = (byte)i;
            var nodeId = new byte[6] { b, b, b, b, b, b };
            var clockId = new byte[2] { b, b };
            return TimeUuid.NewId(nodeId, clockId, CreateTimestampByIndex(i));
        }

        private DateTimeOffset CreateTimestampByIndex(int i)
        {
            return new DateTimeOffset(2018 + i, 1, 1, i % 24, 0, i % 60, 0, TimeSpan.Zero).AddTicks(i * 997);
        }
    }
}
