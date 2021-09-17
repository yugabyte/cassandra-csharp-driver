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
using System.Linq;
using NUnit.Framework;
using Newtonsoft.Json.Linq;

namespace Cassandra.YugaByte.Tests
{
    [TestFixture]
    class JsonTest
    {
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
        public void Basic()
        {
            _session.Execute("DROP TABLE IF EXISTS books");
            _session.Execute("CREATE TABLE books (id int PRIMARY KEY, detail jsonb)");

            var ps = _session.Prepare("INSERT INTO books (id, detail) VALUES (?, ?)");

            var detail = new JObject();
            detail.Add("name", new JValue("Macbeth"));
            var author = new JObject();
            author.Add("first_name", new JValue("William"));
            author.Add("last_name", new JValue("Shakespeare"));
            //Console.WriteLine(detail.ToString());
            var statement = ps.Bind(1, detail.ToString());
            _session.Execute(statement);

            ps = _session.Prepare("SELECT detail FROM books WHERE id=?");
            var rs = _session.Execute(ps.Bind(1));
            foreach (var row in rs)
            {
                var parsedDetails = JObject.Parse(row.GetValue<string>("detail"));
                Assert.AreEqual(detail, parsedDetails);
            }
        }

        [Test]
        public void Json()
        {
            const string json =
            "{ " +
              "\"b\" : 1," +
              "\"a2\" : {}," +
              "\"a3\" : \"\"," +
              "\"a1\" : [1, 2, 3.0, false, true, { \"k1\" : 1, \"k2\" : [100, 200, 300], \"k3\" : true}]," +
              "\"a\" :" +
              "{" +
                "\"d\" : true," +
                "\"q\" :" +
                  "{" +
                    "\"p\" : 4294967295," +
                    "\"r\" : -2147483648," +
                    "\"s\" : 2147483647" +
                  "}," +
                "\"g\" : -100," +
                "\"c\" : false," +
                "\"f\" : \"hello\"," +
                "\"x\" : 2.0," +
                "\"y\" : 9223372036854775807," +
                "\"z\" : -9223372036854775808," +
                "\"u\" : 18446744073709551615," +
                "\"l\" : 2147483647.123123e+75," +
                "\"e\" : null" +
              "}" +
            "}";

            _session.Execute("DROP TABLE IF EXISTS test_json");
            _session.Execute("CREATE TABLE test_json(c1 int, c2 jsonb, PRIMARY KEY(c1))");
            _session.Execute(_session.Prepare("INSERT INTO test_json(c1, c2) values (1, ?)").Bind(json));
            _session.Execute("INSERT INTO test_json(c1, c2) values (2, '\"abc\"');");
            _session.Execute("INSERT INTO test_json(c1, c2) values (3, '3');");
            _session.Execute("INSERT INTO test_json(c1, c2) values (4, 'true');");
            _session.Execute("INSERT INTO test_json(c1, c2) values (5, 'false');");
            _session.Execute("INSERT INTO test_json(c1, c2) values (6, 'null');");
            _session.Execute("INSERT INTO test_json(c1, c2) values (7, '2.0');");
            _session.Execute("INSERT INTO test_json(c1, c2) values (8, '{\"b\" : 1}');");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c1 = 1"));

            // Invalid inserts.
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, abc);");
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, 'abc');");
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, 1);");
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, 2.0);");
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, null);");
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, true);");
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, false);");
            ExecuteInvalid("INSERT INTO test_json(c1, c2) values (123, '{a:1, \"b\":2}');");

            // Test operators.
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
                "'4294967295'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->>'p' = " +
                "'4294967295'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->>'p' = " +
                "'4294967295' AND c1 = 1"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c1 = 1 AND c2->'a'->'q'->>'p' " +
                "= '4294967295'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a1'->5->'k2'->1 = '200'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a1'->5->'k3' = 'true'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a1'->0 = '1'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a2' = '{}'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a3' = '\"\"'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'e' = 'null'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'c' = 'false'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->>'f' = 'hello'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'f' = '\"hello\"'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->>'x' = '2.000000'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q' = " +
                "'{\"r\": -2147483648, \"p\": 4294967295,  \"s\": 2147483647}'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->>'q' = " +
                "'{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}'"));

            TestScalar("\"abc\"", 2);
            TestScalar("3", 3);
            TestScalar("true", 4);
            TestScalar("false", 5);
            TestScalar("null", 6);
            TestScalar("2.0", 7);
            Assert.AreEqual(2, _session.Execute("SELECT * FROM test_json WHERE c2->'b' = '1'")
                .ToArray().Length);

            // Test multiple where expressions.
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'g' = '-100' " +
                "AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) = 2.0"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
                "integer) < 0 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) > 1.0"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
                "integer) <= -100 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) >= 2.0"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
                " IN (-100, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) IN (1.0, 2.0)"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
                " NOT IN (-10, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) IN (1.0, 2.0)"));

            // Test negative where expressions.
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a'->'g' = '-90' " +
                "AND c1 = 1").ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a'->'g' = '-90' " +
                "AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) = 2.0").ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
                "integer) < 0 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) < 1.0").ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
                "integer) <= -110 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) >= 2.0").
                ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
                " IN (-100, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) IN (1.0, 2.3)")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
                " IN (-100, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) NOT IN (1.0, 2.0)")
                .ToArray().Length);

            // Test invalid where expressions.
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a'->'g' = '-100' AND c2 = '{}'");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2 = '{} AND c2->'a'->'g' = '-100'");

            // Test invalid operators. We should never return errors, just return an empty result (this
            // is what postgres does).
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'b'->'c' = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'z' = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->2 = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a'->2 = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a1'->'b' = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a1'->6 = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a2'->'a' = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a3'->'a' = '1'")
                .ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a3'->2 = '1'")
                .ToArray().Length);

            // Test invalid rhs for where clause.
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a1'->5->'k2'->1 = 200");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a1'->5->'k3' = true");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a1'->0 = 1");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a2' = '{a:1}'");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a3' = ''");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a'->'e' = null");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a'->'c' = false");
            ExecuteInvalid("SELECT * FROM test_json WHERE c2->'a'->>'f' = hello");

            // Test json operators in select clause.
            Assert.AreEqual("4294967295",
                _session.Execute(
                    "SELECT c2->'a'->'q'->>'p' FROM test_json WHERE c1 = 1").First().GetValue<string>(0));
            Assert.AreEqual("200",
                _session.Execute(
                    "SELECT c2->'a1'->5->'k2'->1 FROM test_json WHERE c1 = 1").First().GetValue<string>(0));
            Assert.AreEqual("true",
                _session.Execute(
                    "SELECT c2->'a1'->5->'k3' FROM test_json WHERE c1 = 1").First().GetValue<string>(0));
            Assert.AreEqual("2.000000",
                _session.Execute(
                    "SELECT c2->'a'->>'x' FROM test_json WHERE c1 = 1").First().GetValue<string>(0));
            Assert.AreEqual("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
                _session.Execute(
                    "SELECT c2->'a'->'q' FROM test_json WHERE c1 = 1").First().GetValue<string>(0));
            Assert.AreEqual("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
                _session.Execute(
                    "SELECT c2->'a'->>'q' FROM test_json WHERE c1 = 1").First().GetValue<string>(0));
            Assert.AreEqual("\"abc\"",
                _session.Execute(
                    "SELECT c2 FROM test_json WHERE c1 = 2").First().GetValue<string>(0));
            Assert.AreEqual("true",
                _session.Execute(
                    "SELECT c2 FROM test_json WHERE c1 = 4").First().GetValue<string>(0));
            Assert.AreEqual("false",
                _session.Execute(
                    "SELECT c2 FROM test_json WHERE c1 = 5").First().GetValue<string>(0));

            // Json operators in both select and where clause.
            Assert.AreEqual("4294967295",
                _session.Execute(
                    "SELECT c2->'a'->'q'->>'p' FROM test_json WHERE c2->'a1'->5->'k2'->1 = '200'").First().GetValue<string>(0));
            Assert.AreEqual("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
                _session.Execute(
                    "SELECT c2->'a'->'q' FROM test_json WHERE c2->'a1'->5->'k3' = 'true'").First().GetValue<string>(0));

            Assert.AreEqual("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
                _session.Execute(
                    "SELECT c2->'a'->'q' FROM test_json WHERE c2->'a1'->5->'k3' = 'true'").First().GetValue<string>("expr"));

            // Test select with invalid operators, which should result in empty rows.
            VerifyNullRows(_session.Execute("SELECT c2->'b'->'c' FROM test_json WHERE c1 = 1"), 1);
            VerifyNullRows(_session.Execute("SELECT c2->'z' FROM test_json"), 8);
            VerifyNullRows(_session.Execute("SELECT c2->2 FROM test_json"), 8);
            VerifyNullRows(_session.Execute("SELECT c2->'a'->2 FROM test_json"), 8);
            VerifyNullRows(_session.Execute("SELECT c2->'a1'->'b' FROM test_json"), 8);
            VerifyNullRows(_session.Execute("SELECT c2->'a1'->6 FROM test_json"), 8);
            VerifyNullRows(_session.Execute("SELECT c2->'a1'->'a' FROM test_json"), 8);
            VerifyNullRows(_session.Execute("SELECT c2->'a3'->'a' FROM test_json"), 8);
            VerifyNullRows(_session.Execute("SELECT c2->'a3'->2 FROM test_json"), 8);

            // Test casts.
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) = 4294967295"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "decimal) = 4294967295"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "text) = '4294967295'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'r' as " +
                "integer) = -2147483648"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'r' as " +
                "text) = '-2147483648'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'s' as " +
                "integer) = 2147483647"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a1'->5->'k2'->>1 as " +
                "integer) = 200"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'x' as float) =" +
                " 2.0"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'x' as double) " +
                "= 2.0"));

            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) >= 4294967295"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) > 100"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) <= 4294967295"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) < 4294967297"));

            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) >= 4294967297").ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) > 4294967298").ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) = 100").ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "bigint) < 99").ToArray().Length);
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
                "decimal) < 99").ToArray().Length);

            // Invalid cast types.
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as boolean) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as inet) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as map) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as set) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as list) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as timestamp) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as timeuuid) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as uuid) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as varint) = 123");
            ExecuteInvalid("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->'p' as text) = '123'");

            // Test update.
            _session.Execute("UPDATE test_json SET c2->'a'->'q'->'p' = '100' WHERE c1 = 1");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
                "'100'"));
            _session.Execute("UPDATE test_json SET c2->'a'->'q'->'p' = '\"100\"' WHERE c1 = 1 IF " +
                "c2->'a'->'q'->'s' = '2147483647' AND c2->'a'->'q'->'r' = '-2147483648'");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
                "'\"100\"'"));
            _session.Execute("UPDATE test_json SET c2->'a1'->5->'k2'->2 = '2000' WHERE c1 = 1");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a1'->5->'k2'->2 = " +
                "'2000'"));
            _session.Execute("UPDATE test_json SET c2->'a2' = '{\"x1\": 1, \"x2\": 2, \"x3\": 3}' WHERE c1" +
                " = 1");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a2' = " +
                "'{\"x1\": 1, \"x2\": 2, \"x3\": 3}'"));
            _session.Execute("UPDATE test_json SET c2->'a'->'e' = '{\"y1\": 1, \"y2\": {\"z1\" : 1}}' " +
                "WHERE c1 = 1");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'e' = " +
                "'{\"y1\": 1, \"y2\": {\"z1\" : 1}}'"));

            // Test updates that don't apply.
            _session.Execute("UPDATE test_json SET c2->'a'->'q'->'p' = '\"200\"' WHERE c1 = 1 IF " +
                "c2->'a'->'q'->'s' = '2' AND c2->'a'->'q'->'r' = '-2147483648'");
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
                "'\"200\"'").ToArray().Length);

            // Invalid updates.
            // Invalid rhs (needs to be valid json)
            ExecuteInvalid("UPDATE test_json SET c2->'a'->'q'->'p' = 100 WHERE c1 = 1");
            // non-existent key.
            ExecuteInvalid("UPDATE test_json SET c2->'aa'->'q'->'p' = '100' WHERE c1 = 1");
            // Array out of bounds.
            ExecuteInvalid("UPDATE test_json SET c2->'a1'->200->'k2'->2 = '2000' WHERE c1 = 1");
            ExecuteInvalid("UPDATE test_json SET c2->'a1'->-2->'k2'->2 = '2000' WHERE c1 = 1");
            ExecuteInvalid("UPDATE test_json SET c2->'a1'->5->'k2'->100 = '2000' WHERE c1 = 1");
            ExecuteInvalid("UPDATE test_json SET c2->'a1'->5->'k2'->-1 = '2000' WHERE c1 = 1");
            // Mixup arrays and objects.
            ExecuteInvalid("UPDATE test_json SET c2->'a'->'q'->1 = '100' WHERE c1 = 1");
            ExecuteInvalid("UPDATE test_json SET c2->'a1'->5->'k2'->'abc' = '2000' WHERE c1 = 1");
            ExecuteInvalid("UPDATE test_json SET c2->5->'q'->'p' = '100' WHERE c1 = 1");
            ExecuteInvalid("UPDATE test_json SET c2->'a1'->'b'->'k2'->2 = '2000' WHERE c1 = 1");
            // Invalid RHS.
            ExecuteInvalid("UPDATE test_json SET c2->'a'->'q'->'p' = c1->'a' WHERE c1 = 1");
            ExecuteInvalid("UPDATE test_json SET c2->'a'->'q'->>'p' = c2->>'b' WHERE c1 = 1");


            // Update the same column multiple times.
            _session.Execute("UPDATE test_json SET c2->'a'->'q'->'r' = '200', c2->'a'->'x' = '2', " +
                "c2->'a'->'l' = '3.0' WHERE c1 = 1");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'r' = '200'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'x' = '2'"));
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'l' = '3.0'"));

            // Can't set entire column and nested attributes at the same time.
            ExecuteInvalid("UPDATE test_json SET c2->'a'->'q'->'r' = '200', c2 = '{a : 1, b: 2}' WHERE c1" +
                " = 1");
            // Subscript args with json not allowed.
            ExecuteInvalid("UPDATE test_json SET c2->'a'->'q'->'r' = '200', c2[0] = '1' WHERE c1 = 1");

            // Test delete with conditions.
            // Test deletes that don't apply.
            _session.Execute("DELETE FROM test_json WHERE c1 = 1 IF " +
                "c2->'a'->'q'->'s' = '200' AND c2->'a'->'q'->'r' = '-2147483648'");
            VerifyRowSet(_session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
                "'\"100\"'"));

            // Test delete that applies.
            _session.Execute("DELETE FROM test_json WHERE c1 = 1 IF " +
                "c2->'a'->'q'->'s' = '2147483647' AND c2->'a'->'q'->'r' = '200'");
            Assert.AreEqual(0, _session.Execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
                "'\"100\"'").ToArray().Length);
        }

        private void VerifyRowSet(RowSet rs)
        {
            var rows = rs.ToArray();
            Assert.AreEqual(1, rows.Length);
            Row row = rows[0];
            var jsonObject = JObject.Parse(row.GetValue<string>("c2"));
            Assert.AreEqual(1, jsonObject.GetValue<int>("b"));
            var a1 = jsonObject.GetArray("a1");
            Assert.False(a1[3].Value<bool>());
            Assert.AreEqual(3.0, a1[2].Value<double>(), 1e-9);
            Assert.AreEqual(200, a1[5].Value<JObject>().GetArray("k2")[1].Value<int>());
            var a = jsonObject.GetObject("a");
            Assert.AreEqual(2147483647, a.GetObject("q").GetValue<int>("s"));
            Assert.AreEqual("hello", a.GetValue<string>("f"));
        }

        private void ExecuteInvalid(string query)
        {
            try
            {
                _session.Execute(query);
                Assert.Fail("Statement did not fail: {0}", query);
            }
            catch (QueryValidationException qv)
            {
                Trace.TraceInformation("Expected exception", qv);
            }
        }

        private void TestScalar(string json, int c1)
        {
            Row row = _session.Execute(_session.Prepare("SELECT * FROM test_json WHERE c2 = ?").Bind(json)).First();
            Assert.AreEqual(c1, row.GetValue<int>("c1"));
            Assert.AreEqual(json, row.GetValue<string>("c2"));
        }

        private void VerifyNullRows(RowSet rs, int expected_rows)
        {
            var rows = rs.ToArray();
            Assert.AreEqual(expected_rows, rows.Length);
            foreach (var row in rows)
            {
                Assert.True(row.IsNull(0));
            }
        }
    }
}
