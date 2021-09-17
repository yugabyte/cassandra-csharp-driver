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

using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Text;
using System.Collections;
using System.Collections.Generic;

namespace Cassandra.YugaByte.Tests
{
    public struct IOMetrics
    {
        public long localRead;
        public long localWrite;
        public long remoteRead;
        public long remoteWrite;

        public static IOMetrics operator -(IOMetrics lhs, IOMetrics rhs)
        {
            lhs.localRead -= rhs.localRead;
            lhs.localWrite -= rhs.localWrite;
            lhs.remoteRead -= rhs.remoteRead;
            lhs.remoteWrite -= rhs.remoteWrite;
            return lhs;
        }

        public static IOMetrics operator +(IOMetrics lhs, IOMetrics rhs)
        {
            lhs.localRead += rhs.localRead;
            lhs.localWrite += rhs.localWrite;
            lhs.remoteRead += rhs.remoteRead;
            lhs.remoteWrite += rhs.remoteWrite;
            return lhs;
        }
    }

    public struct UDT
    {
        public int A { get; set; }
        public string B { get; set; }
        public float C { get; set; }

        public UDT(int a, string b, float c)
        {
            A = a;
            B = b;
            C = c;
        }
    }

    public static class TestUtils
    {
        const string MetricPrefix = "handler_latency_yb_client_";

        public static IOMetrics FetchMetrics(this ICluster cluster)
        {
            IOMetrics result = new IOMetrics();
            using (var client = new WebClient())
            {
                foreach (var host in cluster.AllHosts())
                {
                    var url = string.Format("http://{0}:{1}/metrics", host.Address.Address, 9000);
                    var response = client.DownloadString(url);
                    var json = JArray.Parse(response);
                    foreach (var child in json.Children())
                    {
                        if (child["type"].ToString() == "server")
                        {
                            foreach (var metric in child["metrics"].Children())
                            {
                                var name = metric["name"].ToString();
                                if (name.StartsWith(MetricPrefix))
                                {
                                    var suffix = name.Substring(MetricPrefix.Length);
                                    var value = metric["total_count"].ToObject<long>();
                                    if (suffix == "write_local")
                                    {
                                        result.localWrite += value;
                                    }
                                    else if (suffix == "read_local")
                                    {
                                        result.localRead += value;
                                    }
                                    else if (suffix == "write_remote")
                                    {
                                        result.remoteWrite += value;
                                    }
                                    else if (suffix == "read_remote")
                                    {
                                        result.remoteRead += value;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return result;
        }

        public static string RandomString(this Random rand, int len)
        {
            var bytes = new byte[len];
            for (int i = 0; i != len; ++i)
            {
                bytes[i] = (byte)rand.Next(32, 127);
            }
            return Encoding.ASCII.GetString(bytes);
        }

        public static byte[] RandomBytes(this Random rand, int len)
        {
            var bytes = new byte[len];
            rand.NextBytes(bytes);
            return bytes;
        }

        public static long NextLong(this Random rand)
        {
            return BitConverter.ToInt64(rand.RandomBytes(8), 0);
        }

        public static long NextLong(this Random rand, long min, long max)
        {
            if (min == long.MinValue && max == long.MaxValue)
            {
                return rand.NextLong();
            }
            ulong delta = (ulong)(max - min + 1);
            return min + (long)((ulong)rand.NextLong() % delta);
        }

        public static IPAddress RandomIPAddress(this Random rand)
        {
            return new IPAddress(rand.RandomBytes(4));
        }

        public static Guid RandomGuid(this Random rand)
        {
            return new Guid(rand.RandomBytes(16));
        }

        public static DateTimeOffset RandomDateTimeOffset(this Random rand)
        {
            // Trim up to milliseconds
            return new DateTimeOffset(rand.NextLong(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks) / 1000000 * 1000000, TimeSpan.Zero);
        }

        public static TimeUuid RandomTimeUuid(this Random rand)
        {
            return TimeUuid.NewId(rand.RandomBytes(6), rand.RandomBytes(2), rand.RandomDateTimeOffset());
        }

        public static float RandomFloatNan(this Random rand)
        {
            var bytes = BitConverter.GetBytes(float.NaN);
            bytes[1] = (byte)rand.Next(255);
            var v = BitConverter.ToSingle(bytes, 0);
            return v;
        }

        public static double RandomDoubleNan(this Random rand)
        {
            var v = BitConverter.DoubleToInt64Bits(double.NaN);
            v ^= (long)rand.Next() << 16;
            return BitConverter.Int64BitsToDouble(v);
        }

        private delegate object RandomValueImpl(Random rand);

        private static IDictionary<Type, RandomValueImpl> _randomValueGenerators = new Dictionary<Type, RandomValueImpl>()
        {
            { typeof(string), rand => rand.RandomString(rand.Next(256)) },
            { typeof(byte[]), rand => rand.RandomBytes(rand.Next(256)) },
            { typeof(sbyte), rand => (sbyte)rand.Next(sbyte.MinValue, sbyte.MaxValue) },
            { typeof(short), rand => (short)rand.Next(short.MinValue, short.MaxValue) },
            { typeof(int), rand => rand.Next() },
            { typeof(long), rand => rand.NextLong() },
            { typeof(DateTimeOffset), rand => rand.RandomDateTimeOffset() },
            { typeof(IPAddress), rand => rand.RandomIPAddress() },
            { typeof(Guid), rand => rand.RandomGuid() },
            { typeof(TimeUuid), rand => rand.RandomTimeUuid() },
            { typeof(bool), rand => (rand.Next() & 1) != 0 },
            { typeof(float), rand => (rand.Next() & 15) != 0 ? rand.RandomFloatNan() : (float)rand.NextDouble() },
            { typeof(double), rand => (rand.Next() & 15) != 0 ? rand.RandomDoubleNan() : rand.NextDouble() },
            { typeof(LocalDate), rand => new LocalDate((uint)rand.Next()) },
            { typeof(LocalTime), rand => new LocalTime(rand.NextLong(0, 86399999999999L)) },
            { typeof(UDT), rand => new UDT(rand.Next(), rand.RandomString(rand.Next(256)), (float)rand.NextDouble()) },
        };

        private static IDictionary<Type, string> _typeToColumnType = new Dictionary<Type, string>()
        {
            { typeof(string), "text" },
            { typeof(byte[]), "blob" },
            { typeof(sbyte), "tinyint" },
            { typeof(short), "smallint" },
            { typeof(int), "int" },
            { typeof(long), "bigint" },
            { typeof(DateTimeOffset), "timestamp" },
            { typeof(IPAddress), "inet" },
            { typeof(Guid), "uuid" },
            { typeof(TimeUuid), "timeuuid" },
            { typeof(bool), "boolean" },
            { typeof(float), "float" },
            { typeof(double), "double" },
            { typeof(LocalDate), "date" },
            { typeof(LocalTime), "time" },
            { typeof(UDT), "frozen<udt>" },
        };

        public static object RandomValue(this Random rand, Type type)
        {
            RandomValueImpl impl;
            if (type.IsGenericType)
            {
                if (type.GetGenericTypeDefinition() == typeof(ISet<>))
                {
                    var resultType = typeof(SortedSet<>).MakeGenericType(type.GenericTypeArguments);
                    var result = Activator.CreateInstance(resultType);
                    var addMethod = resultType.GetMethod("Add");
                    for (var left = rand.Next(16); left-- > 0;)
                    {
                        object randomValue = rand.RandomValue(type.GenericTypeArguments[0]);
                        addMethod.Invoke(result, new object[] { randomValue });
                    }
                    return result;
                }
                if (type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    var resultType = typeof(List<>).MakeGenericType(type.GenericTypeArguments);
                    var result = (IList)Activator.CreateInstance(resultType);
                    for (var left = rand.Next(16); left-- > 0;)
                    {
                        object randomValue = rand.RandomValue(type.GenericTypeArguments[0]);
                        result.Add(randomValue);
                    }
                    return result;
                }
                if (type.GetGenericTypeDefinition() == typeof(IDictionary<,>))
                {
                    var resultType = typeof(SortedDictionary<,>).MakeGenericType(type.GenericTypeArguments);
                    var result = (IDictionary)Activator.CreateInstance(resultType);
                    for (var left = rand.Next(16); left-- > 0;)
                    {
                        object randomKey = rand.RandomValue(type.GenericTypeArguments[0]);
                        object randomValue = rand.RandomValue(type.GenericTypeArguments[1]);
                        result.Add(randomKey, randomValue);
                    }
                    return result;
                }
            }
            if (!_randomValueGenerators.TryGetValue(type, out impl))
            {
                throw new ArgumentException("Cannot generate value for type: " + type);
            }
            return impl(rand);
        }

        public static string TypeToColumnType(Type type)
        {
            string result;
            if (type.IsGenericType)
            {
                if (type.GetGenericTypeDefinition() == typeof(ISet<>))
                {
                    return string.Format("frozen<set<{0}>>", TypeToColumnType(type.GenericTypeArguments[0]));
                }
                if (type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    return string.Format("frozen<list<{0}>>", TypeToColumnType(type.GenericTypeArguments[0]));
                }
                if (type.GetGenericTypeDefinition() == typeof(IDictionary<,>))
                {
                    return string.Format("frozen<map<{0}, {1}>>", TypeToColumnType(type.GenericTypeArguments[0]), TypeToColumnType(type.GenericTypeArguments[1]));
                }
            }
            if (!_typeToColumnType.TryGetValue(type, out result))
            {
                throw new ArgumentException("Cannot convert type to column type: " + type);
            }
            return result;
        }

        public static string TypeToTableName(string prefix, string type)
        {
            return prefix + type.Replace('<', '_').Replace(">", "").Replace(',', '_').Replace(" ", "").Replace("frozen_", "");
        }

        public static JArray GetArray(this JObject obj, string name)
        {
            return (JArray)obj.GetValue(name);
        }

        public static JObject GetObject(this JObject obj, string name)
        {
            return (JObject)obj.GetValue(name);
        }

        public static T GetValue<T>(this JObject obj, string name)
        {
            return obj.GetValue(name).Value<T>();
        }
    }
}
