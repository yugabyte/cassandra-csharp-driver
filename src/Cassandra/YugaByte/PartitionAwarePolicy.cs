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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using Cassandra.Serialization;

namespace Cassandra.YugaByte
{
    public class PartitionAwarePolicy : ILoadBalancingPolicy
    {
        private readonly ILoadBalancingPolicy _childPolicy;
        private ICluster _cluster;

        public PartitionAwarePolicy(ILoadBalancingPolicy childPolicy)
        {
            _childPolicy = childPolicy;
        }

        public void Initialize(ICluster cluster)
        {
            _childPolicy.Initialize(cluster);
            _cluster = cluster;
        }

        public HostDistance Distance(Host host)
        {
            return _childPolicy.Distance(host);
        }

        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            IEnumerable<Host> result = null;
            var boundStatement = query as BoundStatement;
            if (boundStatement != null)
            {
                result = NewQueryPlanImpl(keyspace, boundStatement);
            }
            else
            {
                var batchStatement = query as BatchStatement;
                if (batchStatement != null)
                {
                    result = NewQueryPlanImpl(keyspace, batchStatement);
                }
            }
            if (result == null)
            {
                result = _childPolicy.NewQueryPlan(keyspace, query);
            }
            return result;
        }

        private IEnumerable<Host> NewQueryPlanImpl(string keyspace, BatchStatement batchStatement)
        {
            foreach (var query in batchStatement.Queries)
            {
                var boundStatement = query as BoundStatement;
                if (boundStatement != null)
                {
                    var plan = NewQueryPlanImpl(keyspace, boundStatement);
                    if (plan != null)
                        return plan;
                }
            }
            return null;
        }

        private IEnumerable<Host> NewQueryPlanImpl(string keyspace, BoundStatement boundStatement)
        {
            var pstmt = boundStatement.PreparedStatement;
            var query = pstmt.Cql;
            var variables = pstmt.Variables;

            // Look up the hosts for the partition key. Skip statements that do not have bind variables.
            if (variables.Columns.Length == 0)
            {
                return null;
            }
            int key = GetKey(_cluster, boundStatement);
            if (key < 0)
            {
                return null;
            }

            var fullTableName = variables.Keyspace + "." + variables.Columns[0].Table;
            TableSplitMetadata tableSplitMetadata;
            if (!_cluster.Metadata.TableSplitMetadata.TryGetValue(fullTableName, out tableSplitMetadata))
            {
                return null;
            }

            var hosts = Enumerable.ToArray(tableSplitMetadata.GetHosts(key));
            var consistencyLevel = boundStatement.ConsistencyLevel ?? _cluster.Configuration.QueryOptions.GetConsistencyLevel();
            if (consistencyLevel == ConsistencyLevel.YBConsistentPrefix)
            {
                Shuffle(hosts);
            }
            var strongConsistency = consistencyLevel.IsStrong();
            return IterateUpHosts(keyspace, hosts, strongConsistency);
        }

        private IEnumerable<Host> IterateUpHosts(string keyspace, Host[] hosts, bool strongConsistency)
        {
            foreach (var host in hosts)
            {
                if (host.IsUp && (strongConsistency || _childPolicy.Distance(host) == HostDistance.Local))
                {
                    yield return host;
                }
            }

            foreach (var host in _childPolicy.NewQueryPlan(keyspace, null))
            {
                if (Array.IndexOf(hosts, host) == -1 && (strongConsistency || _childPolicy.Distance(host) == HostDistance.Local))
                {
                    yield return host;
                }
            }
        }

        private static void Shuffle<T>(T[] array) {
            var rng = new Random();
            int n = array.Length;
            while (n > 1)
            {
                int k = rng.Next(n--);
                T temp = array[n];
                array[n] = array[k];
                array[k] = temp;
            }
        }

        public static int GetKey(ICluster cluster, BoundStatement boundStatement)
        {
            var serializer = ((Cluster)cluster).Metadata.ControlConnection.Serializer.GetCurrentSerializer();
            PreparedStatement pstmt = boundStatement.PreparedStatement;
            var hashIndexes = pstmt.RoutingIndexes;

            if (hashIndexes == null || hashIndexes.Length == 0)
            {
                return -1;
            }

            try
            {
                // Compute the hash key bytes, i.e. <h1><h2>...<h...>.
                var variables = pstmt.Variables;
                var values = boundStatement.QueryValues;
                using (var stream = new MemoryStream())
                using (var writer = new BinaryWriter(stream))
                {
                    foreach (var index in hashIndexes)
                    {
                        var type = variables.Columns[index].TypeCode;
                        var info = variables.Columns[index].TypeInfo;
                        var bytes = serializer.Serialize(values[index]);
                        WriteTypedValue(type, info, bytes, 0, bytes.Length, writer);
                    }
                    return BytesToKey(stream.ToArray());
                }
            } catch (InvalidCastException exc)
            {
                Trace.TraceError("Failure during hash computation: {0}", exc);
                // We don't support cases when type of bound value does not match column type.
                return -1;
            }
        }

        public static long YBHashCode(ICluster cluster, BoundStatement boundStatement)
        {
            var hash = GetKey(cluster, boundStatement);
            if (hash == -1)
            {
                return -1;
            }
            return (long)(hash ^ 0x8000) << 48;
        }

        private static int BytesToKey(byte[] bytes)
        {
            ulong Seed = 97;
            ulong h = Jenkins.Hash64(bytes, Seed);
            ulong h1 = h >> 48;
            ulong h2 = 3 * (h >> 32);
            ulong h3 = 5 * (h >> 16);
            ulong h4 = 7 * (h & 0xffff);
            int result = (int)((h1 ^ h2 ^ h3 ^ h4) & 0xffff);
            Trace.TraceInformation("Bytes to key {0}: {1}", BitConverter.ToString(bytes).Replace("-", ""), result);
            return result;
        }

        public static string ByteArrayToString(byte[] ba)
        {
            return BitConverter.ToString(ba).Replace("-", "");
        }

        private static void WriteTypedValue(
            ColumnTypeCode type, IColumnInfo columnInfo, 
            byte[] bytes, int index, int count, BinaryWriter writer)
        {
            switch (type)
            {
                case ColumnTypeCode.Boolean:
                case ColumnTypeCode.TinyInt:
                case ColumnTypeCode.SmallInt:
                case ColumnTypeCode.Int:
                case ColumnTypeCode.Bigint:
                case ColumnTypeCode.Ascii:
                case ColumnTypeCode.Text:
                case ColumnTypeCode.Json:
                case ColumnTypeCode.Varchar:
                case ColumnTypeCode.Blob:
                case ColumnTypeCode.Inet:
                case ColumnTypeCode.Uuid:
                case ColumnTypeCode.Timeuuid:
                case ColumnTypeCode.Date:
                case ColumnTypeCode.Time:
                    writer.Write(bytes, index, count);
                    break;
                case ColumnTypeCode.Float:
                    var copy = new byte[4];
                    Array.Copy(bytes, index, copy, 0, count);
                    Array.Reverse(copy);
                    var floatValue = BitConverter.ToSingle(copy, 0);
                    if (float.IsNaN(floatValue))
                    {
                        writer.Write(0xc07f);
                    }
                    else
                    {
                        writer.Write(bytes, index, count);
                    }
                    break;
                case ColumnTypeCode.Double:
                    var doubleValue = BitConverter.Int64BitsToDouble(IPAddress.NetworkToHostOrder(BitConverter.ToInt64(bytes, index)));
                    if (double.IsNaN(doubleValue))
                    {
                        writer.Write((long)0xf87f);
                    } else
                    {
                        writer.Write(bytes, index, count);
                    }
                    break;
                case ColumnTypeCode.Timestamp:
                    var v = IPAddress.NetworkToHostOrder(BitConverter.ToInt64(bytes, index)) * 1000;
                    writer.Write(IPAddress.HostToNetworkOrder(v));
                    break;
                case ColumnTypeCode.List:
                case ColumnTypeCode.Set:
                    var childColumnDesc = ((ICollectionColumnInfo)columnInfo).GetChildType();
                    var length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, index));
                    index += 4;
                    for (var i = 0; i != length; ++i)
                    {
                        var size = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, index));
                        index += 4;
                        WriteTypedValue(childColumnDesc.TypeCode, childColumnDesc.TypeInfo, bytes, index, size, writer);
                        index += size;
                    }
                    break;
                case ColumnTypeCode.Map:
                    var mapColumnInfo = (MapColumnInfo)columnInfo;
                    var mapLength = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, index));
                    index += 4;
                    for (var i = 0; i != mapLength; ++i)
                    {
                        var size = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, index));
                        index += 4;
                        WriteTypedValue(mapColumnInfo.KeyTypeCode, mapColumnInfo.KeyTypeInfo, bytes, index, size, writer);
                        index += size;

                        size = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, index));
                        index += 4;
                        WriteTypedValue(mapColumnInfo.ValueTypeCode, mapColumnInfo.ValueTypeInfo, bytes, index, size, writer);
                        index += size;
                    }
                    break;
                case ColumnTypeCode.Udt:
                    var udtColumnInfo = (UdtColumnInfo)columnInfo;
                    var end = index + count;
                    for (var fieldIndex = 0; index < end; ++fieldIndex)
                    {
                        var size = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(bytes, index));
                        index += 4;
                        WriteTypedValue(udtColumnInfo.Fields[fieldIndex].TypeCode, udtColumnInfo.Fields[fieldIndex].TypeInfo, bytes, index, size, writer);
                        index += size;
                    }
                    break;
                case ColumnTypeCode.Counter:
                case ColumnTypeCode.Custom:
                case ColumnTypeCode.Decimal:
                case ColumnTypeCode.Tuple:
                case ColumnTypeCode.Varint:
                    throw new InvalidTypeException("Datatype " + type.ToString() + " not supported in a partition key column");
                default:
                    throw new InvalidTypeException("Unknown datatype " + type.ToString() + " for a partition key column");
            }
        }
        public bool RequiresPartitionMap
        {
            get
            {
                return true;
            }
        }

        public bool RequiresTokenMap
        {
            get
            {
                return _childPolicy.RequiresTokenMap;
            }
        }
    }
}
