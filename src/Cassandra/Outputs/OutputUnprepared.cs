//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

namespace Cassandra
{
    internal class OutputUnprepared : OutputError
    {
        private readonly PreparedQueryNotFoundInfo _info = new PreparedQueryNotFoundInfo();

        protected override void Load(FrameReader cb)
        {
            short len = cb.ReadInt16();
            _info.UnknownId = new byte[len];
            cb.Read(_info.UnknownId, 0, len);
        }

        public override DriverException CreateException()
        {
            return new PreparedQueryNotFoundException(Message, _info.UnknownId);
        }
    }
}