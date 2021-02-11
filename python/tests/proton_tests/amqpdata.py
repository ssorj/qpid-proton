#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

NULL   = 0x40 # @
TRUE   = 0x41 # A
FALSE  = 0x42
UINT0  = 0x43 # C
ULONG0 = 0x44
LIST0  = 0x45 # E
UBYTE  = 0x50
UINT8  = 0x52
ULONG8 = 0x53 # S
USHORT = 0x60
VBIN8  = 0xa0
LIST8  = 0xc0
LIST32 = 0xd0

def amqp_bytes(*items):
    out = list()

    for item in items:
        if type(item) is int:
            out.append(bytes([item]))
        elif type(item) is str:
            out.append(item.encode("raw_unicode_escape"))
        elif type(item) is bytes:
            out.append(item)
        else:
            raise Exception("Unhandled item type")

    return b"".join(out)

def amqp_frame(performative, *payload_items):
    payload = amqp_bytes(*payload_items)

    if len(payload) > 255 - 11:
        raise Exception("Unhandled payload size")

    return amqp_bytes("\x00\x00\x00", len(payload) + 11, "\x02\x00\x00\x00", "\x00", ULONG8, performative, payload)
