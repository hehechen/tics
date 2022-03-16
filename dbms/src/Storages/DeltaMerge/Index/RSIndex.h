// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <AggregateFunctions/Helpers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Common/LRUCache.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/Index/RSResult.h>
namespace DB
{
namespace DM
{
class RSIndex;
using RSIndexPtr = std::shared_ptr<RSIndex>;
class RSIndex
{
public:
    virtual void addPack(const IDataType & data_type, const DB::IColumn & column, const DB::ColumnVector<UInt8> * del_mark) = 0;
    virtual void write(const IDataType & type, WriteBuffer & buf) = 0;
    virtual RSResult checkEqual(size_t pack_id, const Field & value, const DataTypePtr & type) = 0;
    virtual RSResult checkGreater(size_t pack_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/) = 0;
    virtual RSResult checkGreaterEqual(size_t pack_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/) = 0;
    virtual String getIndexNameSuffix() = 0;
    virtual size_t byteSize() const = 0;
    virtual ~RSIndex(){};

private:
    DataTypePtr type;
};

} // namespace DM

} // namespace DB