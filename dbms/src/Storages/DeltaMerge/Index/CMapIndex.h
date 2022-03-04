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
class CMapIndex;
using CMapIndexPtr = std::shared_ptr<CMapIndex>;
class CMapIndex
{
public:
    void addPack(const IColumn & column, const ColumnVector<UInt8> * del_mark);

    void write(WriteBuffer & buf);

    static CMapIndexPtr read(const IDataType & type, ReadBuffer & buf, size_t bytes_limit);

    RSResult checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type);

    CMapIndex()
    {
        cmap_buffer =std::make_shared<PaddedPODArray<UInt8>>(32 * 64);
    }

private:
    using CmapBufferPtr = std::shared_ptr<PaddedPODArray<UInt8>>;
    CmapBufferPtr cmap_buffer;
    CMapIndex(CmapBufferPtr cmap_buffer): cmap_buffer(cmap_buffer){}
    static void set(UInt8 * offset, UInt8 charVal, int pos);
    static int indexOffsetBySize(int valueSize);
    void _putValue(UInt8 * packAddr, String && value);
    static bool isSet(UInt8 * offset, UInt8 charVal, int pos);
    RSResult isValue(UInt8 * packAddr, String && value);
    static std::vector<int> INDEX_OFFSET;
};
}
}

