#pragma once

#include <AggregateFunctions/Helpers.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <Common/LRUCache.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Storages/DeltaMerge/Index/RSIndex.h>
#include <Storages/DeltaMerge/Index/RSResult.h>

namespace DB
{
namespace DM
{
class CMapIndex;
using CMapIndexPtr = std::shared_ptr<CMapIndex>;
class CMapIndex : public RSIndex
{
public:
    inline constexpr static const char * CMAP_INDEX_FILE_SUFFIX = ".cmap_idx";

    void addPack(const IDataType & data_type, const IColumn & column, const ColumnVector<UInt8> * del_mark);

    void write(const IDataType & data_type, WriteBuffer & buf);

    static CMapIndexPtr read(const IDataType & type, ReadBuffer & buf, size_t bytes_limit);

    RSResult checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type);

    RSResult checkGreater(size_t pack_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/);
    RSResult checkGreaterEqual(size_t pack_id, const Field & value, const DataTypePtr & type, int /*nan_direction_hint*/);
    String getIndexFileName(const String & file_name_base)
    {
        return file_name_base + CMAP_INDEX_FILE_SUFFIX;
    }
    String getIndexNameSuffix()
    {
        return CMAP_INDEX_FILE_SUFFIX;
    }
    CMapIndex()
    {
    }
    ~CMapIndex()
    {
    }
    size_t byteSize() const
    {
        return cmap_buffers.size() * 64 * 32 * 2;
    }

private:
    using CmapBufferPtr = std::shared_ptr<PaddedPODArray<UInt8>>;
    std::vector<CmapBufferPtr> cmap_buffers;
    CMapIndex(std::vector<CmapBufferPtr> cmap_buffers)
        : cmap_buffers(cmap_buffers)
    {}
    static void set(UInt8 * offset, UInt8 charVal, int pos);
    static int indexOffsetBySize(int valueSize);
    void _putValue(UInt8 * packAddr, String value);
    static bool isSet(UInt8 * offset, UInt8 charVal, int pos);
    RSResult isValue(UInt8 * packAddr, String value);
    static std::vector<int> INDEX_OFFSET;
    RSResult checkNullableEqual(size_t pack_index, const Field & value, const DataTypePtr & type);
};
} // namespace DM
} // namespace DB
