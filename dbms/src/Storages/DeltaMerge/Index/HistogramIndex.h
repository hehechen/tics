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

#include "MinMaxIndex.h"

namespace DB
{
namespace DM
{
class HistogramIndex;
using HistogramIndexPtr = std::shared_ptr<HistogramIndex>;

class HistogramIndex : public RSIndex
{
public:
    explicit HistogramIndex(const DataTypePtr & type)
        : type(type)
    {
        minmax_index_ptr = std::make_shared<MinMaxIndex>(*type);
    }
    void addPack(const IDataType & dataType, const IColumn & column, const ColumnVector<UInt8> * del_mark);

    void write(const IDataType & data_type, WriteBuffer & buf);

    static HistogramIndexPtr read(const DataTypePtr & type, ReadBuffer & buf, size_t bytes_limit);

    RSResult checkEqual(size_t pack_index, const Field & value, const DataTypePtr & dataType);
    RSResult checkGreater(size_t pack_id, const Field & value, const DataTypePtr & dataType, int nan_direction_hint = 0);

    RSResult checkGreaterEqual(size_t pack_id, const Field & value, const DataTypePtr & dataType, int nan_direction_hint = 0);

    inline constexpr static const char * HISTOGRAM_INDEX_FILE_SUFFIX = ".histogram_idx";
    String getIndexFileName(const String & file_name_base)
    {
        return file_name_base + HISTOGRAM_INDEX_FILE_SUFFIX;
    }
    String getIndexNameSuffix()
    {
        return HISTOGRAM_INDEX_FILE_SUFFIX;
    }
    ~HistogramIndex() {}
    size_t byteSize() const
    {
        return 0;
    }
    std::pair<Int64, Int64> getIntMinMax(size_t pack_index)
    {
        return minmax_index_ptr->getIntMinMax(pack_index);
    }

    std::pair<UInt64, UInt64> getUInt64MinMax(size_t pack_index)
    {
        return minmax_index_ptr->getUInt64MinMax(pack_index);
    }

    DataTypePtr type;
    using HistogramBufferPtr = std::shared_ptr<PaddedPODArray<UInt8>>;
    std::vector<HistogramBufferPtr> histogram_buffers;
    static std::vector<int> INDEX_OFFSET;
    void putValue(UInt8 * packAddr, Int64 value, Int64 min, Int64 max);
    void putHash(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float);
    void putHistogram(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float);
    static bool intervalTooLarge(Int64 min, Int64 max);
    bool exactMode(Int64 min, Int64 max);
    RSResult checkHash(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float);
    RSResult checkHistogram(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float);
    RSResult isValue(UInt8 * packAddr, Int64 value, Int64 min, Int64 max);

    MinMaxIndexPtr minmax_index_ptr;

    HistogramIndex(DataTypePtr datatypePtr, std::vector<HistogramBufferPtr> histogram_buffers, MinMaxIndexPtr minmax_index_ptr)
        : type(datatypePtr)
        , histogram_buffers(histogram_buffers)
        , minmax_index_ptr(minmax_index_ptr)
    {}
    template <Field::Types::Which ValueFieldType, typename GetType>
    bool checkNumberValue(UInt8 * packAddr, const Field & value, Int64 min, Int64 max, RSResult & result);
    RSResult checkValue(UInt8 * packAddr, const Field & value, Int64 min, Int64 max);
    bool isFieldArighmetic(const Field & value);
    RSResult checkNullableEqual(size_t pack_index, const Field & value, const IDataType * raw_type);
};

} // namespace DM
} // namespace DB
