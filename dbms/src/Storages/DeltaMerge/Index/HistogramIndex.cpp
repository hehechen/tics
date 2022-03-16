#include "HistogramIndex.h"

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeMyDate.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>

#include <functional>

#include "RoughCheck.h"

namespace DB
{
namespace DM
{
void DB::DM::HistogramIndex::write(const DB::IDataType & data_type, DB::WriteBuffer & buf)
{
    minmax_index_ptr->write(data_type, buf);
    for (const auto & histogram_buf : histogram_buffers)
    {
        buf.write(reinterpret_cast<const char *>(histogram_buf->data()), 128 * 2);
    }
}

DB::DM::HistogramIndexPtr DB::DM::HistogramIndex::read(const DataTypePtr & type, DB::ReadBuffer & buf, size_t bytes_limit)
{
    auto minmax_index = MinMaxIndex::read(*type, buf, bytes_limit, false);
    std::vector<HistogramBufferPtr> histograms;
    while (!buf.eof())
    {
        auto buffer = std::make_shared<PaddedPODArray<UInt8>>(128 * 2);
        buf.read(reinterpret_cast<char *>(buffer->data()), 128 * 2);
        histograms.push_back(buffer);
    }
    return HistogramIndexPtr(new HistogramIndex(type, histograms, minmax_index));
}


bool HistogramIndex::intervalTooLarge(Int64 min, Int64 max)
{ // about 2^62
    return min < -4611686018427387900L && max > 4611686018427387900L;
}

RSResult HistogramIndex::checkHash(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float)
{
    if (intervalTooLarge(min, max))
    {
        return RSResult::Some;
    }
    Int64 interval_value;
    if (is_float)
    {
        Float64 double_value = *(double *)(&value);
        Float64 double_min = *(double *)(&min);
        interval_value = std::hash<Float64>{}(double_value - double_min);
    }
    else
    {
        interval_value = value - min;
    }
    Int32 bit = interval_value & 0x03FF;
    return (((packAddr[bit / 8]) >> (bit & 0x03)) & 1) != 0 ? RSResult::Some : RSResult::None;
}

void HistogramIndex::putHash(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float)
{
    if (intervalTooLarge(min, max))
    {
        return;
    }
    Int64 interval_value;
    if (is_float)
    {
        Float64 double_value = *(double *)(&value);
        Float64 double_min = *(double *)(&min);
        interval_value = std::hash<Float64>{}(double_value - double_min);
    }
    else
    {
        interval_value = value - min;
    }
    Int32 bit = interval_value & 0x03FF;
    UInt8 old_value = packAddr[bit / 8];
    packAddr[bit / 8] = old_value | (1 << (bit & 0x03));
}

bool HistogramIndex::exactMode(Int64 min, Int64 max)
{
    return (max - min) <= 1022;
}

RSResult HistogramIndex::checkHistogram(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float)
{
    if (intervalTooLarge(min, max))
    {
        return RSResult::Some;
    }
    Int32 bit;
    if (is_float)
    {
        Float64 double_value = *reinterpret_cast<double *>(&value);
        Float64 double_min = *reinterpret_cast<double *>(&min);
        Float64 double_max = *reinterpret_cast<double *>(&max);
        if (double_value == double_min || value == double_max)
        {
            return RSResult::All;
        }
        if (double_value > double_max || double_value < double_min)
        {
            return RSResult::None;
        }
        Float64 interval_len = (max - min) / Float64(1024);
        bit = static_cast<Int32>((double_value - double_min) / interval_len);
    }
    else
    {
        if (value == min || value == max)
        {
            return RSResult::All;
        }
        if (value > max || value < min)
        {
            return RSResult::None;
        }
        if (exactMode(min, max))
        {
            bit = static_cast<Int32>(value - min - 1);
        }
        else
        {
            Float64 interval_len = (max - min) / static_cast<Float64>(1024);
            bit = static_cast<Int32>((value - min - 1) / interval_len);
        }
    }
    if ((((*reinterpret_cast<Int32 *>(packAddr + bit / 32 * 4)) >> ((bit & 0x03FF))) & 1) != 0)
    {
        return RSResult::Some;
    }
    return RSResult::None;
}

void HistogramIndex::putHistogram(UInt8 * packAddr, Int64 value, Int64 min, Int64 max, bool is_float)
{
    if (intervalTooLarge(min, max))
    {
        return;
    }
    Int32 bit;
    if (is_float)
    {
        Float64 double_value = *reinterpret_cast<double *>(&value);
        Float64 double_min = *reinterpret_cast<double *>(&min);
        Float64 double_max = *reinterpret_cast<double *>(&max);
        if (double_value == double_min || value == double_max)
        {
            return;
        }
        Float64 interval_len = (max - min) / Float64(1024);
        bit = static_cast<Int32>((double_value - double_min) / interval_len);
    }
    else
    {
        if (value == min || value == max)
        {
            return;
        }
        if (exactMode(min, max))
        {
            bit = static_cast<Int32>(value - min - 1);
        }
        else
        {
            Float64 interval_len = (max - min) / static_cast<Float64>(1024);
            bit = static_cast<Int32>((value - min - 1) / interval_len);
        }
    }
    if (bit > 1024)
    {
        return;
    }
    Int32 old_value = *reinterpret_cast<Int32 *>(packAddr + bit / 32 * 4);
    *reinterpret_cast<Int32 *>(packAddr + bit / 32 * 4) = old_value | (1 << (bit & 0x03FF));
}

RSResult HistogramIndex::isValue(UInt8 * packAddr, Int64 value, Int64 min, Int64 max)
{
    RSResult res = checkHash(packAddr, value, min, max, type->isFloatingPoint());
    if (res == RSResult::None)
    {
        return RSResult::None;
    }
    return checkHistogram(packAddr + 128, value, min, max, type->isFloatingPoint());
}

void HistogramIndex::putValue(UInt8 * packAddr, Int64 value, Int64 min, Int64 max)
{
    putHash(packAddr, value, min, max, type->isFloatingPoint());
    putHistogram(packAddr + 128, value, min, max, type->isFloatingPoint());
}

void HistogramIndex::addPack(const IDataType & dataType, const DB::IColumn & column, const DB::ColumnVector<UInt8> * del_mark)
{
    auto histogram_index_buffer = std::make_shared<PaddedPODArray<UInt8>>(256, 0);
    minmax_index_ptr->addPack(dataType, column, del_mark);

    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());

    size_t size = minmax_index_ptr->getNullMark()->size();
    Int64 min_value = minmax_index_ptr->getMinMaxes()->getInt(2 * (size - 1));
    Int64 max_value = minmax_index_ptr->getMinMaxes()->getInt(2 * (size - 1) + 1);

    for (size_t i = 0; i < column.size(); i++)
    {
        if (!del_mark_data || !(*del_mark_data)[i])
        {
            putValue(histogram_index_buffer->data(), column.getInt(i), min_value, max_value);
        }
    }
    histogram_buffers.push_back(histogram_index_buffer);
}

bool HistogramIndex::isFieldArighmetic(const Field & value)
{
    switch (value.getType())
    {
    case Field::Types::UInt64:
    case Field::Types::Int64:
    case Field::Types::Float64:
    case Field::Types::Int128:
        return true;
    default:
        return false;
    }
}

template <Field::Types::Which ValueFieldType, typename GetType>
bool HistogramIndex::checkNumberValue(UInt8 * packAddr, const Field & value, Int64 min, Int64 max, RSResult & result)
{
    if (ValueFieldType != value.getType())
    {
        return false;
    }

    if (GreaterOp<GetType, Int64>::apply(value.safeGet<GetType>(), INT64_MAX) || LessOp<GetType, Int64>::apply(value.safeGet<GetType>(), INT64_MIN))
    {
        result = RSResult::Some;
        return true;
    }

    result = isValue(packAddr, static_cast<Int64>(value.safeGet<GetType>()), min, max);
    return true;
}

RSResult HistogramIndex::checkValue(UInt8 * packAddr, const Field & value, Int64 min, Int64 max)
{
    if (!isFieldArighmetic(value))
    {
        return RSResult::Some;
    }
    RSResult result;
    if (!(checkNumberValue<Field::Types::Which::UInt64, UInt64>(packAddr, value, min, max, result)
          || checkNumberValue<Field::Types::Which::Int64, Int64>(packAddr, value, min, max, result)
          || checkNumberValue<Field::Types::Which::Float64, Float64>(packAddr, value, min, max, result)
          || checkNumberValue<Field::Types::Which::Int128, Int128>(packAddr, value, min, max, result)))
    {
        throw Exception("Illegal check value " + std::string(value.getTypeName()));
    }
    return result;
}

RSResult HistogramIndex::checkNullableEqual(size_t pack_index, const Field & value, const IDataType * raw_type)
{
    const ColumnNullable & column_nullable = static_cast<const ColumnNullable &>(*minmax_index_ptr->getMinMaxes());
#define DISPATCH(TYPE)                                                                      \
    if (typeid_cast<const DataType##TYPE *>(raw_type))                                      \
    {                                                                                       \
        auto & minmaxes_data = toColumnVectorData<TYPE>(column_nullable.getNestedColumn()); \
        auto min = minmaxes_data[pack_index * 2];                                           \
        auto max = minmaxes_data[pack_index * 2 + 1];                                       \
        return checkValue(histogram_buffers[pack_index]->data(), value, min, max);          \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        if (value.getType() != Field::Types::String)
        {
            return RSResult::Some;
        }
        auto & minmaxes_data = toColumnVectorData<DataTypeDate::FieldType>(column_nullable.getNestedColumn());
        auto min = minmaxes_data[pack_index * 2];
        auto max = minmaxes_data[pack_index * 2 + 1];
        DayNum date;
        ReadBufferFromMemory in(value.safeGet<std::string>().data(), value.safeGet<std::string>().size());
        readDateText(date, in);
        return isValue(histogram_buffers[pack_index]->data(), date, min, max);
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        if (value.getType() != Field::Types::String)
        {
            return RSResult::Some;
        }
        auto & minmaxes_data = toColumnVectorData<DataTypeDateTime::FieldType>(column_nullable.getNestedColumn());
        auto min = minmaxes_data[pack_index * 2];
        auto max = minmaxes_data[pack_index * 2 + 1];
        time_t date_time;
        ReadBufferFromMemory in(value.safeGet<std::string>().data(), value.safeGet<std::string>().size());
        readDateTimeText(date_time, in);
        if (!in.eof())
            throw Exception("String is too long for DateTime: " + value.safeGet<std::string>());
        return isValue(histogram_buffers[pack_index]->data(), date_time, min, max);
    }
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        auto & minmaxes_data = toColumnVectorData<DataTypeMyTimeBase::FieldType>(column_nullable.getNestedColumn());
        auto min = minmaxes_data[pack_index * 2];
        auto max = minmaxes_data[pack_index * 2 + 1];
        return isValue(histogram_buffers[pack_index]->data(), value.safeGet<DataTypeMyTimeBase::FieldType>(), min, max);
    }
    return RSResult::Some;
}

RSResult HistogramIndex::checkEqual(size_t pack_index, const Field & value, const DataTypePtr & dataType)
{
    if (!(*minmax_index_ptr->getValueMark())[pack_index])
        return RSResult::None;

    const auto * raw_type = dataType.get();
    if (typeid_cast<const DataTypeNullable *>(raw_type))
    {
        raw_type = dataType->getNestedDataType().get();
        return checkNullableEqual(pack_index, value, raw_type);
    }
#define DISPATCH(TYPE)                                                                    \
    if (typeid_cast<const DataType##TYPE *>(raw_type))                                    \
    {                                                                                     \
        auto & minmaxes_data = toColumnVectorData<TYPE>(minmax_index_ptr->getMinMaxes()); \
        auto min = minmaxes_data[pack_index * 2];                                         \
        auto max = minmaxes_data[pack_index * 2 + 1];                                     \
        return checkValue(histogram_buffers[pack_index]->data(), value, min, max);        \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        if (value.getType() != Field::Types::String)
        {
            return RSResult::Some;
        }
        auto & minmaxes_data = toColumnVectorData<DataTypeDate::FieldType>(minmax_index_ptr->getMinMaxes());
        auto min = minmaxes_data[pack_index * 2];
        auto max = minmaxes_data[pack_index * 2 + 1];
        DayNum date;
        ReadBufferFromMemory in(value.safeGet<std::string>().data(), value.safeGet<std::string>().size());
        readDateText(date, in);
        return isValue(histogram_buffers[pack_index]->data(), date, min, max);
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        if (value.getType() != Field::Types::String)
        {
            return RSResult::Some;
        }
        auto & minmaxes_data = toColumnVectorData<DataTypeDateTime::FieldType>(minmax_index_ptr->getMinMaxes());
        auto min = minmaxes_data[pack_index * 2];
        auto max = minmaxes_data[pack_index * 2 + 1];
        time_t date_time;
        ReadBufferFromMemory in(value.safeGet<std::string>().data(), value.safeGet<std::string>().size());
        readDateTimeText(date_time, in);
        if (!in.eof())
            throw Exception("String is too long for DateTime: " + value.safeGet<std::string>());
        return isValue(histogram_buffers[pack_index]->data(), date_time, min, max);
    }
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        // For DataTypeMyDateTime / DataTypeMyDate, simply compare them as comparing UInt64 is OK.
        // Check `struct MyTimeBase` for more details.
        auto & minmaxes_data = toColumnVectorData<DataTypeMyTimeBase::FieldType>(minmax_index_ptr->getMinMaxes());
        auto min = minmaxes_data[pack_index * 2];
        auto max = minmaxes_data[pack_index * 2 + 1];
        return isValue(histogram_buffers[pack_index]->data(), value.safeGet<DataTypeMyTimeBase::FieldType>(), min, max);
    }
    return RSResult::Some;
}

RSResult HistogramIndex::checkGreater(size_t pack_id, const Field & value, const DataTypePtr & dataType, int nan_direction_hint)
{
    return minmax_index_ptr->checkGreater(pack_id, value, dataType, nan_direction_hint);
}

RSResult HistogramIndex::checkGreaterEqual(size_t pack_id, const Field & value, const DataTypePtr & dataType, int nan_direction_hint)
{
    return minmax_index_ptr->checkGreaterEqual(pack_id, value, dataType, nan_direction_hint);
}

} // namespace DM
} // namespace DB
