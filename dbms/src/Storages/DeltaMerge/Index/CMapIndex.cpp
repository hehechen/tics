//
// Created by tongli chen on 2022/3/3.
//

#include "CMapIndex.h"

#include <Common/Logger.h>

#include <boost/algorithm/string/trim.hpp>
namespace DB
{
namespace DM
{
std::vector<int> CMapIndex::INDEX_OFFSET(63, 0);

void CMapIndex::set(UInt8 * offset, UInt8 charVal, int pos)
{
    int charUnsigned = charVal & 0xFF;
    UInt8 * addr = offset + (pos << 5) + (charUnsigned >> 3);
    *addr = (*addr | (1 << (charUnsigned % 8)));
}

static int MAX_POSISTIONS = 64;
static int INDEX_POS_COUNT[8] = {0, 1, 2, 4, 8, 16, 32, 64};

std::vector<int> CMapIndex::size_array = {0, 1, 2, 4, 4, 8, 8, 8, 8, 16, 16, 16, 16, 16, 16, 16, 16, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64};

int CMapIndex::indexOffsetBySize(int valueSize)
{
    if (valueSize < 0)
    {
        return 0;
    }
    if (valueSize > 64)
    {
        return 64;
    }
    return size_array[valueSize];
}

void CMapIndex::_putValue(UInt8 * packAddr, std::string_view value)
{
    int valueSize = value.size();

    UInt8 * offset = packAddr + indexOffsetBySize(valueSize);
    int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

    if (checkSize == 0)
    {
        // mark empty string exists.
        set(offset, 1, 0);
    }
    else
    {
        for (int pos = 0; pos < checkSize; pos++)
        {
            set(offset, value[pos], pos);
        }
    }
}

void CMapIndex::addPack(const IDataType & type, const DB::IColumn & column, const DB::ColumnVector<UInt8> * del_mark)
{
    auto cmap_buffer = std::make_shared<PaddedPODArray<UInt8>>(64 * 32 * 2, 0);
    cmap_buffers.push_back(cmap_buffer);
    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());
    for (size_t i = 0; i < column.size(); i++)
    {
        if (!del_mark_data || !(*del_mark_data)[i])
        {
            if (type.getTypeId() == TypeIndex::Nullable)
            {
                const ColumnNullable & column_nullable = static_cast<const ColumnNullable &>(column);
                _putValue(cmap_buffer->data(), static_cast<std::string_view>(column_nullable.getNestedColumn().getDataAt(i)));
            }
            else
            {
                _putValue(cmap_buffer->data(), static_cast<std::string_view>(column.getDataAt(i)));
            }
        }
    }
}

void CMapIndex::write(const IDataType &, DB::WriteBuffer & buf)
{
    for (auto cmap_buffer : cmap_buffers)
    {
        buf.write(reinterpret_cast<const char *>(cmap_buffer->data()), 64 * 32 * 2);
    }
}

bool CMapIndex::isSet(UInt8 * offset, UInt8 charVal, int pos)
{
    int charUnsigned = charVal & 0xFF;
    UInt8 * addr = offset + (pos * 32) + charUnsigned / 8;
    return (((*addr) >> (charUnsigned % 8)) & 1) == 1;
}

RSResult CMapIndex::isValue(UInt8 * packAddr, String value)
{
    boost::trim_right(value);
    int valueSize = value.size();

    UInt8 * offset = packAddr + indexOffsetBySize(valueSize);
    int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

    if (checkSize == 0)
    {
        if (!isSet(offset, 1, 0))
        {
            return RSResult::None;
        }
    }
    else
    {
        for (int pos = 0; pos < checkSize; pos++)
        {
            if (!isSet(offset, value[pos], pos))
            {
                return RSResult::None;
            }
        }
    }
    return RSResult::Some;
}

CMapIndexPtr CMapIndex::read(const DB::IDataType & type, DB::ReadBuffer & buf, size_t bytes_limit)
{
    if (type.getTypeId() != TypeIndex::String && (type.getTypeId() == TypeIndex::Nullable && type.getNestedDataType()->getTypeId() != TypeIndex::String))
    {
        throw DB::TiFlashException("Bad type: " + std::to_string(static_cast<double>(type.getTypeId())),
                                   Errors::DeltaTree::Internal);
    }
    size_t buf_pos = buf.count();
    std::vector<CmapBufferPtr> cmap_indexes;
    while (!buf.eof())
    {
        auto buffer = std::make_shared<PaddedPODArray<UInt8>>(64 * 32 * 2);
        buf.read(reinterpret_cast<char *>(buffer->data()), 64 * 32 * 2);
        cmap_indexes.push_back(buffer);
    }
    size_t bytes_read = buf.count() - buf_pos;
    if (bytes_read != bytes_limit)
    {
        throw DB::TiFlashException("Bad file format: expected read index content size: " + std::to_string(bytes_limit)
                                       + " vs. actual: " + std::to_string(bytes_read),
                                   Errors::DeltaTree::Internal);
    }
    return CMapIndexPtr(new CMapIndex(cmap_indexes));
}

// type: index type
RSResult CMapIndex::checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type)
{
    if (type->getTypeId() != TypeIndex::String && (type->getTypeId() == TypeIndex::Nullable && type->getNestedDataType()->getTypeId() != TypeIndex::String))
    {
        return RSResult::Some;
    }
    if (value.getType() != Field::Types::String)
    {
        return RSResult::Some;
    }
    return isValue(cmap_buffers[pack_index]->data(), value.safeGet<String>());
}

RSResult CMapIndex::checkGreater(size_t pack_id, const Field & value, const DataTypePtr & type, int)
{
    fmt::ignore_unused(pack_id, value, type);
    return RSResult::Some;
}

RSResult CMapIndex::checkGreaterEqual(size_t pack_id, const Field & value, const DataTypePtr & type, int)
{
    fmt::ignore_unused(pack_id, value, type);
    return RSResult::Some;
}

size_t CMapIndex::getLikePatternMinLength(String & str, Int64 escape_char)
{
    size_t min_length = 0;
    bool is_escape;
    for (size_t i = 0; i < str.size(); i++)
    {
        if (str[i] == escape_char)
        {
            is_escape = true;
        }
        else if (str[i] == '_')
        {
            is_escape = false;
            min_length++;
        }
        else if (str[i] == '%')
        {
            if (is_escape)
            {
                is_escape = false;
                min_length++;
            }
        }
        else
        {
            if (is_escape)
            {
                // TiDB will push down to TiFlash the illegal like pattern, won't go into this branch, but we need to process it.
                is_escape = false;
                min_length += 2;
            }
            min_length++;
        }
    }
    return min_length;
}

RSResult CMapIndex::isLike(UInt8 * packAddr, String value, Int64 escape_char)
{
    boost::trim_right(value);
    int min_length = getLikePatternMinLength(value, escape_char);

    for (auto i : INDEX_POS_COUNT)
    {
        if (i < min_length && i != MAX_POSISTIONS)
        {
            continue;
        }
        UInt8 * offset = packAddr + indexOffsetBySize(i);
        int checkSize = static_cast<int>(value.size()) < i ? value.size() : i;
        if (checkSize == 0)
        {
            if (!isSet(offset, 1, 0))
            {
                return RSResult::None;
            }
        }
        else
        {
            int origin_index = 0;
            for (int pos = 0; pos < checkSize; pos++, origin_index++)
            {
                if (value[pos] == '%')
                {
                    return RSResult::Some;
                }
                if (value[pos] == '_')
                {
                    continue;
                }
                if (value[pos] == escape_char)
                {
                    pos++;
                    if (!isSet(offset, value[pos], origin_index))
                    {
                        return RSResult::None;
                    }
                    continue;
                }
                if (!isSet(offset, value[pos], pos))
                {
                    return RSResult::None;
                }
            }
        }
    }
    return RSResult::Some;
}

RSResult CMapIndex::checkLike(size_t pack_id, const Field & value, const Field & escape_char, const DataTypePtr & type)
{
    if (type->getTypeId() != TypeIndex::String && (type->getTypeId() == TypeIndex::Nullable && type->getNestedDataType()->getTypeId() != TypeIndex::String))
    {
        return RSResult::Some;
    }
    if (value.getType() != Field::Types::String || escape_char.getType() != Field::Types::Int64)
    {
        return RSResult::Some;
    }
    String str = value.safeGet<String>();
    return isLike(cmap_buffers[pack_id]->data(), str, escape_char.safeGet<Int64>());
}

} // namespace DM
} // namespace DB
