//
// Created by tongli chen on 2022/3/3.
//

#include "CMapIndex.h"

#include <boost/algorithm/string/trim.hpp>
namespace DB
{
namespace DM
{
std::vector<int> CMapIndex::INDEX_OFFSET(63, 0);

void CMapIndex::set(UInt8 * offset, UInt8 charVal, int pos)
{
    int charUnsigned = charVal & 0xFF;
    UInt8 * addr = offset + (pos * 32) + charUnsigned / 8;
    *addr = (*addr | (1 << (charUnsigned % 8)));
}

static int MAX_POSISTIONS = 64;

int ceil_log2(unsigned long long x)
{
    static const unsigned long long t[6] = {
        0xFFFFFFFF00000000ull,
        0x00000000FFFF0000ull,
        0x000000000000FF00ull,
        0x00000000000000F0ull,
        0x000000000000000Cull,
        0x0000000000000002ull};

    int y = (((x & (x - 1)) == 0) ? 0 : 1);
    int j = 32;
    int i;

    for (i = 0; i < 6; i++)
    {
        int k = (((x & t[i]) == 0) ? 0 : j);
        y += k;
        x >>= k;
        j >>= 1;
    }

    return y;
}

int CMapIndex::indexOffsetBySize(int valueSize)
{
    if (valueSize == 0)
    {
        return 0;
    }
    return 1 << ceil_log2(valueSize);
}

void CMapIndex::_putValue(UInt8 * packAddr, String value)
{
    boost::trim_right(value);
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
                _putValue(cmap_buffer->data(), column_nullable.getNestedColumn().getDataAt(i).toString());
            }
            else
            {
                _putValue(cmap_buffer->data(), column.getDataAt(i).toString());
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

} // namespace DM
} // namespace DB
