//
// Created by tongli chen on 2022/3/3.
//

#include "CMapIndex.h"
namespace DB
{
namespace DM
{

std::vector<int> CMapIndex::INDEX_OFFSET = {0, 1, 2, 4, 8, 16, 32, 64};

void CMapIndex::set(UInt8 * offset, UInt8 charVal, int pos) {
    int charUnsigned = charVal & 0xFF;
    UInt8 * addr = offset + (pos * 32) + charUnsigned / 8;
    *addr = (*addr | (1 << (charUnsigned & 0x03)));
}

static int MAX_POSISTIONS = 64;

int CMapIndex::indexOffsetBySize(int valueSize) {
    if (valueSize > MAX_POSISTIONS) {
        return INDEX_OFFSET[MAX_POSISTIONS];
    }
    return INDEX_OFFSET[valueSize];
}

void CMapIndex::_putValue(UInt8 * packAddr, String && value) {
    int valueSize = value.size();

    UInt8 * offset = packAddr + indexOffsetBySize(valueSize);
    int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

    if (checkSize == 0) {
        // mark empty string exists.
        set(offset, 1, 0);
    } else {
        for (int pos = 0; pos < checkSize; pos++) {
            set(offset, value[pos], pos);
        }
    }
}

void CMapIndex::addPack(const DB::IColumn & column, const DB::ColumnVector<UInt8> * del_mark)
{
    const auto * del_mark_data = (!del_mark) ? nullptr : &(del_mark->getData());
    for (size_t i = 0 ; i < column.size(); i++) {
        if (!del_mark_data || !(*del_mark_data)[i])
        {
            _putValue(cmap_buffer->data(), column.getDataAt(i).toString());
        }
    }
}

void CMapIndex::write(DB::WriteBuffer & buf)
{
    buf.write(reinterpret_cast<const char *>(cmap_buffer->data()), 64*32*2);
}

bool CMapIndex::isSet(UInt8 * offset, UInt8 charVal, int pos) {
    int charUnsigned = charVal & 0xFF;
    UInt8 * addr = offset + (pos * 32) + charUnsigned / 8;
    return (((*addr) >> (charUnsigned & 0x03)) & 1) == 1;
}

RSResult CMapIndex::isValue(UInt8 * packAddr, String && value) {
    int valueSize = value.size();

    UInt8 * offset = packAddr + indexOffsetBySize(valueSize);
    int checkSize = valueSize < MAX_POSISTIONS ? valueSize : MAX_POSISTIONS;

    if (checkSize == 0) {
        if (!isSet(offset, 1, 0))
        {
            return RSResult::None;
        }
    } else {
        for (int pos = 0; pos < checkSize; pos++) {
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
    auto buffer = std::make_shared<PaddedPODArray<UInt8>>(64*32*2);
    buf.read(reinterpret_cast<char *>(buffer->data()), 64*32*2);
    return CMapIndexPtr(new CMapIndex(buffer));
}

RSResult CMapIndex::checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type)
{
    if (value.getType() != Field::Types::String)
    {
        return RSResult::Unknown;
    }
    return isValue(cmap_buffer->data(), value.toString());
}

}
}
