#pragma once

#include <DataTypes/IDataType.h>

#include "CMapIndex.h"
#include "MinMaxIndex.h"
namespace DB
{
namespace DM
{
class RoughIndexManager
{
public:
    explicit RoughIndexManager(const IDataType & type)
    {
        if (type.getName() == "String")
        {
            cmap_index = std::make_shared<CMapIndex>();
        }
        else
        {
            minmax_index = std::make_shared<MinMaxIndex>(type);
        }
    }

    void addPack(const IColumn & column, const ColumnVector<UInt8> * del_mark);
    void write(const IDataType & type, WriteBuffer & buf);
    static RoughIndexManager read(const IDataType & type, ReadBuffer & buf, size_t bytes_limit);
    RSResult checkEqual(size_t pack_index, const Field & value, const DataTypePtr & type);
    RSResult checkGreater(size_t pack_index, const Field & value, const DataTypePtr & type, int nan_direction);
    RSResult checkGreaterEqual(size_t pack_index, const Field & value, const DataTypePtr & type, int nan_direction);
private:
    CMapIndexPtr cmap_index = nullptr;
    MinMaxIndexPtr minmax_index = nullptr;
};
} // namespace DM
}

