#pragma once

#include <DataTypes/IDataType.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <Poco/File.h>

#include <functional>

#include "CMapIndex.h"
#include "HistogramIndex.h"
#include "MinMaxIndex.h"
namespace DB
{
namespace DM
{
class RSIndexManager;
using RSIndexManagerPtr = std::shared_ptr<RSIndexManager>;

class RSIndexManager
{
public:
    explicit RSIndexManager(DataTypePtr type, bool is_extra_column, bool is_version_column)
        : type(type)
    {
        if (is_extra_column || is_version_column)
        {
            rs_index = std::make_shared<MinMaxIndex>(*type);
        }
        else if (supportedStringType(type) || (type->isNullable() && supportedStringType(type->getNestedDataType())))
        {
            rs_index = std::make_shared<CMapIndex>();
        }
        else if (supportedNumberType(type) || (type->isNullable() && supportedNumberType(type->getNestedDataType())))
        {
            rs_index = std::make_shared<HistogramIndex>(type);
        }
    }

    static bool supportedNumberType(DataTypePtr type)
    {
        return type->isInteger() || type->isUnsignedInteger() || type->isFloatingPoint() || type->isDateOrDateTime() || type->isMyDateOrMyDateTime();
    }

    static bool supportedStringType(DataTypePtr type)
    {
        return type->isStringOrFixedString();
    }

    static bool supportedType(DataTypePtr type)
    {
        return supportedNumberType(type) || supportedStringType(type);
    }

    void addPack(const IDataType & data_type, const IColumn & column, const ColumnVector<UInt8> * del_mark)
    {
        rs_index->addPack(data_type, column, del_mark);
    }
    // write only support one index now
    void write(const IDataType & data_type, WriteBuffer & buf)
    {
        rs_index->write(data_type, buf);
    }
    RSResult checkEqual(size_t pack_index, const Field & value, const DataTypePtr & dataType)
    {
        return rs_index->checkEqual(pack_index, value, dataType);
    }
    RSResult checkGreater(size_t pack_index, const Field & value, const DataTypePtr & dataType, int nan_direction)
    {
        return rs_index->checkGreater(pack_index, value, dataType, nan_direction);
    }
    RSResult checkGreaterEqual(size_t pack_index, const Field & value, const DataTypePtr & dataType, int nan_direction)
    {
        return rs_index->checkGreaterEqual(pack_index, value, dataType, nan_direction);
    }
    RSResult checkLike(size_t pack_index, const Field & value, const Field & escape_char, const DataTypePtr & dataType)
    {
        return rs_index->checkLike(pack_index, value, escape_char, dataType);
    }
    String getIndexNameSuffix()
    {
        return rs_index->getIndexNameSuffix();
    }

    std::pair<Int64, Int64> getIntMinMax(size_t pack_index);

    std::pair<StringRef, StringRef> getStringMinMax(size_t pack_index);

    std::pair<UInt64, UInt64> getUInt64MinMax(size_t pack_index);

    size_t byteSize() const
    {
        return rs_index->byteSize();
    }
    DataTypePtr getType() const
    {
        return type;
    }

    using IndexBufGetter = std::function<ReadBufferFromFileProvider(String)>;

    static RSIndexManagerPtr read(const DataTypePtr & type, ReadBuffer & buf, size_t bytes_limit, const String & exist_index_file_suffix)
    {
        if (exist_index_file_suffix == MinMaxIndex::INDEX_FILE_SUFFIX)
        {
            return RSIndexManagerPtr(new RSIndexManager(type, MinMaxIndex::read(*type, buf, bytes_limit)));
        }
        if (exist_index_file_suffix == CMapIndex::CMAP_INDEX_FILE_SUFFIX)
        {
            return RSIndexManagerPtr(new RSIndexManager(type, CMapIndex::read(*type, buf, bytes_limit)));
        }
        if (exist_index_file_suffix == HistogramIndex::HISTOGRAM_INDEX_FILE_SUFFIX)
        {
            return RSIndexManagerPtr(new RSIndexManager(type, HistogramIndex::read(type, buf, bytes_limit)));
        }
        return nullptr;
    }


    static String getExistIndexFile(const String & file_path_base)
    {
        if (Poco::File(file_path_base + MinMaxIndex::INDEX_FILE_SUFFIX).exists())
        {
            return MinMaxIndex::INDEX_FILE_SUFFIX;
        }
        if (Poco::File(file_path_base + CMapIndex::CMAP_INDEX_FILE_SUFFIX).exists())
        {
            return CMapIndex::CMAP_INDEX_FILE_SUFFIX;
        }
        if (Poco::File(file_path_base + HistogramIndex::HISTOGRAM_INDEX_FILE_SUFFIX).exists())
        {
            return HistogramIndex::HISTOGRAM_INDEX_FILE_SUFFIX;
        }
        return nullptr;
    }

private:
    DataTypePtr type;
    RSIndexPtr rs_index;
    RSIndexManager(const DataTypePtr & type, RSIndexPtr index)
        : type(type)
        , rs_index(index)
    {}
};

struct RSIndexWeightFunction
{
    size_t operator()(const RSIndexManager & index) const { return index.byteSize(); }
};

class RSIndexCache : public LRUCache<String, RSIndexManager, std::hash<String>, RSIndexWeightFunction>
{
private:
    using Base = LRUCache<String, RSIndexManager, std::hash<String>, RSIndexWeightFunction>;

public:
    RSIndexCache(size_t max_size_in_bytes, const Delay & expiration_delay)
        : Base(max_size_in_bytes, expiration_delay)
    {}

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto result = Base::getOrSet(key, load);
        return result.first;
    }
};

using RSIndexCachePtr = std::shared_ptr<RSIndexCache>;

} // namespace DM
} // namespace DB
