#include "RSIndexManager.h"

namespace DB
{
namespace DM
{
std::pair<Int64, Int64> RSIndexManager::getIntMinMax(size_t pack_index)
{
    auto minmax_ptr = std::dynamic_pointer_cast<MinMaxIndex>(rs_index);
    return minmax_ptr->getIntMinMax(pack_index);
}

std::pair<StringRef, StringRef> RSIndexManager::getStringMinMax(size_t pack_index)
{
    auto minmax_ptr = std::dynamic_pointer_cast<MinMaxIndex>(rs_index);
    return minmax_ptr->getStringMinMax(pack_index);
}

std::pair<UInt64, UInt64> RSIndexManager::getUInt64MinMax(size_t pack_index)
{
    auto minmax_ptr = std::dynamic_pointer_cast<MinMaxIndex>(rs_index);
    return minmax_ptr->getUInt64MinMax(pack_index);
}
} // namespace DM
} // namespace DB
