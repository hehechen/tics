//
// Created by tongli chen on 2022/3/10.
//

#include "RoughIndexManager.h"
void DB::DM::RoughIndexManager::addPack(const DB::IColumn & column, const DB::ColumnVector<UInt8> * del_mark)
{

}

void DB::DM::RoughIndexManager::write(const DB::IDataType & type, DB::WriteBuffer & buf)
{
}
DB::DM::RSResult DB::DM::RoughIndexManager::checkEqual(size_t pack_index, const DB::Field & value, const DB::DataTypePtr & type)
{
    return DB::DM::RSResult::Unknown;
}
DB::DM::RSResult DB::DM::RoughIndexManager::checkGreater(size_t pack_index, const DB::Field & value, const DB::DataTypePtr & type, int nan_direction)
{
    return DB::DM::RSResult::Unknown;
}
DB::DM::RSResult DB::DM::RoughIndexManager::checkGreaterEqual(size_t pack_index, const DB::Field & value, const DB::DataTypePtr & type, int nan_direction)
{
    return DB::DM::RSResult::Unknown;
}
