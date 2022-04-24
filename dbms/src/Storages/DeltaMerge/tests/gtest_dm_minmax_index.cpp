// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/Logger.h>
#include <Core/BlockGen.h>
#include <DataTypes/DataTypeEnum.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/RoughCheck.h>
#include <Storages/DeltaMerge/Index/ValueComparison.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ctime>
#include <ext/scope_guard.h>
#include <memory>

namespace DB
{
namespace DM
{
namespace tests
{
static const ColId DEFAULT_COL_ID = 0;
static const String DEFAULT_COL_NAME = "2020-09-26";

class DMMinMaxIndexTest : public ::testing::Test
{
public:
    DMMinMaxIndexTest() {}

protected:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        context = std::make_unique<Context>(DMTestEnv::getContext());
        if (!context->getMinMaxIndexCache())
        {
            context->setMinMaxIndexCache(5368709120);
        }
    }

    void TearDown() override
    {
        context->dropMinMaxIndexCache();
    }

private:
protected:
    // a ptr to context, we can reload context with different settings if need.
    std::unique_ptr<Context> context;
};

Attr attr(String type)
{
    return Attr{DEFAULT_COL_NAME, DEFAULT_COL_ID, DataTypeFactory::instance().get(type)};
}

Attr pkAttr()
{
    const ColumnDefine & col = getExtraHandleColumnDefine(true);
    return Attr{col.name, col.id, col.type};
}

bool checkMatch(
    const String & test_case,
    Context & context,
    const String & type,
    const CSVTuples block_tuples,
    const RSOperatorPtr & filter,
    bool is_common_handle = false,
    bool check_pk = false)
{
    String name = "DMMinMaxIndexTest_" + test_case;

    auto clean_up = [&]() {
        context.dropMinMaxIndexCache();
    };

    clean_up();
    SCOPE_EXIT({ clean_up(); });

    RowKeyRange all_range = RowKeyRange::newAll(is_common_handle, 1);

    ColumnDefine cd(DEFAULT_COL_ID, DEFAULT_COL_NAME, DataTypeFactory::instance().get(type));

    ColumnDefines table_columns;
    table_columns.push_back(getExtraHandleColumnDefine(is_common_handle));
    table_columns.push_back(getVersionColumnDefine());
    table_columns.push_back(getTagColumnDefine());
    table_columns.push_back(cd);

    Block header = toEmptyBlock(table_columns);
    Block block = genBlock(header, block_tuples);

    DeltaMergeStorePtr store = std::make_shared<DeltaMergeStore>(
        context,
        false,
        "test_database",
        name,
        /*table_id*/ 100,
        table_columns,
        getExtraHandleColumnDefine(is_common_handle),
        is_common_handle,
        1);

    auto & settings = context.getSettingsRef();
    settings.dt_segment_stable_pack_rows = 10;

    store->write(context, context.getSettingsRef(), block);
    store->flushCache(context, all_range);
    store->mergeDeltaAll(context);

    const ColumnDefine & col_to_read = check_pk ? getExtraHandleColumnDefine(is_common_handle) : cd;
    auto streams = store->read(context, context.getSettingsRef(), {col_to_read}, {all_range}, 1, std::numeric_limits<UInt64>::max(), filter, name);
    streams[0]->readPrefix();
    auto rows = streams[0]->read().rows();
    streams[0]->readSuffix();
    //   sleep(10);
    store->drop();

    return rows != 0;
}

bool checkMatch(const String & test_case, Context & context, const String & type, const String & value, const RSOperatorPtr & filter)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{"0", "0", "0", value}};
    return checkMatch(test_case, context, type, tuples, filter);
}

bool checkDelMatch(const String & test_case, Context & context, const String & type, const String & value, const RSOperatorPtr & filter)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{"0", "0", "1", value}};
    return checkMatch(test_case, context, type, tuples, filter);
}

bool checkPkMatch(const String & test_case, Context & context, const String & type, const String & pk_value, const RSOperatorPtr & filter, bool is_common_handle)
{
    // The first three values are pk, version and del_mark.
    // For del_mark, 1 means deleted.
    CSVTuples tuples = {{pk_value, "0", "0", "0"}};
    return checkMatch(test_case, context, type, tuples, filter, is_common_handle, true);
}

CSVTuples generateTuples(CSVTuple basic_tuple, size_t tuple_num)
{
    CSVTuples result(tuple_num, basic_tuple);
    for (size_t i = 1; i < tuple_num; i++)
    {
        CSVTuple origin_tuple = result[i - 1];
        for (size_t j = 0; j < origin_tuple.size(); j++)
        {
            if (j == 2)
            {
                continue;
            }
            result[i][j] = toString(std::stoi(result[i - 1][j]) + 1);
        }
    }
    return result;
}

TEST_F(DMMinMaxIndexTest, Basic)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    ASSERT_EQ(true, checkMatch(case_name, *context, "String", "hehe", createLike(attr("String"), Field((String) "1|_a\\_a%23"), Field((String)"\\"))));
    //CSVTuples tuples = generateTuples({"0", "0", "0", "1111"}, 300);
    //   ASSERT_EQ(false, checkMatch(case_name, *context, "String", tuples, createEqual(attr("String"), Field((String)"1111"))));

    // clang-format off
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", "100", createEqual(attr("Int64"), Field((Int64)101))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createIn(attr("Int64"), {Field((Int64)100)})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", "100", createIn(attr("Int64"), {Field((Int64)101)})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createGreater(attr("Int64"), Field((Int64)99), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", "100", createGreater(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createGreaterEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", "100", createGreaterEqual(attr("Int64"), Field((Int64)101), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createLess(attr("Int64"), Field((Int64)101), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", "100", createLess(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createLessEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", "100", createLessEqual(attr("Int64"), Field((Int64)99), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Date", "2020-09-27", createEqual(attr("Date"), Field((String) "2020-09-27"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Date", "2020-09-27", createEqual(attr("Date"), Field((String) "2020-09-28"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Date", "2020-09-27", createIn(attr("Date"), {Field((String) "2020-09-27")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Date", "2020-09-27", createIn(attr("Date"), {Field((String) "2020-09-28")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Date", "2020-09-27", createGreater(attr("Date"), Field((String) "2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Date", "2020-09-27", createGreater(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Date", "2020-09-27", createGreaterEqual(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Date", "2020-09-27", createGreaterEqual(attr("Date"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Date", "2020-09-27", createLess(attr("Date"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Date", "2020-09-27", createLess(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Date", "2020-09-27", createLessEqual(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Date", "2020-09-27", createLessEqual(attr("Date"), Field((String) "2020-09-26"), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:02"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createIn(attr("DateTime"), {Field((String) "2020-01-01 05:00:01")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createIn(attr("DateTime"), {Field((String) "2020-01-01 05:00:02")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createGreater(attr("DateTime"), Field((String) "2020-01-01 05:00:00"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createGreater(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createGreaterEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createGreaterEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:02"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createLess(attr("DateTime"), Field((String) "2020-01-01 05:00:02"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createLess(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createLessEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "DateTime", "2020-01-01 05:00:01", createLessEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:00"), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-27")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-28")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "MyDateTime", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));

    ASSERT_EQ(false, checkDelMatch(case_name, *context, "Int64", "100", createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createEqual(pkAttr(), Field((Int64)100)), true));
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createGreater(pkAttr(), Field((Int64)99), 0), true));
    ASSERT_EQ(true, checkPkMatch(case_name, *context, "Int64", "100", createGreater(pkAttr(), Field((Int64)99), 0), false));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createNotEqual(attr("Int64"), Field((Int64)101))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "String", "test_like_filter", createLike(attr("String"), Field((String) "*filter"), Field((String) "\\"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "String", "test_not_like_filter", createNotLike(attr("String"), Field(Field((String) "*test_like_filter")))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createNotIn(attr("Int64"), {Field((Int64)101), Field((Int64)102), Field((Int64)103)})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", "100", createIn(attr("Int64"), {Field((Int64)100), Field((Int64)101), Field((Int64)102)})));

    ASSERT_EQ(true, checkMatch(case_name, *context, "String", "hehe", createEqual(attr("String"), Field((String)"hehe"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "String", "hoho", createEqual(attr("String"), Field((String)"hehe"))));
    // clang-format on
}
CATCH

TEST_F(DMMinMaxIndexTest, NullableToNullable)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    // clang-format off
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createEqual(attr("Nullable(Int64)"), Field((Int64)100))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createEqual(attr("Nullable(Int64)"), Field((Int64)101))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createIn(attr("Nullable(Int64)"), {Field((Int64)100)})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createIn(attr("Nullable(Int64)"), {Field((Int64)101)})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreater(attr("Nullable(Int64)"), Field((Int64)99), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreater(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreaterEqual(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreaterEqual(attr("Nullable(Int64)"), Field((Int64)101), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLess(attr("Nullable(Int64)"), Field((Int64)101), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLess(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLessEqual(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLessEqual(attr("Nullable(Int64)"), Field((Int64)99), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createEqual(attr("Nullable(Date)"), Field((String) "2020-09-27"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createEqual(attr("Nullable(Date)"), Field((String) "2020-09-28"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createIn(attr("Nullable(Date)"), {Field((String) "2020-09-27")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createIn(attr("Nullable(Date)"), {Field((String) "2020-09-28")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreater(attr("Nullable(Date)"), Field((String) "2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreater(attr("Nullable(Date)"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreaterEqual(attr("Nullable(Date)"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreaterEqual(attr("Nullable(Date)"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLess(attr("Nullable(Date)"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLess(attr("Nullable(Date)"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLessEqual(attr("Nullable(Date)"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLessEqual(attr("Nullable(Date)"), Field((String) "2020-09-26"), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:02"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createIn(attr("DateTime"), {Field((String) "2020-01-01 05:00:01")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createIn(attr("DateTime"), {Field((String) "2020-01-01 05:00:02")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreater(attr("DateTime"), Field((String) "2020-01-01 05:00:00"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreater(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreaterEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreaterEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:02"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLess(attr("DateTime"), Field((String) "2020-01-01 05:00:02"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLess(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLessEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLessEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:00"), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-27")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-28")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));

    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createEqual(attr("Nullable(Int64)"), Field((Int64)101))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createIn(attr("Nullable(Int64)"), {Field((Int64)101)})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createGreater(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createGreaterEqual(attr("Nullable(Int64)"), Field((Int64)101), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createLess(attr("Nullable(Int64)"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createLessEqual(attr("Nullable(Int64)"), Field((Int64)99), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(String)", {{"0", "0", "0", "hehe"}, {"1", "1", "0", "\\N"}}, createEqual(attr("Nullable(String)"), Field((String)("hehe")))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(String)", {{"0", "0", "0", "haha"}, {"1", "1", "0", "hehe"}}, createEqual(attr("Nullable(String)"), Field((String)("hehe")))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(String)", {{"0", "0", "0", "haha"}, {"1", "1", "0", "hehe"}}, createEqual(attr("Nullable(String)"), Field((String)("hoho")))));


}
CATCH

TEST_F(DMMinMaxIndexTest, NullableToValue)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    // clang-format off
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createEqual(attr("Int64"), Field((Int64)101))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createIn(attr("Int64"), {Field((Int64)100)})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createIn(attr("Int64"), {Field((Int64)101)})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreater(attr("Int64"), Field((Int64)99), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreater(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreaterEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createGreaterEqual(attr("Int64"), Field((Int64)101), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLess(attr("Int64"), Field((Int64)101), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLess(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLessEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", "100", createLessEqual(attr("Int64"), Field((Int64)99), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createEqual(attr("Date"), Field((String) "2020-09-27"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createEqual(attr("Date"), Field((String) "2020-09-28"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createIn(attr("Date"), {Field((String) "2020-09-27")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createIn(attr("Date"), {Field((String) "2020-09-28")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreater(attr("Date"), Field((String) "2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreater(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreaterEqual(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createGreaterEqual(attr("Date"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLess(attr("Date"), Field((String) "2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLess(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLessEqual(attr("Date"), Field((String) "2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Date)", "2020-09-27", createLessEqual(attr("Date"), Field((String) "2020-09-26"), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:02"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createIn(attr("DateTime"), {Field((String) "2020-01-01 05:00:01")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createIn(attr("DateTime"), {Field((String) "2020-01-01 05:00:02")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreater(attr("DateTime"), Field((String) "2020-01-01 05:00:00"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreater(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreaterEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createGreaterEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:02"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLess(attr("DateTime"), Field((String) "2020-01-01 05:00:02"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLess(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLessEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:01"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(DateTime)", "2020-01-01 05:00:01", createLessEqual(attr("DateTime"), Field((String) "2020-01-01 05:00:00"), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-27")})));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createIn(attr("MyDateTime"), {parseMyDateTime("2020-09-28")})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreater(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createGreaterEqual(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-28"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLess(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-27"), 0)));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(MyDateTime)", "2020-09-27", createLessEqual(attr("MyDateTime"), parseMyDateTime("2020-09-26"), 0)));

    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createEqual(attr("Int64"), Field((Int64)101))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createIn(attr("Int64"), {Field((Int64)101)})));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createGreater(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createGreaterEqual(attr("Int64"), Field((Int64)101), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createLess(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(Int64)", {{"0", "0", "0", "100"}, {"1", "1", "0", "\\N"}}, createLessEqual(attr("Int64"), Field((Int64)99), 0)));

    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(String)", {{"0", "0", "0", "hehe"}, {"1", "1", "0", "\\N"}}, createEqual(attr("String"), Field((String)("hehe")))));
    ASSERT_EQ(true, checkMatch(case_name, *context, "Nullable(String)", {{"0", "0", "0", "haha"}, {"1", "1", "0", "hehe"}}, createEqual(attr("String"), Field((String)("hehe")))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Nullable(String)", {{"0", "0", "0", "haha"}, {"1", "1", "0", "hehe"}}, createEqual(attr("String"), Field((String)("hoho")))));

}
CATCH

TEST_F(DMMinMaxIndexTest, Logical)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", "100", createNot(createEqual(attr("Int64"), Field((Int64)100)))));
    ASSERT_EQ(false,
              checkMatch(case_name,
                         *context,
                         "Int64",
                         "100",
                         createAnd({createEqual(attr("Int64"), Field((Int64)101)), createEqual(attr("Int64"), Field((Int64)100))})));
    ASSERT_EQ(true,
              checkMatch(case_name,
                         *context,
                         "Int64",
                         "100",
                         createOr({createEqual(attr("Int64"), Field((Int64)101)), createEqual(attr("Int64"), Field((Int64)100))})));
}
CATCH

TEST_F(DMMinMaxIndexTest, DelMark)
try
{
    const auto * case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    ASSERT_EQ(true, checkMatch(case_name, *context, "Int64", {{"0", "0", "0", "100"}}, createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(false, checkMatch(case_name, *context, "Int64", {{"0", "0", "1", "100"}}, createEqual(attr("Int64"), Field((Int64)100))));
    ASSERT_EQ(true,
              checkMatch(case_name,
                         *context,
                         "Int64",
                         {{"0", "0", "1", "100"}, {"1", "1", "0", "100"}},
                         createGreaterEqual(attr("Int64"), Field((Int64)100), 0)));
    ASSERT_EQ(false,
              checkMatch(case_name,
                         *context,
                         "Int64",
                         {{"0", "0", "1", "88"}, {"1", "1", "0", "100"}},
                         createLess(attr("Int64"), Field((Int64)100), 0)));
}
CATCH

TEST_F(DMMinMaxIndexTest, Enum8ValueCompare)
try
{
    DataTypeEnum8::Values values;
    values.push_back({"test", 50});
    values.push_back({"test_2", 100});
    values.push_back({"test_3", 0});
    auto enum8_type = std::make_shared<DataTypeEnum8>(values);
    ASSERT_EQ(RoughCheck::Cmp<EqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_2"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_3"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOp>::compare(Field((String) "test"), enum8_type, (Int8)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test_3"), enum8_type, (Int8)-1), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOp>::compare(Field((String) "test"), enum8_type, (Int8)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum8_type, (Int8)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test_2"), enum8_type, (Int8)101), ValueCompareResult::True);
}
CATCH

TEST_F(DMMinMaxIndexTest, Enum16ValueCompare)
try
{
    DataTypeEnum16::Values values;
    values.push_back({"test", 50});
    values.push_back({"test_2", 100});
    values.push_back({"test_3", 0});
    auto enum16_type = std::make_shared<DataTypeEnum16>(values);
    ASSERT_EQ(RoughCheck::Cmp<EqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_2"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test_3"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<NotEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOp>::compare(Field((String) "test"), enum16_type, (Int16)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)49), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<GreaterOrEqualsOp>::compare(Field((String) "test_3"), enum16_type, (Int16)-1), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOp>::compare(Field((String) "test"), enum16_type, (Int16)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)50), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test"), enum16_type, (Int16)51), ValueCompareResult::True);
    ASSERT_EQ(RoughCheck::Cmp<LessOrEqualsOp>::compare(Field((String) "test_2"), enum16_type, (Int16)101), ValueCompareResult::True);
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB
