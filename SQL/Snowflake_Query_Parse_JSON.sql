select 
w_id,
alt_id,
supplement_id,
comments
from 
( select 
raw_data:w_id::string AS w_id,
raw_data:_test_status:main.alt_id::string as alt_id,
raw_data:_test_status:main.supplement_id::string as supplement_id,
raw_data:comments::string   AS      comments
from origin_json_test,
        LATERAL FLATTEN(input => raw_data:_test_status:main.alt_id,outer => true) as alt_id,
        LATERAL FLATTEN(input => raw_data:_test_status:main.supplement_id,outer => true) as supplement_id
        UNION ALL
select 
raw_data:w_id::string AS w_id,
raw_data:_test_status:supplements."1".alt_id::string as alt_id,
raw_data:_test_status:supplements."1".supplement_id::string as supplement_id,
raw_data:_test_status:supplements."1".fields.comments::string as comments
from origin_json_test,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."1".alt_id,outer => true) as alt_id,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."1".supplement_id,outer => true) as supplement_id,
        --LATERAL FLATTEN(input => raw_data:_test_status:supplements."1".fields.comments,outer => true) as comments
    WHERE raw_data:_test_status:supplements."1".supplement_id is not null
    UNION ALL
select 
raw_data:w_id::string AS w_id,
raw_data:_test_status:supplements."2".alt_id::string as alt_id,
raw_data:_test_status:supplements."2".supplement_id::string as supplement_id,
raw_data:_test_status:supplements."2".fields.comments::string as comments
from origin_json_test,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."2".alt_id,outer => true) as alt_id,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."2".supplement_id,outer => true) as supplement_id,
        --LATERAL FLATTEN(input => raw_data:_test_status:supplements."2".fields.comments,outer => true) as comments
    WHERE raw_data:_test_status:supplements."2".supplement_id is not null
    UNION ALL

select 
raw_data:w_id::string AS w_id,
raw_data:_test_status:supplements."3".alt_id::string as alt_id,
raw_data:_test_status:supplements."3".supplement_id::string as supplement_id,
raw_data:_test_status:supplements."3".fields.comments::string as comments
from origin_json_test,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."3".alt_id,outer => true) as alt_id,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."3".supplement_id,outer => true) as supplement_id,
        --LATERAL FLATTEN(input => raw_data:_test_status:supplements."3".fields.comments,outer => true) as comments
    WHERE raw_data:_test_status:supplements."3".supplement_id is not null
    UNION ALL

select 
raw_data:w_id::string AS w_id,
raw_data:_test_status:supplements."4".alt_id::string as alt_id,
raw_data:_test_status:supplements."4".supplement_id::string as supplement_id,
raw_data:_test_status:supplements."4".fields.comments::string as comments
from origin_json_test,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."4".alt_id,outer => true) as alt_id,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."4".supplement_id,outer => true) as supplement_id,
       --LATERAL FLATTEN(input => raw_data:_test_status:supplements."4".fields.comments,outer => true) as comments
    WHERE raw_data:_test_status:supplements."4".supplement_id is not null
    UNION ALL

select 
raw_data:w_id::string AS w_id,
raw_data:_test_status:supplements."5".alt_id::string as alt_id,
raw_data:_test_status:supplements."5".supplement_id::string as supplement_id,
raw_data:_test_status:supplements."5".fields.comments::string as comments
from origin_json_test,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."5".alt_id,outer => true) as alt_id,
        LATERAL FLATTEN(input => raw_data:_test_status:supplements."5".supplement_id,outer => true) as supplement_id,
        --LATERAL FLATTEN(input => raw_data:_test_status:supplements."5".fields.comments,outer => true) as comments
    WHERE raw_data:_test_status:supplements."5".supplement_id is not null)
    order by w_id, alt_id, supplement_id);