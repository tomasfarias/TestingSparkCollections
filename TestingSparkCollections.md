## Testing Spark Collections

One of the struggles I faced when developing Spark applications with Python was testing the methods I wrote to operate on DataFrames and RDDs during the Transform part of an ETL process. As of me writing this, `pyspark` doesn't include any testing module like `pandas` or `numpy` do, in fact, one of the first posts I found recommended calling `toPandas` in a `pyspark` DataFrame to turn it into a `pandas` DataFrame and test that. This introduces a couple of problems: introduces a new dependency and `pyspark` DataFrames are not exactly the same as `pandas` DataFrames.

Enter [spark-test](https://github.com/tomasfarias/spark-test): a testing module which contains functions to test Spark collections, inspired by what `pandas` offers. I will now go over a test scenario where spark-test can be used and how collections are actually tested.

### Are my DataFrame transformations working?

Imagine we have a function to operate on a Spark DataFrame that looks like this:

<script src="https://gist.github.com/tomasfarias/81e1e01594c1c44ff943b646b0862068.js"></script>

And some test csv data that looks like this:

```
Name,Species,Age,AdoptedDate
Max,Dog,2,2019-04-05
Daisy,Cat,1,2018-01-10
Jake,Dog,4,2017-12-01
Zoe,Cat,8,2019-10-15
Cooper,Dog,7,2018-07-28
Coco,Cat,10,2017-08-25
Lola,Dog,1,2019-12-19
Lucky,Dog,3,2018-10-31
```

We would like to know if our function is outputting what we expect it to output, so we are going to write a test for that: let's create a DataFrame ourselves that looks exactly how we expect our output to look and then assert that it's equal to the value returned by our `transform` function. In order to compare the DataFrames we will be using `assert_dataframe_equal` from `spark-test`.

Here's how a test file could look<sup>1</sup>:

<script src="https://gist.github.com/tomasfarias/6409ce33c4f9c3ee5ef9a00be42cfa3e.js"></script>

### Let's break the schema

When we run the tests with `pytests` as usual we get a Failure! Upon inspecting the output we can see `assert_dataframe_equal` added some information of what could be going wrong:

```
$ pytest tests/test_transform.py
============================= test session starts =============================
platform linux -- Python 3.7.3, pytest-5.0.1, py-1.8.0, pluggy-0.12.0
rootdir: /home/tfarias/repos/blogposts/TestingSparkCollections
collected 1 item

tests/test_transform.py F                                               [100%]

================================== FAILURES ===================================

[...]

            assert l_field.dataType == r_field.dataType, msg.format(
>               attr='dataType', l_val=l_field.dataType, r_val=r_field.dataType
            )
E           AssertionError: Difference in schema when comparing fields
E            +  where AdoptedYear's dataType = LongType
E            +  where AdoptedYear's dataType = IntegerType
```

Looks like our `AdoptedYear` column has a different type: when creating a DataFrame with `spark.createDataFrame` without a proper schema specified Spark sets the type of our `AdoptedYear` column to `LongType` whereas our resulting DataFrame comes with an `IntegerType` column.

What can we do to fix our tests? Well, the answer depends on how our expected result is supposed to look: if we want our `AdoptedYear` column to be an `IntegerType` we should alter our expected DataFrame. On the other hand, if we wish to use a `LongType`, we should review our `transform` function. Finally, if we don't care about the schema at all, we can just pass `check_schema=False` to `assert_dataframe_equal`, although I do not recommend this.

Regardless of the answer it is always a good idea to explicitly state the schema when creating the expected DataFrame. I'll go with the first option for this example, as it makes sense to have a year column as an `IntegerType`:

<script src="https://gist.github.com/tomasfarias/11f39fdb02a1955b57b3c2787475edb8.js"></script>

I also went ahead and set the `count` column to `LongType` and set `nullable` to `False` as those are defaults when calling `groupBy` and I didn't want to run into the same error twice or thrice. After running the new tests we get a passing result:

```
$ pytest tests/test_transform_schema.py
============================= test session starts ==============================
platform linux -- Python 3.7.3, pytest-5.0.1, py-1.8.0, pluggy-0.12.0
rootdir: /home/tfarias/repos/blogposts/TestingSparkCollections
collected 1 item

tests/test_transform_fixed.py .                                         [100%]

========================== 1 passed in 21.52 seconds ===========================
```

### That's not how counting works

One of the great advantages of tests is that they'll be there to tell you whenever a new feature introduces an unexpected bug. Let's introduce a bug in our transform function by adding `1` to `count` when `Species` equals `'Cat'`. When we run our tests we get the following error message:

```
$ pytest tests/test_transform_schema.py
============================= test session starts ==============================
platform linux -- Python 3.7.3, pytest-5.0.1, py-1.8.0, pluggy-0.12.0
rootdir: /home/tfarias/repos/blogposts/TestingSparkCollections
collected 2 items

tests/test_transform_fixed.py .F                                        [100%]

=================================== FAILURES ===================================
_______________________ test_transform[transform_bugged] _______________________

[...]

E           AssertionError: On row 0. Values for count do not match
E            +  where left=1
E            +  where right=2
```

Immediatly we can tell something must be going on with the `count` column: our expected value of 1 did not match the result of 2. `spark-test` even adds a row index to help us debug the issue.

By default, `spark-test` will also check that the order of our rows matches the expected order. So if we change the sorting order in our transform function our tests will fail:

```
$ pytest tests/test_transform_fixed.py
============================= test session starts ==============================
platform linux -- Python 3.7.3, pytest-5.0.1, py-1.8.0, pluggy-0.12.0
rootdir: /home/tfarias/repos/blogposts/TestingSparkCollections
collected 3 items

tests/test_transform_fixed.py .FF                                        [100%]

=================================== FAILURES ===================================
____________________ test_transform[transform_wrong_order] _____________________

[...]

E           AssertionError: On row 1.Values for AdoptedYear do not match
E            +  where left=2017
E            +  where right=2018
```

This can be disabled by passing `check_order=False` to `assert_dataframe_equal`. Internally, when `check_order` is set to `False`, `assert_dataframe_equal` constructs a `Counter` with a list of rows from each DataFrames, and then compares that all rows are present and that the counts match.

### Conclusion

Thank you for reading and feel free to checkout [spark-test](https://github.com/tomasfarias/spark-test), you can get it from PyPi by running `pip install spark-test`. I hope it at least serves you as a starting point to enhance your tests!

---

1. I'm also using a `conftest.py` file to define a couple of useful fixtures, check out the [GitHub repository](https://github.com/tomasfarias/TestingSparkCollections) of this blogpost to get the full code.
