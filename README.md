# use-case-1

In use case 1,I have preformed below task.
->Data cleaning
->Changing Data type
->creating Athena tables via crawler(for standard report and summary report)

1)copying data from inbound to preprocess directory(refer function-copy_file() in use case1.py file)

2)used indicator check(refer function-ind_check() in use case1.py file)

3)converted xls to csv.(refer function-cnvrt_xls_csv() in use case1.py file)

4)placing files to landing folders(refer function-move_to_landing() in use case1.py file)

5) data cleaning and changing data type as per requirement performed(refer function-create_df() in use case1.py file)
6)creating Athena tables via crawler(for standard report and summary report)(refer function-stand_create_crawler() and sum_create_crawler() in use case1.py file)
