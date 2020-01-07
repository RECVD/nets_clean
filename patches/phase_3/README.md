# Prospective Bad Data Patch

## State of the Variables

- Auxiliary Non-hierarchy: Fine

- Auxiliary Hierarchy: Fine except some weird edge cases (zoo and vpa)

- Main as combinations of non-hierarchies: fine

- Main as combinations of hierarchies: Not right.  These should be the same as a simple sum of hierarchy categories within the main categories and they're not


# Patch Workflow

1.  Run pytest in test_phase_3.py for unit testing.
2. Run phase_3.py to create phase 3 transformed data.
3. Run create_categorized_file.py to create a file that only contains NETS records that fall into at least one category.
4. Run create_category_distribution.py to create a probability distribution which will be used to sample a dataframe for further testing.
5. Run sample_df.py to sample the categorized file based on that distribution.
6. Run phase_3_test_output.py to run further tests based on the sample drawn from sample_df.py.

