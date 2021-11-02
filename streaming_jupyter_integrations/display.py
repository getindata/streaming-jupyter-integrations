import pandas as pd
from IPython import display
from pyflink.table import ResultKind


def display_execution_result(execution_result) -> pd.DataFrame:
    """
     Displays the execution result and returns a dataframe containing all the results.
     Display is done in a stream-like fashion displaying the results as they come.
    """

    columns = execution_result.get_table_schema().get_field_names()
    df = pd.DataFrame(columns=columns)
    result_kind = execution_result.get_result_kind()

    if result_kind == ResultKind.SUCCESS_WITH_CONTENT:
        with execution_result.collect() as results:
            for result in results:
                res = [cell for cell in result]
                a_series = pd.Series(res, index=df.columns)
                # rolling display for long / real time queries
                display.clear_output(wait=True)
                df = df.append(a_series, ignore_index=True)
                display.display(df)
    else:
        series = pd.Series([pyflink_result_kind_to_string(result_kind)], index=df.columns)
        display.clear_output(wait=True)
        df = df.append(series, ignore_index=True)
        display.display(df)

    return df


def pyflink_result_kind_to_string(result_kind) -> str:
    if result_kind == ResultKind.SUCCESS:
        return "SUCCESS"
    return "UNDEFINED"
