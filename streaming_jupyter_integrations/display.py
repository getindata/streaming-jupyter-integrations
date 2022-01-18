from pyflink.table import ResultKind


def pyflink_result_kind_to_string(result_kind: ResultKind) -> str:
    if result_kind == ResultKind.SUCCESS:
        return "SUCCESS"
    return "UNDEFINED"
