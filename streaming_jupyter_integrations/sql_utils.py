
def inline_sql_in_cell(cell_contents) -> str:
    """
    Converts cell contents to a single line SQL statement.
    """
    return cell_contents.replace('\n', ' ')
