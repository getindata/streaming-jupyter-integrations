import os.path
from typing import List


class SQLSyntaxHighlighting:
    def __init__(self, magic_names: List[str], jupyter_dir: str):
        self.custom_js_path = os.path.join(jupyter_dir, "custom", "custom.js")
        self.magic_names = magic_names

    def add_syntax_highlighting_js(self) -> None:
        """
        Ensures that the custom.js config file for magic_names magics contains
        javascript code to highlight SQL syntax in cells decorated by magic_names.
        """
        self.__ensure_customjs_exists()
        self.__ensure_new_code_in_customjs()

    def __ensure_new_code_in_customjs(self) -> None:
        """
        Appends new code to custom.js file.
        To remove the strain here we do not attempt to parse the config and just append
        the possibly new code at the end of the file. This ensures that the new code replaces
        the old code and if the user has some kind of a config there we would not replace / delete it completely.
        """
        code = self.__sql_highlight_code()
        with open(self.custom_js_path) as f:
            if code in f.read():
                return
        with open(self.custom_js_path, "a") as f:
            f.write(f"\n{code}\n")
            print(
                "SQL syntax highlighting has been added. "
                "To see it immediately a one-time notebook reload is required. "
                "Please shutdown the notebook and reconnect again."
            )

    def __ensure_customjs_exists(self) -> None:
        """
        Ensures that custom.js file exists under jupyter config.
        Creates all the directories on the custom.js path if they do not exist.
        """
        if not os.path.exists(self.custom_js_path):
            os.makedirs(os.path.dirname(self.custom_js_path), exist_ok=True)
            open(self.custom_js_path, "w")

    def __make_magics_regex(self) -> str:
        """
        Returns a regex that matches only the first set of characters that match any magic.
        Magic names are passed to this class and are joined with `|` operator.
        %% prefix is automatically appended for the magic names.
        """
        match_any_of = "|".join(self.magic_names)
        return f"^%%({match_any_of})"

    # https://stackoverflow.com/questions/43641362/adding-syntax-highlighting-to-jupyter-notebook-cell-magic
    def __sql_highlight_code(self) -> str:
        """
        Returns a javascript code that enabled SQL syntax highlighting.
        """
        return (
            """
require(['notebook/js/codecell'], function(codecell) {
    codecell.CodeCell.options_default.highlight_modes['magic_text/x-mssql'] = {'reg':[/"""
            + self.__make_magics_regex()
            + """/]} ;
    Jupyter.notebook.events.one('kernel_ready.Kernel', function(){
    Jupyter.notebook.get_cells().map(function(cell){
        if (cell.cell_type == 'code'){ cell.auto_highlight(); } }) ;
    });
});
"""
        )
