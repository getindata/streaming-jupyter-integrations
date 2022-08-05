import { JupyterFrontEnd, JupyterFrontEndPlugin } from '@jupyterlab/application';
import { ICodeMirror } from '@jupyterlab/codemirror';
import { ILSPCodeExtractorsManager } from '@krassowski/jupyterlab-lsp';
import { graphExtractors } from './extractors';

const FLAG_OPTS = [
  '-c',
  '--cache', //          Cache dataframe
  '-e',
  '--eager', //          Cache dataframe with eager load
  '-q',
  '--qgrid', //          Display results in qgrid
].join('|');
const SPACE = ' ';
const OPTION_VALUE = '[0-9a-zA-Z/._]+';
const SHORT_OPTS = `-[a-z] ${OPTION_VALUE}`;
const LONG_OPTS = `--[a-zA-Z]+ ${OPTION_VALUE}`;
const COMMANDS = `(?:${SPACE}|${FLAG_OPTS}|${SHORT_OPTS}|${LONG_OPTS})*`;

// Based on https://github.com/jupyterlab/jupyterlab/blob/1.0.x/packages/codemirror/src/codemirror-ipython.ts
// and https://github.com/CybercentreCanada/jupyterlab-sql-editor/blob/6aa0e515747dd84425374101bbb0198048664586/src/index.ts
function codeMirrorWithSqlSyntaxHighlightSupport(c: ICodeMirror) {
  c.CodeMirror.defineMode(
    'ipython',
    (config: CodeMirror.EditorConfiguration, modeOptions?: any) => {
      const pythonConf: any = {};
      for (const prop in modeOptions) {
        if (Object.prototype.hasOwnProperty.call(modeOptions, prop)) {
          pythonConf[prop] = modeOptions[prop];
        }
      }
      pythonConf.name = 'python';
      pythonConf.singleOperators = new RegExp('^[\\+\\-\\*/%&|@\\^~<>!\\?]');
      pythonConf.identifiers = new RegExp(
        '^[_A-Za-z\u00A1-\uFFFF][_A-Za-z0-9\u00A1-\uFFFF]*'
      );

      const pythonMode = c.CodeMirror.getMode(config, pythonConf);
      const sqlMode = c.CodeMirror.getMode(config, 'sql');
      return c.CodeMirror.multiplexingMode(pythonMode, {
        open: RegExp(`%%flink_execute_sql${COMMANDS}`) as unknown as string,
        close: '__A MARKER THAT WILL NEVER BE MATCHED__', // Cell magic: capture chars till the end of the cell
        parseDelimiters: false,
        mode: sqlMode,
      });
    }
  );
}

/**
 * Initialization data for the flink_sql_lsp_extension extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'flink_sql_lsp_extension:plugin',
  requires: [ICodeMirror, ILSPCodeExtractorsManager],
  autoStart: true,
  activate: (
    app: JupyterFrontEnd,
    cm: ICodeMirror,
    codeExtractors: ILSPCodeExtractorsManager
  ) => {
    codeMirrorWithSqlSyntaxHighlightSupport(cm);

    for (const [language, extractors] of Object.entries(graphExtractors)) {
      for (const extractor of extractors) {
        codeExtractors.register(extractor, language);
      }
    }
  },
};

export default plugin;
