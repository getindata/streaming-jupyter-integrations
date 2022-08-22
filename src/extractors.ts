import {
  IForeignCodeExtractorsRegistry,
  RegExpForeignCodeExtractor,
} from '@krassowski/jupyterlab-lsp';

export const SQL_URL_PATTERN = '(?:(?:.*?)://(?:.*))';
// note: -a/--connection_arguments and -f/--file are not supported yet
const single_argument_options = [
  '-x',
  '--close',
  '-c',
  '--creator',
  '-p',
  '--persist',
  '--append',
];
const zero_argument_options = ['-l', '--connections'];

const COMMAND_PATTERN =
  '(?:' +
  (zero_argument_options.join('|') +
    '|' +
    single_argument_options.map((command) => command + ' \\w+').join('|')) +
  ')';

export const graphExtractors: IForeignCodeExtractorsRegistry = {
  // general note: to match new lines use [^] instead of dot, unless the target is ES2018, then use /s
  python: [
    new RegExpForeignCodeExtractor({
      language: 'sql',
      pattern: `^%%flink_execute_sql(?: (?:${SQL_URL_PATTERN}|${COMMAND_PATTERN}|(?:\\w+ << )|(?:\\w+@\\w+)))?\n?((?:.+\n)?(?:[^]*))`,
      foreign_capture_groups: [1],
      is_standalone: true,
      file_extension: 'sql',
    }),
    new RegExpForeignCodeExtractor({
      language: 'sql',
      pattern: `(?:^|\n)%flink_execute_sql (?:${SQL_URL_PATTERN}|${COMMAND_PATTERN}|(.*))\n?`,
      foreign_capture_groups: [1],
      is_standalone: false,
      file_extension: 'sql',
    }),
  ],
};
